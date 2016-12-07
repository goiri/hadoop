/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_SECS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_SECS_KEY;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * The {@link Router} periodically checks the state of a Namenode (usually on
 * the same server) and reports their high availability (HA) state and
 * load/space status to the
 * {@link org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService}
 * . Note that this is an optional role as a Router can be independent of any
 * subcluster.
 * <p>
 * For performance with Namenode HA, the Router uses the high availability state
 * information in the State Store to forward the request to the Namenode that is
 * most likely to be active.
 * <p>
 * Note that this service can be embedded into the Namenode itself to simplify
 * the operation.
 */
public class NamenodeHeartbeatService extends PeriodicService {

  private static final Log LOG =
      LogFactory.getLog(NamenodeHeartbeatService.class);

  /** Configuration for the heartbeat. */
  private Configuration conf;

  /** Router performing the heartbeating. */
  private ActiveNamenodeResolver resolver;

  /** Interface to the tracked NN. */
  private String nameserviceId;
  private String namenodeId;

  /** Namenode HA target. */
  private NNHAServiceTarget localTarget;
  /** RPC address for the namenode. */
  private String serviceAddress;
  /** HTTP address for the namenode. */
  private String webAddress;

  /**
   * Create a new Namenode status updater.
   * @param resolver Namenode resolver service to handle NN registration.
   * @param nameserviceId Identifier of the nameservice.
   * @param namenodeId Identifier of the namenode in HA.
   */
  public NamenodeHeartbeatService(ActiveNamenodeResolver resolver,
      String nameserviceId, String namenodeId) {
    super(NamenodeHeartbeatService.class.getName());

    this.resolver = resolver;

    this.nameserviceId = nameserviceId;
    this.namenodeId = namenodeId;

    this.setRunnable(new Runnable() {
      public void run() {
        updateState();
      }
    });
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    super.serviceInit(configuration);
    this.conf = configuration;
    this.setIntervalSecs(conf.getInt(
        DFS_ROUTER_HEARTBEAT_INTERVAL_SECS_KEY,
        DFS_ROUTER_HEARTBEAT_INTERVAL_SECS_DEFAULT));

    if (this.namenodeId != null && !this.namenodeId.isEmpty()) {
      this.localTarget = new NNHAServiceTarget(
          conf, nameserviceId, namenodeId);
    } else {
      this.localTarget = null;
    }

    // Get the endpoints for the Namenode to monitor from the config
    this.serviceAddress =
        DFSUtil.getNamenodeServiceAddr(conf, nameserviceId, namenodeId);
    if (this.serviceAddress == null) {
      throw new IOException("Unable to locate RPC service address for NN "
          + nameserviceId + "-" + namenodeId);
    }

    this.webAddress =
        DFSUtil.getNamenodeWebAddr(conf, nameserviceId, namenodeId);
  }


  /**
   * Update the state of the Namenode.
   */
  private void updateState() {
    NamenodeStatusReport report = getNamenodeStatusReport();
    if (!report.registrationValid()) {
      // Not operational
      LOG.error("Namenode is not operational: " + getNamenodeDesc());
    } else if (report.haStateValid()) {
      // block and HA status available
      LOG.debug("Received service state: " + report.getState()
          + " from HA namenode: " + getNamenodeDesc());
    } else if (localTarget == null) {
      // block info available, HA status not expected
      LOG.debug(
          "Reporting non-HA namenode as operational: " + getNamenodeDesc());
    } else {
      // block info available, HA status should be available, but was not
      // fetched do nothing and let the current state stand
      return;
    }
    try {
      if (!resolver.registerNamenode(report)) {
        LOG.warn("Unable to register namenode " + report);
      }
    } catch (IOException e) {
      LOG.info("Unable to register namenode with state store.");
    } catch (Exception ex) {
      LOG.error("Unhandled exception updating NN registration for "
          + getNamenodeDesc(), ex);
    }
  }

  /**
   * Get the status report for the Namenode monitored by this heartbeater.
   * @return Namenode status report.
   */
  protected NamenodeStatusReport getNamenodeStatusReport() {
    NamenodeStatusReport report = new NamenodeStatusReport(nameserviceId,
        namenodeId, serviceAddress, webAddress);

    try {
      LOG.debug("Probing NN at service address: " + serviceAddress);

      URI serviceURI = new URI("hdfs://" + serviceAddress);
      // Read the filesystem info from RPC (required)
      NamenodeProtocol nn = NameNodeProxies
          .createProxy(this.conf, serviceURI, NamenodeProtocol.class)
          .getProxy();

      if (nn != null) {
        NamespaceInfo info = nn.versionRequest();
        if (info != null) {
          report.setNamespaceInfo(info);
        }
      }
      if (!report.registrationValid()) {
        return report;
      }

      // Check for safemode from the client protocol. Currently optional, but
      // should be required at some point for QoS
      try {
        ClientProtocol client = NameNodeProxies
            .createProxy(this.conf, serviceURI, ClientProtocol.class)
            .getProxy();
        if (client != null) {
          boolean isSafeMode = client.setSafeMode(
              SafeModeAction.SAFEMODE_GET, false);
          report.setSafeMode(isSafeMode);
        }
      } catch (Exception e) {
        LOG.error("Unable to fetch safemode state for " + getNamenodeDesc(), e);
      }

      // Read the stats from JMX (optional)
      updateJMXParameters(webAddress, report);

      if (localTarget != null) {
        // Try to get the HA status
        try {
          // Determine if NN is active
          // TODO: dynamic timeout
          HAServiceProtocol haProtocol = localTarget.getProxy(conf, 30*1000);
          HAServiceStatus status = haProtocol.getServiceStatus();
          report.setHAServiceState(status.getState());
        } catch (Throwable e) {
          // TODO: Debug This
          // Failed to fetch HA status, ignoring failure
          LOG.error("Unable to fetch HA status for "
              + getNamenodeDesc() + ": " + e.getMessage(), e);
        }
      }
    } catch(IOException e) {
      LOG.error("Unable to communicate with " + getNamenodeDesc() + ": " +
          e.getMessage());
    } catch(Throwable e) {
      // Generic error that we don't know about
      LOG.error("Unexpected exception while communicating with "
          + getNamenodeDesc() + ": " + e.getMessage(), e);
    }
    return report;
  }

  /**
   * Get the description of the Namenode to monitor.
   * @return Description of the Namenode to monitor.
   */
  public String getNamenodeDesc() {
    if (namenodeId != null && !namenodeId.isEmpty()) {
      return nameserviceId + "-" + namenodeId + ":" + serviceAddress;
    } else {
      return nameserviceId + ":" + serviceAddress;
    }
  }

  /**
   * Get the parameters for a Namenode from JMX and add them to the report.
   * @param webAddress Web interface of the Namenode to monitor.
   * @param report Namenode status report to update with JMX data.
   */
  private void updateJMXParameters(
      String address, NamenodeStatusReport report) {
    try {
      // TODO part of this should be moved to its own utility
      String query = "Hadoop:service=NameNode,name=FSNamesystem*";
      JSONArray aux = FederationUtil.getJmx(query, address);
      if (aux != null) {
        for (int i = 0; i < aux.length(); i++) {
          JSONObject jsonObject = aux.getJSONObject(i);
          String name = jsonObject.getString("name");
          if (name.equals("Hadoop:service=NameNode,name=FSNamesystemState")) {
            report.setDatanodeInfo(
                jsonObject.getInt("NumLiveDataNodes"),
                jsonObject.getInt("NumDeadDataNodes"),
                jsonObject.getInt("NumDecomLiveDataNodes"));
          } else if (name.equals(
              "Hadoop:service=NameNode,name=FSNamesystem")) {
            report.setNamesystemInfo(
                jsonObject.getLong("CapacityRemaining"),
                jsonObject.getLong("CapacityTotal"),
                jsonObject.getLong("FilesTotal"),
                jsonObject.getLong("BlocksTotal"),
                jsonObject.getLong("MissingBlocks"),
                jsonObject.getLong("PendingReplicationBlocks"),
                jsonObject.getLong("UnderReplicatedBlocks"),
                jsonObject.getLong("PendingDeletionBlocks"));
          }
        }
      }
    } catch (Exception e) {
      LOG.error(
          "Unable to get stat from " + getNamenodeDesc() + " using JMX", e);
    }
  }
}
