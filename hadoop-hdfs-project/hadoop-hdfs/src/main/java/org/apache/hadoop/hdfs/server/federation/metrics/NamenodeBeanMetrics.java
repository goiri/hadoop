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
package org.apache.hadoop.hdfs.server.federation.metrics;

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStoreUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNodeMXBean;
import org.apache.hadoop.hdfs.server.namenode.NameNodeStatusMXBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import com.google.common.collect.ImmutableMap;

/**
 * Expose the Namenode metrics as the Router was one.
 */
public class NamenodeBeanMetrics
    implements FSNamesystemMBean, NameNodeMXBean, NameNodeStatusMXBean {

  private static final Log LOG = LogFactory.getLog(NamenodeBeanMetrics.class);

  private Router router;

  /** FSNamesystem bean. */
  private ObjectName fsBeanName;
  /** FSNamesystemState bean. */
  private ObjectName fsStateBeanName;
  /** NameNodeInfo bean. */
  private ObjectName nnInfoBeanName;
  /** NameNodeStatus bean. */
  private ObjectName nnStatusBeanName;

  public NamenodeBeanMetrics(Router router) {
    this.router = router;

    try {
      // TODO this needs to be done with the Metrics from FSNamesystem
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      this.fsBeanName = MBeans.register("NameNode", "FSNamesystem", bean);
      LOG.info("Registered FSNamesystem MBean: " + this.fsBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad FSNamesystem MBean setup", e);
    }

    try {
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      this.fsStateBeanName =
          MBeans.register("NameNode", "FSNamesystemState", bean);
      LOG.info("Registered FSNamesystemState MBean: " + this.fsStateBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad FSNamesystemState MBean setup", e);
    }

    try {
      StandardMBean bean = new StandardMBean(this, NameNodeMXBean.class);
      this.nnInfoBeanName = MBeans.register("NameNode", "NameNodeInfo", bean);
      LOG.info("Registered NameNodeInfo MBean: " + this.nnInfoBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad NameNodeInfo MBean setup", e);
    }

    try {
      StandardMBean bean = new StandardMBean(this, NameNodeStatusMXBean.class);
      this.nnStatusBeanName =
          MBeans.register("NameNode", "NameNodeStatus", bean);
      LOG.info("Registered NameNodeStatus MBean: " + this.nnStatusBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad NameNodeStatus MBean setup", e);
    }
  }

  /**
   * De-register the JMX interfaces.
   */
  public void close() {
    if (fsStateBeanName != null) {
      MBeans.unregister(fsStateBeanName);
      fsStateBeanName = null;
    }
    if (nnInfoBeanName != null) {
      MBeans.unregister(nnInfoBeanName);
      nnInfoBeanName = null;
    }
    // Remove the NameNode status bean
    if (nnStatusBeanName != null) {
      MBeans.unregister(nnStatusBeanName);
      nnStatusBeanName = null;
    }
  }

  /////////////////////////////////////////////////////////
  // NameNodeMXBean
  /////////////////////////////////////////////////////////

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override
  public String getSoftwareVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public long getUsed() {
    return this.router.getMetrics().getUsedCapacity();
  }

  @Override
  public long getFree() {
    return this.router.getMetrics().getRemainingCapacity();
  }

  @Override
  public long getTotal() {
    return this.router.getMetrics().getTotalCapacity();
  }

  @Override
  public String getSafemode() {
    // We assume that the global federated view is never in safe mode
    return "";
  }

  @Override
  public boolean isUpgradeFinalized() {
    // We assume the upgrade is always finalized in a federated biew
    return true;
  }

  @Override
  public RollingUpgradeInfo.Bean getRollingUpgradeStatus() {
    return null;
  }

  @Override
  public long getNonDfsUsedSpace() {
    return 0;
  }

  @Override
  public float getPercentUsed() {
    return DFSUtil.getPercentUsed(getCapacityUsed(), getCapacityTotal());
  }

  @Override
  public float getPercentRemaining() {
    return DFSUtil.getPercentUsed(getCapacityRemaining(), getCapacityTotal());
  }

  @Override
  public long getCacheUsed() {
    return 0;
  }

  @Override
  public long getCacheCapacity() {
    return 0;
  }

  @Override
  public long getBlockPoolUsedSpace() {
    return 0;
  }

  @Override
  public float getPercentBlockPoolUsed() {
    return 0;
  }

  @Override
  public long getTotalBlocks() {
    return this.router.getMetrics().getNumBlocks();
  }

  @Override
  public long getNumberOfMissingBlocks() {
    return this.router.getMetrics().getNumOfMissingBlocks();
  }

  @Override
  public long getPendingReplicationBlocks() {
    return this.router.getMetrics().getNumOfBlocksPendingReplication();
  }

  @Override
  public long getUnderReplicatedBlocks() {
    return this.router.getMetrics().getNumOfBlocksUnderReplicated();
  }

  @Override
  public long getPendingDeletionBlocks() {
    return this.router.getMetrics().getNumOfBlocksPendingDeletion();
  }

  @Override
  public long getScheduledReplicationBlocks() {
    return -1;
  }

  @Override
  public long getTotalFiles() {
    return this.router.getMetrics().getNumFiles();
  }

  @Override
  public String getCorruptFiles() {
    return "TODO";
  }

  @Override
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  @Override
  public String getLiveNodes() {
    return this.getNodes(DatanodeReportType.LIVE);
  }

  @Override
  public String getDeadNodes() {
    return this.getNodes(DatanodeReportType.DEAD);
  }

  @Override
  public String getDecomNodes() {
    return this.getNodes(DatanodeReportType.DECOMMISSIONING);
  }

  /**
   * Get all the nodes in the federation from a particular type.
   * TODO this is expensive, we may want to cache it.
   * @param type Type of the datanodes to check.
   * @return JSON with the nodes.
   */
  private String getNodes(DatanodeReportType type) {
    final Map<String, Map<String, Object>> info =
        new HashMap<String, Map<String, Object>>();
    try {
      RouterRpcServer rpcServer = this.router.getRpcServer();
      DatanodeInfo[] datanodes = rpcServer.getDatanodeReport(type);
      for (DatanodeInfo node : datanodes) {
        ImmutableMap.Builder<String, Object> innerinfo =
            ImmutableMap.<String, Object> builder();
        innerinfo
            .put("infoAddr", node.getInfoAddr())
            .put("infoSecureAddr", node.getInfoSecureAddr())
            .put("xferaddr", node.getXferAddr())
            .put("location", node.getNetworkLocation())
            .put("lastContact", getLastContact(node))
            .put("usedSpace", node.getDfsUsed())
            .put("adminState", node.getAdminState().toString())
            .put("nonDfsUsedSpace", node.getNonDfsUsed())
            .put("capacity", node.getCapacity())
            .put("numBlocks", -1) // node.numBlocks()
            .put("version", (node.getSoftwareVersion() == null ?
                "UNKNOWN" : node.getSoftwareVersion()))
            .put("used", node.getDfsUsed())
            .put("remaining", node.getRemaining())
            .put("blockScheduled", -1) // node.getBlocksScheduled()
            .put("blockPoolUsed", node.getBlockPoolUsed())
            .put("blockPoolUsedPercent", node.getBlockPoolUsedPercent())
            .put("volfails", -1);
        info.put(node.getHostName() + ":" + node.getXferPort(),
            innerinfo.build());
      }
    } catch (StandbyException e) {
      LOG.error("Cannot get " + type + " nodes, Router in safe mode");
    } catch (IOException e) {
      LOG.error("Cannot get " + type + " nodes", e);
    }
    return JSON.toString(info);
  }

  @Override
  public String getClusterId() {
    try {
      return FederationMembershipStateStoreUtils
          .aggregateFederationNamespaceInfo(router.getStateStore()
              .getRegisteredInterface(FederationMembershipStateStore.class),
              "getClusterId")
          .toString();
    } catch (IOException e) {
      LOG.error("Unable to fetch cluster ID metrics - " + e.getMessage());
      return "";
    }
  }

  @Override
  public String getBlockPoolId() {
    try {
      return FederationMembershipStateStoreUtils
          .aggregateFederationNamespaceInfo(router.getStateStore()
              .getRegisteredInterface(FederationMembershipStateStore.class),
              "getBlockPoolId")
          .toString();
    } catch (IOException e) {
      LOG.error("Unable to fetch block pool ID metrics - " + e.getMessage());
      return "";
    }
  }

  @Override
  public String getNameDirStatuses() {
    return "TODO";
  }

  @Override
  public String getNodeUsage() {
    return "TODO";
  }

  @Override
  public String getNameJournalStatus() {
    return "TODO";
  }

  @Override
  public String getJournalTransactionInfo() {
    return "TODO";
  }

  @Override
  public String getNNStarted() {
    return new Date(this.router.getStartTime()).toString();
  }

  @Override
  public String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() +
        " from " + VersionInfo.getBranch();
  }

  @Override
  public int getDistinctVersionCount() {
    return 0;
  }

  @Override
  public Map<String, Integer> getDistinctVersions() {
    return null;
  }

  /////////////////////////////////////////////////////////
  // FSNamesystemMBean
  /////////////////////////////////////////////////////////

  @Override
  public String getFSState() {
    // We assume is not in safe mode
    return "Operational";
  }

  @Override
  public long getBlocksTotal() {
    return this.getTotalBlocks();
  }

  @Override
  public long getCapacityTotal() {
    return this.getTotal();
  }

  @Override
  public long getCapacityRemaining() {
    return this.getFree();
  }

  @Override
  public long getCapacityUsed() {
    return this.getUsed();
  }

  @Override
  public long getFilesTotal() {
    return this.getTotalFiles();
  }

  @Override
  public int getTotalLoad() {
    return -1;
  }

  @Override
  public int getNumLiveDataNodes() {
    return this.router.getMetrics().getNumLiveNodes();
  }

  @Override
  public int getNumDeadDataNodes() {
    return this.router.getMetrics().getNumDeadNodes();
  }

  @Override
  public int getNumStaleDataNodes() {
    return -1;
  }

  @Override
  public int getNumDecomLiveDataNodes() {
    return -1;
  }

  @Override
  public int getNumDecomDeadDataNodes() {
    return -1;
  }

  @Override
  public int getNumDecommissioningDataNodes() {
    return -1;
  }

  @Override
  public String getSnapshotStats() {
    return null;
  }

  @Override
  public long getMaxObjects() {
    return -1;
  }

  @Override
  public long getBlockDeletionStartTime() {
    return -1;
  }

  @Override
  public int getNumStaleStorages() {
    return -1;
  }

  private long getLastContact(DatanodeInfo node) {
    return (now() - node.getLastUpdate()) / 1000;
  }

  /////////////////////////////////////////////////////////
  // NameNodeStatusMXBean
  /////////////////////////////////////////////////////////

  @Override
  public String getNNRole() {
    return NamenodeRole.NAMENODE.toString();
  }

  @Override
  public String getState() {
    String state = HAServiceState.ACTIVE.toString();
    if (router.getRouterState() == RouterServiceState.SAFEMODE) {
      state = HAServiceState.STANDBY.toString();
    }
    return state;
  }

  @Override
  public String getHostAndPort() {
    return NetUtils.getHostPortString(router.getRpcServerAddress());
  }

  @Override
  public boolean isSecurityEnabled() {
    return false;
  }
}