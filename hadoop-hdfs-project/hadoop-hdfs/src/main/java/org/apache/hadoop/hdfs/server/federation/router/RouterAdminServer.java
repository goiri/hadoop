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


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.RouterAdminProtocol;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.RouterAdminProtocolService;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceStatsStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.service.AbstractService;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

/**
 * This class is responsible for handling all of the Admin calls to the HDFS
 * router. It is created, started, and stopped by {@link Router}.
 */
public class RouterAdminServer extends AbstractService
    implements RouterAdminProtocol, FederationMountTableStore {
  private static final Log LOG = LogFactory.getLog(RouterAdminServer.class);

  private Configuration conf;
  private final Router router;
  private FederationMountTableStore mountTableStore;
  private FederationNamespaceStatsStore statsStore;

  /** The Admin server that listens to requests from clients. */
  private final Server adminServer;
  private final InetSocketAddress adminAddress;

  public RouterAdminServer(Configuration conf, Router router)
      throws IOException {
    super(RouterAdminServer.class.getName());

    this.conf = conf;
    this.router = router;

    int handlerCount = this.conf.getInt(
        DFSConfigKeys.DFS_ROUTER_ADMIN_HANDLER_COUNT_KEY,
        DFSConfigKeys.DFS_ROUTER_ADMIN_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(this.conf, RouterAdminProtocolPB.class,
        ProtobufRpcEngine.class);

    RouterAdminProtocolServerSideTranslatorPB routerAdminProtocolTranslator =
        new RouterAdminProtocolServerSideTranslatorPB(this);
    BlockingService clientNNPbService = RouterAdminProtocolService.
        newReflectiveBlockingService(routerAdminProtocolTranslator);

    WritableRpcEngine.ensureInitialized();

    InetSocketAddress confRpcAddress =
        FederationUtil.getRouterAdminServerAddress(conf);
    String bindHost = FederationUtil.getRouterAdminServerBindHost(conf);
    if (bindHost == null) {
      bindHost = confRpcAddress.getHostName();
    }
    LOG.info("Admin server binding to " + bindHost + ":" +
        confRpcAddress.getPort());

    this.adminServer = new RPC.Builder(this.conf)
        .setProtocol(
            org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(bindHost)
        .setPort(confRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();

    // The RPC-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.adminServer.getListenerAddress();
    this.adminAddress = new InetSocketAddress(
        confRpcAddress.getHostName(), listenAddress.getPort());
    router.setAdminServerAddress(this.adminAddress);
  }

  /** Allow access to the client RPC server for testing. */
  @VisibleForTesting
  Server getAdminServer() {
    return this.adminServer;
  }

  private FederationMountTableStore getMountTableStore() throws IOException {
    if (this.mountTableStore == null) {
      this.mountTableStore = router.getStateStore()
          .getRegisteredInterface(FederationMountTableStore.class);
      if (this.mountTableStore == null) {
        throw new IOException("Mount table state store is not available.");
      }
    }
    return this.mountTableStore;
  }

  private FederationNamespaceStatsStore getStatsStore() throws IOException {
    if (this.statsStore == null) {
      this.statsStore = router.getStateStore()
          .getRegisteredInterface(FederationNamespaceStatsStore.class);
      if(this.statsStore == null) {
        throw new IOException("Namespace stats state store is not available.");
      }
    }
    return this.statsStore;
  }

  /**
   * Get the RPC address of the admin service.
   * @return Administration service RPC address.
   */
  public InetSocketAddress getRpcAddress() {
    return this.adminAddress;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    this.adminServer.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.adminServer != null) {
      this.adminServer.stop();
    }
    super.serviceStop();
  }

  @Override
  public PathTree<Long> getPathStats(long timeWindow, boolean max)
      throws IOException {
    return getStatsStore().getPathStats(timeWindow, max);
  }

  @Override
  public void resetPerfCounters(long flags) throws IOException {

    if ((flags
        & RouterAdmin.ROUTER_PERF_FLAG_RPC) ==
        RouterAdmin.ROUTER_PERF_FLAG_RPC) {
      LOG.info("Reseting router perf flag rpc.");
      router.restartRpcServer();
    }
    if ((flags
        & RouterAdmin.ROUTER_PERF_FLAG_STATESTORE) ==
        RouterAdmin.ROUTER_PERF_FLAG_STATESTORE) {
      // TODO
      LOG.info("Reseting router perf flag statestore.");
    }
    if ((flags
        & RouterAdmin.ROUTER_PERF_FLAG_ERRORS) ==
        RouterAdmin.ROUTER_PERF_FLAG_ERRORS) {
      // TODO
      LOG.info("Reseting router perf flag errors.");
    }
  }

  @Override
  public void setConfiguration(String key, String value) throws IOException {
    Configuration newConf = new Configuration(false);
    newConf.set(key, value);
    router.updateConfiguration(newConf);
  }

  @Override
  public void restartRpcServer() throws IOException {
    router.restartRpcServer();
  }

  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException {
    return getMountTableStore().addMountTableEntry(request);
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException {
    return getMountTableStore().updateMountTableEntry(request);
  }

  @Override
  public RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException {
    return getMountTableStore().removeMountTableEntry(request);
  }

  @Override
  public GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request)
      throws IOException {
    return getMountTableStore().getMountTableEntries(request);
  }
}
