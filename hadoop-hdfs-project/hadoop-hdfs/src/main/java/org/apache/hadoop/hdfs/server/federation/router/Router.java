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

import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.NamenodeBeanMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.FederationPathLockStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;

/**
 * Router that provides a unified view of multiple federated HDFS clusters. It
 * has two main roles: (1) federated interface and (2) NameNode heartbeat.
 * <p>
 * For the federated interface, the Router receives a client request, checks the
 * State Store for the correct subcluster, and forwards the request to the
 * active Namenode of that subcluster. The reply from the Namenode then flows in
 * the opposite direction. The Routers are stateless and can be behind a load
 * balancer. HDFS clients connect to the router using the same interfaces as are
 * used to communicate with a namenode, namely the ClientProtocol RPC interface
 * and the WebHdfs HTTP interface exposed by the router. {@link RouterRpcServer}
 * {@link RouterHttpServer}
 * <p>
 * For NameNode heartbeat, the Router periodically checks the state of a
 * NameNode (usually on the same server) and reports their high availability
 * (HA) state and load/space status to the State Store. Note that this is an
 * optional role as a Router can be independent of any subcluster.
 * {@link FederationStateStoreService} {@link NamenodeHeartbeatService}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Router extends CompositeService {

  private static final Log LOG = LogFactory.getLog(Router.class);

  private Configuration conf;

  /** RPC interface to the client. */
  private RouterRpcServer rpcServer;
  private InetSocketAddress rpcAddress;

  /** RPC interface for the admin. */
  private RouterAdminServer adminServer;
  private InetSocketAddress adminAddress;

  /** HTTP interface and web application. */
  private RouterHttpServer httpServer;

  /** Interface with the State Store. */
  private FederationStateStoreService stateStore;

  /** Interface to map global name space to HDFS subcluster name spaces. */
  private FileSubclusterResolver subclusterResolver;
  /** Tracks which path are locked for write. */
  private FederationPathLockStore pathLockManager;

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private ActiveNamenodeResolver namenodeResolver;
  /** Updates the namenode status in the namenode resolver. */
  private Collection<NamenodeHeartbeatService> namenodeHearbeatServices;

  /** Manages the current state of the router. */
  private FederationRouterStateStore routerStateManager;
  /** Heartbeat our run status to the router state manager. */
  private RouterHeartbeatService routerHeartbeatService;
  /** Enter/exit safemode. */
  private RouterSafemodeService safemodeService;

  /** Router metrics. */
  private static RouterMetrics metrics;
  /** Router JMX interface. */
  private FederationMetrics federationMetrics;
  /** Namenode JMX interface. */
  private NamenodeBeanMetrics nnMetrics;

  /** Router address/identifier. */
  private String routerId;

  /** The start time of the namesystem. */
  private final long startTime = now();

  /** State of the Router. */
  private RouterServiceState state = RouterServiceState.NONE;


  /** Usage string for help message. */
  private static final String USAGE = "Usage: java Router";

  /** Shutdown hook to stop the Router gracefully. */
  private static CompositeServiceShutdownHook routerShutdownHook;

  /** Priority of the Router shutdown hook. */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;


  /////////////////////////////////////////////////////////
  // Constructor
  /////////////////////////////////////////////////////////

  public Router() {
    super(Router.class.getName());
  }

  /////////////////////////////////////////////////////////
  // Service management
  /////////////////////////////////////////////////////////

  /**
   * Initialize and start the Router.
   *
   * @param conf Configuration for the Router.
   * @param hasToReboot If it has to reboot.
   */
  protected void initAndStartRouter(
      Configuration configuration, boolean hasToReboot) {
    try {
      // Remove the old hook if we are rebooting.
      if (hasToReboot && null != routerShutdownHook) {
        ShutdownHookManager.get().removeShutdownHook(routerShutdownHook);
      }

      routerShutdownHook = new CompositeServiceShutdownHook(this);
      ShutdownHookManager.get().addShutdownHook(
          routerShutdownHook, SHUTDOWN_HOOK_PRIORITY);

      this.init(configuration);
      this.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting Router", t);
      System.exit(-1);
    }
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;
    updateRouterState(RouterServiceState.INITIALIZING);

    if (conf.getBoolean(
        DFSConfigKeys.DFS_ROUTER_STATESTORE_ENABLE,
        DFSConfigKeys.DFS_ROUTER_STATESTORE_ENABLE_DEFAULT)) {
      // Interface to the State Store
      this.stateStore = new FederationStateStoreService();
      addService(this.stateStore);

      // Periodically update the router state
      this.routerHeartbeatService = new RouterHeartbeatService(this);
      addService(this.routerHeartbeatService);
    }

    // Resolver to track active NNs
    this.namenodeResolver = FederationUtil.createActiveNamenodeResolver(
        this.conf, this.stateStore, FederationStateStoreService.class);
    if (this.namenodeResolver == null) {
      throw new IOException("Unable to find namenode resolver.");
    }

    if (conf.getBoolean(
        DFSConfigKeys.DFS_ROUTER_RPC_ENABLE,
        DFSConfigKeys.DFS_ROUTER_RPC_ENABLE_DEFAULT)) {

      // Lookup interface to map between the global and subcluster name spaces
      this.subclusterResolver = FederationUtil.createFileSubclusterResolver(
          this.conf, this.stateStore, FederationStateStoreService.class);
      if (this.subclusterResolver == null) {
        throw new IOException("Unable to find subcluster resolver");
      }

      // Safemode service to refuse RPC calls when the router is out of sync
      if (conf.getBoolean(
          DFSConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
          DFSConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT)) {
        // Create safemode monitoring service
        this.safemodeService = new RouterSafemodeService(this);
        addService(this.safemodeService);
      }

      // Create RPC server
      this.rpcServer = createRpcServer();
      addService(this.rpcServer);
      this.setRpcServerAddress(rpcServer.getRpcAddress());
    }

    if (conf.getBoolean(DFSConfigKeys.DFS_ROUTER_ADMIN_ENABLE,
        DFSConfigKeys.DFS_ROUTER_ADMIN_ENABLE_DEFAULT)) {
      // Create admin server
      this.adminServer = createAdminServer();
      addService(this.adminServer);
    }

    if (conf.getBoolean(DFSConfigKeys.DFS_ROUTER_HTTP_ENABLE,
        DFSConfigKeys.DFS_ROUTER_HTTP_ENABLE_DEFAULT)) {
      // Create HTTP server
      this.httpServer = createHttpServer();
      addService(this.httpServer);
    }

    if (conf.getBoolean(DFSConfigKeys.DFS_ROUTER_HEARTBEAT_ENABLE,
        DFSConfigKeys.DFS_ROUTER_HEARTBEAT_ENABLE_DEFAULT)) {
      // Create status updater for each monitored Namenode
      this.namenodeHearbeatServices = createNamenodeHearbeatServices();
      for (NamenodeHeartbeatService hearbeatService :
        this.namenodeHearbeatServices) {
        addService(hearbeatService);
      }
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {

    super.serviceStart();
    LOG.info("Router has started");

    if (conf.getBoolean(
        DFSConfigKeys.DFS_ROUTER_METRICS_ENABLE,
        DFSConfigKeys.DFS_ROUTER_METRICS_ENABLE_DEFAULT)) {

      Router.initMetrics(conf);

      // Wrapper for all the FSNamesystem JMX interfaces
      this.nnMetrics = new NamenodeBeanMetrics(this);

      // Federation MBean JMX interface
      this.federationMetrics = new FederationMetrics(this);
    }

    if (this.safemodeService == null) {
      // Router is running now
      updateRouterState(RouterServiceState.RUNNING);
    }

    if (this.rpcServer != null) {
      LOG.info("Router RPC up at: " + this.rpcServer.getRpcAddress());
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // Remove JMX interfaces
    if (this.federationMetrics != null) {
      this.federationMetrics.close();
    }

    // Remove Namenode JMX interfaces
    if (this.nnMetrics != null) {
      this.nnMetrics.close();
    }

    // Shutdown metrics
    if (metrics != null) {
      metrics.shutdown();
    }

    // Update state
    updateRouterState(RouterServiceState.SHUTDOWN);
    LOG.info("Router has shutdown");

    // Shutdown other services
    super.serviceStop();
  }

  /**
   * Shutdown the router.
   */
  public void shutDown() {
    new Thread() {
      @Override
      public void run() {
        Router.this.stop();
      }
    }.start();
  }

  /**
   * Main run loop for the router.
   *
   * @param argv parameters.
   */
  public static void main(String[] argv) {
    if (DFSUtil.parseHelpArgument(argv, Router.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(Router.class, argv, LOG);
      Router router = createRouter(argv, null);
      if (router != null) {
        router.join();
      }
    } catch (Throwable e) {
      LOG.error("Failed to start router.", e);
      terminate(1, e);
    }
  }

  public static Router createRouter(String argv[], Configuration conf)
      throws IOException {
    LOG.info("createRouter " + Arrays.asList(argv));

    if (conf == null){
      conf = new HdfsConfiguration();
    }

    DefaultMetricsSystem.initialize("Router");
    Router router = new Router();
    router.initAndStartRouter(conf, false);
    return router;
  }

  public void join() {
    try {
      this.rpcServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Caught interrupted exception ", ie);
    }
  }

  static void initMetrics(Configuration conf) {
    metrics = RouterMetrics.create(conf);
  }

  public static RouterMetrics getRouterMetrics() {
    return metrics;
  }

  /////////////////////////////////////////////////////////
  // RPC Server
  /////////////////////////////////////////////////////////

  /**
   * Create a new Router RPC server to proxy ClientProtocol requests.
   *
   * @return RouterRpcServer
   * @throws IOException If the router RPC server was not started.
   */
  protected RouterRpcServer createRpcServer() throws IOException {
    return new RouterRpcServer(this.conf, this.getNamenodeResolver(),
        this.getSubclusterResolver(), this.getPathLockManager());
  }

  /**
   * Get the Router RPC server.
   *
   * @return Router RPC server.
   */
  public RouterRpcServer getRpcServer() {
    return this.rpcServer;
  }

  /**
   * Set the current RPC socket for the router.
   *
   * @param rpcAddress RPC address.
   */
  protected void setRpcServerAddress(InetSocketAddress address) {
    this.rpcAddress = address;
    // Use this address as our unique router Id
    if (this.rpcAddress != null) {
      try {
        setRouterId(InetAddress.getLocalHost().getHostName() + ":"
            + this.rpcAddress.getPort());
      } catch (UnknownHostException ex) {
        LOG.error("Unable to set unique router ID, address is not resolvable "
            + this.rpcAddress);
      }
    }
  }

  /**
   * Restarts an instance of the RPC server with the current conf settings.
   *
   * @throws IOException If the RPC server was not successfully restarted.
   */
  protected void restartRpcServer() throws IOException {
    this.rpcServer.stop();
    removeService(this.rpcServer);

    this.rpcServer = createRpcServer();
    addService(this.rpcServer);

    this.rpcServer.init(this.conf);
    this.rpcServer.start();
  }

  /**
   * Get the current RPC socket address for the router.
   *
   * @return InetSocketAddress
   */
  public InetSocketAddress getRpcServerAddress() {
    return this.rpcAddress;
  }

  /////////////////////////////////////////////////////////
  // Admin server
  /////////////////////////////////////////////////////////

  /**
   * Create a new router admin server to handle the router admin interface.
   *
   * @return RouterAdminServer
   * @throws IOException If the admin server was not successfully started.
   */
  protected RouterAdminServer createAdminServer() throws IOException {
    return new RouterAdminServer(this.conf, this);
  }

  /**
   * Set the current Admin socket for the router.
   *
   * @param adminAddress Admin RPC address.
   */
  protected void setAdminServerAddress(InetSocketAddress address) {
    this.adminAddress = address;
  }

  /**
   * Update a router configuration key via an admin protocol command. Used to
   * alter RPC server configuration settings via the admin CLI. Updates the
   * configuration for the currenttly running instance only.
   *
   * @param updatedKeys Keys to overwrite in the router configuration
   */
  protected void updateConfiguration(Configuration updatedKeys) {
    LOG.info("Updating configuration keys");
    Iterator<Map.Entry<String, String>> collection = updatedKeys.iterator();
    while (collection.hasNext()) {
      Map.Entry<String, String> entry = collection.next();
      LOG.info("    " + entry.getKey() + " - " + entry.getValue());
    }
    this.conf.addResource(updatedKeys);
  }

  /**
   * Get the current Admin socket address for the router.
   *
   * @return InetSocketAddress Admin address.
   */
  public InetSocketAddress getAdminServerAddress() {
    return adminAddress;
  }

  /////////////////////////////////////////////////////////
  // HTTP server
  /////////////////////////////////////////////////////////

  /**
   * Create an HTTP server for this Router.
   *
   * @return HTTP server for this Router.
   */
  protected RouterHttpServer createHttpServer() {
    return new RouterHttpServer(this);
  }

  /**
   * Get the current HTTP socket address for the router.
   *
   * @return InetSocketAddress HTTP address.
   */
  public InetSocketAddress getHttpServerAddress() {
    if (httpServer != null) {
      return httpServer.getHttpAddress();
    }
    return null;
  }

  /////////////////////////////////////////////////////////
  // Namenode heartbeat monitors
  /////////////////////////////////////////////////////////

  /**
   * Create each of the services that will monitor a Namenode.
   *
   * @return List of heartbeat services.
   */
  protected Collection<NamenodeHeartbeatService>
  createNamenodeHearbeatServices() {

    HashMap<String, NamenodeHeartbeatService> ret =
        new HashMap<String, NamenodeHeartbeatService>();

    if (conf.getBoolean(
        DFSConfigKeys.DFS_ROUTER_MONITOR_LOCAL_NAMENODE,
        DFSConfigKeys.DFS_ROUTER_MONITOR_LOCAL_NAMENODE_DEFAULT)) {
      // Create a local heartbet service
      NamenodeHeartbeatService localHeartbeatService =
          createLocalNamenodeHearbeatService();
      if (localHeartbeatService != null) {
        String nnDesc = localHeartbeatService.getNamenodeDesc();
        ret.put(nnDesc, localHeartbeatService);
      }
    }

    // Create heartbeat services for a list specified by the admin
    String namenodes = this.conf.get(
        DFSConfigKeys.DFS_ROUTER_MONITOR_NAMENODE);
    if (namenodes != null) {
      for (String namenode : namenodes.split(",")) {
        String[] namenodeSplit = namenode.split("\\.");
        String nsId = null;
        String nnId = null;
        if (namenodeSplit.length == 2) {
          nsId = namenodeSplit[0];
          nnId = namenodeSplit[1];
        } else if (namenodeSplit.length == 1) {
          nsId = namenode;
        } else {
          LOG.error("Wrong Namenode to monitor: " + namenode);
        }
        if (nsId != null) {
          NamenodeHeartbeatService heartbeatService =
              createNamenodeHearbeatService(nsId, nnId);
          if (heartbeatService != null) {
            ret.put(heartbeatService.getNamenodeDesc(), heartbeatService);
          }
        }
      }
    }

    return ret.values();
  }

  /**
   * Create a new status updater for the local Namenode.
   *
   * @return Updater of the status for the local Namenode.
   */
  protected NamenodeHeartbeatService createLocalNamenodeHearbeatService() {
    // Detect NN running in this machine
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String nnId = null;
    if (HAUtil.isHAEnabled(conf, nsId)) {
      nnId = HAUtil.getNameNodeId(conf, nsId);
      if (nnId == null) {
        LOG.error("Cannot find namenode id for local " + nsId);
      }
    }

    return createNamenodeHearbeatService(nsId, nnId);
  }

  /**
   * Create a heartbeat monitor for a particular Namenode.
   *
   * @param nsId Identifier of the nameservice to monitor.
   * @param nnId Identifier of the namenode (HA) to monitor.
   * @return Updater of the status for the specified Namenode.
   */
  protected NamenodeHeartbeatService createNamenodeHearbeatService(
      String nsId, String nnId) {

    if (nsId == null || nnId == null) {
      return null;
    }

    LOG.info("Creating heartbeat service for Namenode " + nnId + " in " + nsId);
    NamenodeHeartbeatService ret = new NamenodeHeartbeatService(
        namenodeResolver, nsId, nnId);
    return ret;
  }

  /////////////////////////////////////////////////////////
  // Router State Management
  /////////////////////////////////////////////////////////

  /**
   * Update the router state and heartbeat to the state store.
   *
   * @param state The new router state.
   */
  public void updateRouterState(RouterServiceState newState) {
    this.state = newState;
    if (this.routerHeartbeatService != null) {
      this.routerHeartbeatService.updateState();
    }
  }

  /**
   * Get the status of the router.
   *
   * @return Status of the router.
   */
  public RouterServiceState getRouterState() {
    return this.state;
  }

  /////////////////////////////////////////////////////////
  // Submodule getters
  /////////////////////////////////////////////////////////

  /**
   * Get the State Store service.
   *
   * @return State Store service.
   */
  public FederationStateStoreService getStateStore() {
    return this.stateStore;
  }

  /**
   * Get the federation metrics.
   *
   * @return Federation metrics.
   */
  public FederationMetrics getMetrics() {
    return this.federationMetrics;
  }

  /**
   * Get the subcluster resolver for files.
   *
   * @return Subcluster resolver for files.
   */
  public FileSubclusterResolver getSubclusterResolver() {
    return this.subclusterResolver;
  }

  /**
   * Get the namenode resolver for a subcluster.
   *
   * @return The namenode resolver for a subcluster.
   */
  public ActiveNamenodeResolver getNamenodeResolver() {
    return this.namenodeResolver;
  }

  /**
   * Get the state store interface for the router heartbeats.
   *
   * @return FederationRouterStateStore state store API handle.
   */
  public FederationRouterStateStore getRouterStateManager() {
    if (this.routerStateManager == null && this.stateStore != null) {
      this.routerStateManager = this.stateStore.getRegisteredInterface(
          FederationRouterStateStore.class);
    }
    return this.routerStateManager;
  }

  /**
   * Get the state store interface for path locks.
   *
   * @return FederationPathLockStore state store API handle.
   */
  public FederationPathLockStore getPathLockManager() {
    if (this.pathLockManager == null && this.stateStore != null) {
      this.pathLockManager = this.stateStore.getRegisteredInterface(
              FederationPathLockStore.class);
    }
    return this.pathLockManager;
  }

  /////////////////////////////////////////////////////////
  // Router info
  /////////////////////////////////////////////////////////

  /**
   * Get the start date of the Router.
   *
   * @return Start date of the router.
   */
  public long getStartTime() {
    return this.startTime;
  }

  /**
   * Unique ID for the router, typically the hostname:port string for the
   * router's RPC server. This ID may be null on router startup before the RPC
   * server has bound to a port.
   *
   * @return Router identifier.
   */
  public String getRouterId() {
    return this.routerId;
  }

  /**
   * Sets a unique ID for this router.
   *
   * @param router Identifier of the Router.
   */
  public void setRouterId(String router) {
    this.routerId = router;
    if (this.stateStore != null) {
      this.stateStore.setIdentifier(router);
    }
    if (this.namenodeResolver != null) {
      this.namenodeResolver.setRouterId(router);
    }
  }
}
