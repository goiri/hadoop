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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.service.Service.STATE;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The the safe mode for the {@link Router} controlled by
 * {@link SafeModeTimer}.
 */
public class TestRouter {

  private static Configuration conf;

  @BeforeClass
  public static void create() throws IOException {
    // Basic configuration without the state store
    conf = new Configuration();
    // 5 sec startup standby
    conf.setInt(DFSConfigKeys.DFS_ROUTER_SAFEMODE_STARTUP_INTERVAL, 2);
    // 1 sec cache refresh
    conf.setInt(DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY, 1);
    // 2 sec post cache update before entering safemode (2 intervals)
    conf.setInt(DFSConfigKeys.DFS_ROUTER_SAFEMODE_CACHE_EXPIRATION_SECS, 2);
    // Mock resolver classes
    conf.set(DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MockResolver.class.getCanonicalName());
    conf.set(DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MockResolver.class.getCanonicalName());

    // Bind to any available port
    conf.set(DFSConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY, "0.0.0.0");
    conf.set(DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY, "0.0.0.0");
    conf.set(DFSConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY, "0.0.0.0");

    // Simulate a co-located NN
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns0");
    conf.set("fs.defaultFS", "hdfs://" + "ns0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + "ns0",
            "127.0.0.1:0" + 0);
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY + "." + "ns0",
            "127.0.0.1:" + 0);
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY + "." + "ns0",
            "0.0.0.0");
  }

  @AfterClass
  public static void destroy() {
  }

  @Before
  public void setup() throws IOException, URISyntaxException {
  }

  @After
  public void cleanup() {
  }

  private static void testRouterStartup(Configuration routerConfig)
      throws InterruptedException, IOException {
    Router router = new Router();
    assertEquals(RouterServiceState.NONE, router.getRouterState());
    router.init(routerConfig);
    if (routerConfig.getBoolean(
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT)) {
      assertEquals(RouterServiceState.SAFEMODE, router.getRouterState());
    } else {
      assertEquals(RouterServiceState.INITIALIZING, router.getRouterState());
    }
    assertEquals(STATE.INITED, router.getServiceState());
    router.start();
    if (routerConfig.getBoolean(
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT)) {
      assertEquals(RouterServiceState.SAFEMODE, router.getRouterState());
    } else {
      assertEquals(RouterServiceState.RUNNING, router.getRouterState());
    }
    assertEquals(STATE.STARTED, router.getServiceState());
    router.stop();
    assertEquals(RouterServiceState.SHUTDOWN, router.getRouterState());
    assertEquals(STATE.STOPPED, router.getServiceState());
    router.close();
  }

  @Test
  public void testRouterService() throws InterruptedException, IOException {

    // Admin only
    testRouterStartup((new RouterConfigBuilder(conf)).admin().build());

    // Http only
    testRouterStartup((new RouterConfigBuilder(conf)).http().build());

    // Rpc only
    testRouterStartup((new RouterConfigBuilder(conf)).rpc().build());

    // Safemode only
    testRouterStartup((new RouterConfigBuilder(conf)).rpc().safemode().build());

    // Metrics only
    testRouterStartup((new RouterConfigBuilder(conf)).metrics().build());

    // Statestore only
    testRouterStartup((new RouterConfigBuilder(conf)).stateStore().build());

    // Heartbeat only
    testRouterStartup((new RouterConfigBuilder(conf)).heartbeat().build());

    // Run with all services
    testRouterStartup((new RouterConfigBuilder(conf)).all().build());
  }

  @Test
  public void testRouterRestartRpcService() throws IOException {

    // Start
    Router router = new Router();
    router.init((new RouterConfigBuilder(conf)).rpc().build());
    router.start();

    // Verify RPC server is running
    assertNotNull(router.getRpcServerAddress());
    RouterRpcServer rpcServer = router.getRpcServer();
    assertNotNull(rpcServer);
    assertEquals(STATE.STARTED, rpcServer.getServiceState());

    // Restart RPC server
    router.restartRpcServer();
    RouterRpcServer newServer = router.getRpcServer();
    assertNotNull(newServer);
    assertTrue(newServer != rpcServer);
    assertEquals(STATE.STARTED, newServer.getServiceState());

    // Stop router and rpc server
    router.stop();
    assertEquals(STATE.STOPPED, newServer.getServiceState());

    router.close();
  }
}
