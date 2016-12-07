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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
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
public class TestRouterSafemode {

  private Router router;
  private static Configuration conf;

  @BeforeClass
  public static void create() throws IOException {
    // Wipe state store
    FederationStateStoreTestUtils.deleteStateStore();
    // Configuration that supports the state store
    conf = FederationStateStoreTestUtils.generateStateStoreConfiguration();
    // 5 sec startup stanby
    conf.setInt(DFSConfigKeys.DFS_ROUTER_SAFEMODE_STARTUP_INTERVAL, 5);
    // 1 sec cache refresh
    conf.setInt(DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY, 1);
    // 2 sec post cache update before entering safemode (2 intervals)
    conf.setInt(DFSConfigKeys.DFS_ROUTER_SAFEMODE_CACHE_EXPIRATION_SECS, 2);
    RouterConfigBuilder builder = new RouterConfigBuilder(conf);
    // RPC + statestore + safemode only
    conf = builder.rpc().safemode().stateStore().build();
  }

  @AfterClass
  public static void destroy() {
  }

  @Before
  public void setup() throws IOException, URISyntaxException {

    // Create new router
    router = new Router();
    router.init(conf);
    router.start();
  }

  @After
  public void cleanup() throws IOException {
    router.stop();
  }

  @Test
  public void testSafemodeService() throws IOException {
    RouterSafemodeService server = new RouterSafemodeService(router);
    server.init(conf);
    assertEquals(STATE.INITED, server.getServiceState());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    server.close();
  }

  private void verifyRouter(RouterServiceState status)
      throws IllegalStateException, IOException {
    assertEquals(status, router.getRouterState());
  }

  @Test
  public void testRouterExitSafemode()
      throws InterruptedException, IllegalStateException, IOException {
    assertTrue(router.getRpcServer().isSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);

    // Wait for initial time
    Thread.sleep((conf
        .getInt(DFSConfigKeys.DFS_ROUTER_SAFEMODE_STARTUP_INTERVAL, 10)
        + conf.getInt(DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY, 10))
        * 1000);

    assertFalse(router.getRpcServer().isSafeMode());
    verifyRouter(RouterServiceState.RUNNING);
  }

  @Test
  public void testRouterEnterSafemode()
      throws InterruptedException, IllegalStateException, IOException {

    // Verify starting state
    assertTrue(router.getRpcServer().isSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);

    // Wait for initial time
    Thread.sleep((conf
        .getInt(DFSConfigKeys.DFS_ROUTER_SAFEMODE_STARTUP_INTERVAL, 10)
        + conf.getInt(DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY, 10))
        * 1000);

    // Running
    assertFalse(router.getRpcServer().isSafeMode());
    verifyRouter(RouterServiceState.RUNNING);

    // Disable cache
    router.getStateStore().stopCacheUpdateService();

    // Wait until the state store cache is stale.
    Thread.sleep((conf.getInt(
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_CACHE_EXPIRATION_SECS +
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY, 10)
        + conf.getInt(DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY, 10))
            * 1000);

    // Safemode
    assertTrue(router.getRpcServer().isSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);
  }
}
