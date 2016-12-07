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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Before;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationMountTableStore} functionality.
 */
public class TestMetricsBase {

  public static FederationStateStoreService stateStore;
  public static FederationMembershipStateStore membershipStore;
  public static FederationRouterStateStore routerStore;
  public static Router router;
  public static Configuration routerConfig;

  public static List<MembershipState> activeMemberships;
  public static List<MembershipState> standbyMemberships;
  public static List<RouterState> mockRouters;
  public static List<MountTable> mockMountTable;
  public static List<String> nameservices;

  @Before
  public void setupBase() throws Exception {

    if (router == null) {
      router = new Router();
      router.init(routerConfig);
      router.start();
      stateStore = router.getStateStore();

      routerStore = router.getStateStore()
          .getRegisteredInterface(FederationRouterStateStore.class);
      membershipStore = router.getStateStore()
          .getRegisteredInterface(FederationMembershipStateStore.class);

      // Read all data and load all caches
      FederationStateStoreService.waitStateStore(stateStore, 10000);
      createFixtures();
      stateStore.refreshCaches();
      Thread.sleep(1000);
    }
  }

  @AfterClass
  public static void tearDownBase() throws IOException {
    router.stop();
    router.close();
  }

  public static void createFixtures() throws IOException {

    // Clear all records
    FederationStateStoreTestUtils.clearAllRecords(stateStore);

    nameservices = new ArrayList<String>();
    nameservices.add(FederationTestUtils.NAMESERVICE1);
    nameservices.add(FederationTestUtils.NAMESERVICE2);

    // 2 NNs per NS
    activeMemberships = new ArrayList<MembershipState>();
    standbyMemberships = new ArrayList<MembershipState>();

    for (String nameservice : nameservices) {
      MembershipState namenode =
          FederationStateStoreTestUtils.createMockRegistrationForNamenode(
              nameservice, FederationTestUtils.NAMENODE1,
              FederationNamenodeServiceState.ACTIVE);
      assertTrue(FederationMembershipStateStoreUtils
          .sendNamenodeHeartbeat(membershipStore, namenode));
      activeMemberships.add(namenode);

      namenode =
          FederationStateStoreTestUtils.createMockRegistrationForNamenode(
              nameservice, FederationTestUtils.NAMENODE2,
              FederationNamenodeServiceState.STANDBY);
      assertTrue(FederationMembershipStateStoreUtils
          .sendNamenodeHeartbeat(membershipStore, namenode));
      standbyMemberships.add(namenode);
    }

    // Add 2 mount table memberships
    mockMountTable =
        FederationStateStoreTestUtils.createMockMountTable(nameservices);
    stateStore.synchronizeRecords(mockMountTable, MountTable.class);

    // Add 2 router memberships in addition to the runnning router.
    mockRouters = new ArrayList<RouterState>();
    assertTrue(FederationRouterStateStoreUtils.sendRouterHeartbeat(routerStore,
        "router1", RouterServiceState.RUNNING, Time.now()));
    RouterState router = FederationRouterStateStoreUtils
        .getRouterRegistration(routerStore, "router1");
    mockRouters.add(router);

    assertTrue(FederationRouterStateStoreUtils.sendRouterHeartbeat(routerStore,
        "router2", RouterServiceState.RUNNING, Time.now()));
    router = FederationRouterStateStoreUtils.getRouterRegistration(routerStore,
        "router2");
    mockRouters.add(router);
  }
}
