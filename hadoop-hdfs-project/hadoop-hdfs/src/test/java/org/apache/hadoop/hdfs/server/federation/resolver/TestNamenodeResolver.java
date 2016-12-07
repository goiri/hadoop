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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link ActiveNamenodeResolver} functionality.
 */
public class TestNamenodeResolver {

  private static FederationStateStoreService stateStore;
  private static ActiveNamenodeResolver namenodeResolver;

  @BeforeClass
  public static void create() throws Exception {

    Configuration conf = FederationStateStoreTestUtils.generateStateStoreConfiguration();
    // Reduce expirations to 5 seconds
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_REGISTRATION_EXPIRATION_SEC,
        5);
    stateStore = FederationStateStoreService.createStateStore(conf);
    assertNotNull(stateStore);
    namenodeResolver = new MembershipNamenodeResolver(conf, stateStore);
    namenodeResolver.setRouterId(FederationTestUtils.ROUTER1);
  }

  @AfterClass
  public static void destroy() throws Exception {
    stateStore.stop();
    stateStore.close();
  }

  @Before
  public void setup() throws IOException, InterruptedException {

    // Wait for state store to connect
    stateStore.loadDriver();
    FederationStateStoreService.waitStateStore(stateStore, 10000);

    // Clear NN registrations
    assertTrue(FederationStateStoreTestUtils.clearRecords(stateStore, MembershipState.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Add an entry to the store
    NamenodeStatusReport report = FederationTestUtils.createNamenodeReport(
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1,
        HAServiceState.ACTIVE);
    assertTrue(
        namenodeResolver.registerNamenode(report));

    // Close the data store driver
    stateStore.closeDriver();
    assertEquals(false, stateStore.isDriverReady());

    // Flush the caches
    stateStore.refreshCaches();

    // Verify commands fail due to no cached data and no state store
    // connectivity.
    assertEquals(0, namenodeResolver
        .getPrioritizedNamenodesForBlockPoolId(FederationTestUtils.NAMESERVICE1)
        .size());

    FederationTestUtils.verifyException(namenodeResolver, "registerNamenode",
        StateStoreUnavailableException.class,
        new Class[] { NamenodeStatusReport.class }, new Object[] { report });
  }

  private void verifyFirstRegistration(String ns, String nn, int resultsCount,
      FederationNamenodeServiceState state) throws IOException {
    List<? extends FederationNamenodeContext> namenodes =
        namenodeResolver.getPrioritizedNamenodesForNameserviceId(ns);
    assertEquals(resultsCount, namenodes.size());
    if (namenodes.size() > 0) {
      assertEquals(state, namenodes.get(0).getState());
      assertEquals(nn, namenodes.get(0).getNamenodeId());
    }
  }

  @Test
  public void testRegistrationExpired()
      throws InterruptedException, IOException {

    // Populate the state store with a single NN element
    // 1) ns0:nn0 - Active
    // Wait for the entry to expire without heartbeating
    // Verify the NN entry is not accessible once expired.

    NamenodeStatusReport report = FederationTestUtils.createNamenodeReport(
        FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE1, HAServiceState.ACTIVE);
    assertTrue(
        namenodeResolver.registerNamenode(report));

    // Load cache
    stateStore.refreshCaches();

    // Verify
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE1, 1,
        FederationNamenodeServiceState.ACTIVE);

    // Wait past expiration (set in conf to 5 seconds)
    Thread.sleep(6000);
    // Reload cache
    stateStore.refreshCaches();

    // Verify entry is now expired and is no longer in the cache
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE1, 0,
        FederationNamenodeServiceState.ACTIVE);

    // Heartbeat again, updates dateModified
    assertTrue(
        namenodeResolver
        .registerNamenode(report));
    // Reload cache
    stateStore.refreshCaches();

    // Verify updated entry is marked as active again and is accessible to rpc
    // server
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE1, 1,
        FederationNamenodeServiceState.ACTIVE);
  }

  
  @Test
  public void testRegistrationNamenodeSelection()
      throws InterruptedException, IOException {

    // 1) ns0:nn0 - Active
    // 2) ns0:nn1 - Standby (newest)
    // Verify the selected entry is the active entry
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE1, HAServiceState.ACTIVE)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE2, HAServiceState.STANDBY)));

    stateStore.refreshCaches();

    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE1, 2,
        FederationNamenodeServiceState.ACTIVE);
    
    // 1) ns0:nn0 - Expired (stale)
    // 2) ns0:nn1 - Standby (newest)
    // Verify the selected entry is the standby entry as the active entry is
    // stale
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE1, HAServiceState.ACTIVE)));

    // Expire active registration
    Thread.sleep(6000);

    // Refresh standby registration
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE2, HAServiceState.STANDBY)));

    // Verify that standby is selected (active is now expired)
    stateStore.refreshCaches();
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE2, 1,
        FederationNamenodeServiceState.STANDBY);

    // 1) ns0:nn0 - Active
    // 2) ns0:nn1 - Unavailable (newest)
    // Verify the selected entry is the active entry
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE1,
                HAServiceState.ACTIVE)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
                FederationTestUtils.NAMESERVICE1,
                FederationTestUtils.NAMENODE2, null)));
    stateStore.refreshCaches();
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE1, 2,
        FederationNamenodeServiceState.ACTIVE);

    // 1) ns0:nn0 - Unavailable (newest)
    // 2) ns0:nn1 - Standby
    // Verify the selected entry is the standby entry
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE2,
                HAServiceState.STANDBY)));
    Thread.sleep(1000);
    assertTrue(namenodeResolver.registerNamenode(
            FederationTestUtils.createNamenodeReport(
                FederationTestUtils.NAMESERVICE1,
                FederationTestUtils.NAMENODE1, null)));

    stateStore.refreshCaches();
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE2, 2,
        FederationNamenodeServiceState.STANDBY);

    // 1) ns0:nn0 - Active (oldest)
    // 2) ns0:nn1 - Standby 
    // 3) ns0:nn2 - Active (newest)
    // Verify the selected entry is the newest active entry
    assertTrue(namenodeResolver.registerNamenode(
            FederationTestUtils.createNamenodeReport(
                FederationTestUtils.NAMESERVICE1,
                FederationTestUtils.NAMENODE1, null)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE2,
                HAServiceState.STANDBY)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE3,
                HAServiceState.ACTIVE)));

    stateStore.refreshCaches();
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE3, 3,
        FederationNamenodeServiceState.ACTIVE);

    // 1) ns0:nn0 - Standby (oldest)
    // 2) ns0:nn1 - Standby (newest)
    // 3) ns0:nn2 - Standby
    // Verify the selected entry is the newest standby entry
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE1,
                HAServiceState.STANDBY)));
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE3,
                HAServiceState.STANDBY)));
    Thread.sleep(1500);
    assertTrue(namenodeResolver.registerNamenode(
        FederationTestUtils.createNamenodeReport(
            FederationTestUtils.NAMESERVICE1,
            FederationTestUtils.NAMENODE2,
                HAServiceState.STANDBY)));

    stateStore.refreshCaches();
    verifyFirstRegistration(FederationTestUtils.NAMESERVICE1,
        FederationTestUtils.NAMENODE2, 3,
        FederationNamenodeServiceState.STANDBY);
  }
}
