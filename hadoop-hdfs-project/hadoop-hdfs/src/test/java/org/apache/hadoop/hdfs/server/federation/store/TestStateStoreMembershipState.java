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
package org.apache.hadoop.hdfs.server.federation.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationMembershipStateStore} membership
 * functionality.
 */
public class TestStateStoreMembershipState extends TestStateStoreBase {

  private static FederationMembershipStateStore membershipStore;

  @BeforeClass
  public static void create() {
    // Reduce expirations to 5 seconds
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_REGISTRATION_EXPIRATION_SEC,
        5);
  }

  @Before
  public void setup() throws IOException, InterruptedException {

    membershipStore =
        stateStore.getRegisteredInterface(FederationMembershipStateStore.class);

    // Clear NN registrations
    assertTrue(FederationStateStoreTestUtils.clearRecords(stateStore,
        MembershipState.class));
  }

  @Test
  public void testNamenodeStateOverride() throws Exception {
    // Populate the state store
    // 1) ns0:nn0 - Standby
    String ns = "ns0";
    String nn = "nn0";
    MembershipState report = createRegistration(ns, nn,
        FederationTestUtils.ROUTER2, FederationNamenodeServiceState.STANDBY);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));

    // Load data into cache and calculate quorum
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));

    MembershipState existingState = FederationMembershipStateStoreUtils
        .getSingleMembership(membershipStore, ns, nn);
    assertEquals(FederationNamenodeServiceState.STANDBY,
        existingState.getState());

    // Override cache
    assertTrue(FederationMembershipStateStoreUtils.overrideNamenodeHeartbeat(
        membershipStore, ns, nn, FederationNamenodeServiceState.ACTIVE));

    MembershipState newState = FederationMembershipStateStoreUtils
        .getSingleMembership(membershipStore, ns, nn);
    assertEquals(FederationNamenodeServiceState.ACTIVE, newState.getState());
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    stateStore.closeDriver();
    assertEquals(false, stateStore.isDriverReady());

    NamenodeHeartbeatRequest hbRequest = FederationProtocolFactory
.newInstance(NamenodeHeartbeatRequest.class);
    hbRequest.setNamenodeMembership(
        FederationStateStoreTestUtils.createMockRegistrationForNamenode("test",
            "test", FederationNamenodeServiceState.UNAVAILABLE));
    FederationTestUtils.verifyException(membershipStore, "namenodeHeartbeat",
        StateStoreUnavailableException.class,
        new Class[] { NamenodeHeartbeatRequest.class },
        new Object[] { hbRequest });

    // Information from cache, no exception should be triggered for these
    // TODO - should cached info expire at some point?
    GetNamenodeRegistrationsRequest getRequest = FederationProtocolFactory
        .newInstance(GetNamenodeRegistrationsRequest.class);
    FederationTestUtils.verifyException(membershipStore,
        "getNamenodeRegistrations", null,
        new Class[] { GetNamenodeRegistrationsRequest.class },
        new Object[] { getRequest });

    FederationTestUtils.verifyException(membershipStore,
        "getExpiredNamenodeRegistrations", null,
        new Class[] {}, new Object[] {});

    OverrideNamenodeRegistrationRequest overrideRequest =
        FederationProtocolFactory
            .newInstance(OverrideNamenodeRegistrationRequest.class);
    FederationTestUtils.verifyException(membershipStore,
        "overrideNamenodeRegistration", null,
        new Class[] { OverrideNamenodeRegistrationRequest.class},
        new Object[] { overrideRequest });
  }

  private void registerAndLoadRegistrations(
      List<MembershipState> registrationList) throws IOException {
    // Populate
    assertTrue(stateStore.synchronizeRecords(registrationList,
        MembershipState.class));

    // Load into cache
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));
  }

  private MembershipState createRegistration(String ns, String nn, 
      String router, FederationNamenodeServiceState state) throws IOException {
    MembershipState record = MembershipState.newInstance(
        router, ns,
        nn, "testcluster", "testblock-" + ns, "testrpc-"+ ns + nn,
        "testweb-" + ns + nn, state, false);
    return record;
  }

  @Test
  public void testRegistrationMajorityQuorum()
      throws InterruptedException, IOException {

    // Populate the state store with a set of non-matching elements
    // 1) ns0:nn0 - Standby (newest)
    // 2) ns0:nn0 - Active (oldest)
    // 3) ns0:nn0 - Active (2nd oldest)
    // 4) ns0:nn0 - Active (3nd oldest element, newest active element)
    // Verify the selected entry is the newest majority opinion (4)
    String ns = "ns0";
    String nn = "nn0";

    // Active - oldest
    MembershipState report = createRegistration(ns, nn,
        FederationTestUtils.ROUTER2, FederationNamenodeServiceState.ACTIVE);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));
    Thread.sleep(1000);

    // Active - 2nd oldest
    report = createRegistration(ns, nn, FederationTestUtils.ROUTER3,
        FederationNamenodeServiceState.ACTIVE);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));
    Thread.sleep(1000);

    // Active - 3rd oldest, newest active element
    report = createRegistration(ns, nn, FederationTestUtils.ROUTER4,
        FederationNamenodeServiceState.ACTIVE);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));

    // standby - newest overall
    report = createRegistration(ns, nn, FederationTestUtils.ROUTER1,
        FederationNamenodeServiceState.STANDBY);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));

    // Load and calculate quorum
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));

    // Verify quorum entry
    MembershipState quorumEntry =
        FederationMembershipStateStoreUtils.getSingleMembership(membershipStore,
            report.getNameserviceId(), report.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(quorumEntry.getRouterId(), FederationTestUtils.ROUTER4);
  }

  @Test
  public void testRegistrationQuorumExcludesExpired()
      throws InterruptedException, IOException {

    // Populate the state store with some expired entries and verify the expired
    // entries are ignored.
    // 1) ns0:nn0 - Active
    // 2) ns0:nn0 - Expired
    // 3) ns0:nn0 - Expired
    // 4) ns0:nn0 - Expired
    // Verify the selected entry is the active entry
    List<MembershipState> registrationList =
        new ArrayList<MembershipState>();
    String ns = "ns0";
    String nn = "nn0";
    String rpcAddress = "testrpcaddress";
    String blockPoolId = "testblockpool";
    String clusterId = "testcluster";
    String webAddress = "testwebaddress";
    boolean safemode = false;

    // Active
    MembershipState record = MembershipState.newInstance(
        FederationTestUtils.ROUTER1, ns, nn,
        clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.ACTIVE,
        safemode);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(FederationTestUtils.ROUTER2,
        ns, nn,
        clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.EXPIRED,
        safemode);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(FederationTestUtils.ROUTER3,
        ns, nn, clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.EXPIRED,
        safemode);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(FederationTestUtils.ROUTER4,
        ns, nn, clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.EXPIRED,
        safemode);
    registrationList.add(record);
    registerAndLoadRegistrations(registrationList);

    // Verify quorum entry chooses active membership
    MembershipState quorumEntry =
        FederationMembershipStateStoreUtils.getSingleMembership(membershipStore,
            record.getNameserviceId(), record.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(FederationTestUtils.ROUTER1, quorumEntry.getRouterId());
  }

  @Test
  public void testRegistrationQuorumAllExpired()
 throws IOException {

    // 1) ns0:nn0 - Expired (oldest)
    // 2) ns0:nn0 - Expired
    // 3) ns0:nn0 - Expired
    // 4) ns0:nn0 - Expired
    // Verify no entry is either selected or cached
    List<MembershipState> registrationList =
        new ArrayList<MembershipState>();
    String ns = FederationTestUtils.NAMESERVICE1;
    String nn = FederationTestUtils.NAMENODE1;
    String rpcAddress = "testrpcaddress";
    String blockPoolId = "testblockpool";
    String clusterId = "testcluster";
    String webAddress = "testwebaddress";
    boolean safemode = false;
    long startingTime = Time.now();

    // Expired
    MembershipState record = MembershipState.newInstance(
        FederationTestUtils.ROUTER1, ns, nn,
        clusterId, blockPoolId, rpcAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    record.setDateModified(startingTime - 10000);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(FederationTestUtils.ROUTER2,
        ns, nn, clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.EXPIRED,
        safemode);
    record.setDateModified(startingTime);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(FederationTestUtils.ROUTER3,
        ns, nn, clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.EXPIRED,
        safemode);
    record.setDateModified(startingTime);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(FederationTestUtils.ROUTER4,
        ns, nn, clusterId, blockPoolId,
        rpcAddress, webAddress, FederationNamenodeServiceState.EXPIRED,
        safemode);
    record.setDateModified(startingTime);
    registrationList.add(record);

    registerAndLoadRegistrations(registrationList);

    // Verify no entry is found for this nameservice
    assertNull(FederationMembershipStateStoreUtils.getSingleMembership(
        membershipStore,
        record.getNameserviceId(), record.getNamenodeId()));
  }

  @Test
  public void testRegistrationNoQuorum()
      throws InterruptedException, IOException {

    // Populate the state store with a set of non-matching elements
    // 1) ns0:nn0 - Standby (newest)
    // 2) ns0:nn0 - Standby (oldest)
    // 3) ns0:nn0 - Active (2nd oldest)
    // 4) ns0:nn0 - Active (3nd oldest element, newest active element)
    // Verify the selected entry is the newest entry (1)
    MembershipState report1 = createRegistration(
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1,
        FederationTestUtils.ROUTER2, FederationNamenodeServiceState.STANDBY);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report1));
    Thread.sleep(100);
    MembershipState report2 = createRegistration(
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1,
        FederationTestUtils.ROUTER3, FederationNamenodeServiceState.ACTIVE);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report2));
    Thread.sleep(100);
    MembershipState report3 = createRegistration(
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1,
        FederationTestUtils.ROUTER4, FederationNamenodeServiceState.ACTIVE);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report3));
    Thread.sleep(100);
    MembershipState report4 = createRegistration(
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1,
        FederationTestUtils.ROUTER1, FederationNamenodeServiceState.STANDBY);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report4));

    // Load and calculate quorum
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));

    // Verify quorum entry uses the newest data, even though it is standby
    MembershipState quorumEntry =
        FederationMembershipStateStoreUtils.getSingleMembership(membershipStore,
            report1.getNameserviceId(), report1.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(FederationTestUtils.ROUTER1, quorumEntry.getRouterId());
    assertEquals(FederationNamenodeServiceState.STANDBY,
        quorumEntry.getState());
  }

  @Test
  public void testRegistrationExpired()
      throws InterruptedException, IOException {

    // Populate the state store with a single NN element
    // 1) ns0:nn0 - Active
    // Wait for the entry to expire without heartbeating
    // Verify the NN entry is populated as EXPIRED internally in the state store

    MembershipState report = createRegistration(
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1,
        FederationTestUtils.ROUTER1, FederationNamenodeServiceState.ACTIVE);
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));

    // Load cache
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));

    // Verify quorum and entry
    MembershipState quorumEntry =
        FederationMembershipStateStoreUtils.getSingleMembership(membershipStore,
            report.getNameserviceId(), report.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(FederationTestUtils.ROUTER1, quorumEntry.getRouterId());
    assertEquals(FederationNamenodeServiceState.ACTIVE, quorumEntry.getState());

    // Wait past expiration (set in conf to 5 seconds)
    Thread.sleep(6000);
    // Reload cache
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));

    // Verify entry is now expired and is no longer in the cache
    quorumEntry =
        FederationMembershipStateStoreUtils.getSingleMembership(membershipStore,
        FederationTestUtils.NAMESERVICE1, FederationTestUtils.NAMENODE1);
    assertNull(quorumEntry);

    // Verify entry is now expired and can't be used by rpc service
    quorumEntry = FederationMembershipStateStoreUtils.getSingleMembership(
        membershipStore,
        report.getNameserviceId(), report.getNamenodeId());
    assertNull(quorumEntry);

    // Heartbeat again, updates dateModified
    assertTrue(FederationMembershipStateStoreUtils
        .sendNamenodeHeartbeat(membershipStore, report));
    // Reload cache
    assertTrue(stateStore.loadCache(FederationMembershipStateStore.class));

    // Verify updated entry is marked as active again and is accessible to rpc
    // server
    quorumEntry = FederationMembershipStateStoreUtils.getSingleMembership(
        membershipStore,
        report.getNameserviceId(), report.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(FederationTestUtils.ROUTER1, quorumEntry.getRouterId());
    assertEquals(FederationNamenodeServiceState.ACTIVE, quorumEntry.getState());
  }
}
