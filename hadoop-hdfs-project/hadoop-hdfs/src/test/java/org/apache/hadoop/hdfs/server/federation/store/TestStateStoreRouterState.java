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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationRouterStateStore} functionality.
 */
public class TestStateStoreRouterState extends TestStateStoreBase {

  private static FederationRouterStateStore routerStore;

  @BeforeClass
  public static void create() {
    // Reduce expirations to 5 seconds
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_ROUTER_EXPIRATION_SEC, 5);
  }

  @Before
  public void setup() throws IOException, InterruptedException {

    if(routerStore == null) {
      routerStore =
          stateStore.getRegisteredInterface(FederationRouterStateStore.class);
    }

    // Clear router status registrations
    assertTrue(FederationStateStoreTestUtils.clearRecords(stateStore,
        RouterState.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    stateStore.closeDriver();
    assertEquals(false, stateStore.isDriverReady());

    // Test all APIs that access the data store to ensure they throw the correct
    // exception.
    GetRouterRegistrationRequest getSingleRequest = FederationProtocolFactory
        .newInstance(GetRouterRegistrationRequest.class);
    FederationTestUtils.verifyException(routerStore, "getRouterRegistration",
        StateStoreUnavailableException.class,
        new Class[] { GetRouterRegistrationRequest.class },
        new Object[] { getSingleRequest });

    GetRouterRegistrationsRequest getRequest = FederationProtocolFactory
        .newInstance(GetRouterRegistrationsRequest.class);
    FederationTestUtils.verifyException(routerStore, "getRouterRegistrations",
        StateStoreUnavailableException.class,
        new Class[] { GetRouterRegistrationsRequest.class },
        new Object[] { getRequest });

    RouterHeartbeatRequest hbRequest = FederationProtocolFactory
.newInstance(RouterHeartbeatRequest.class);
    hbRequest.setDateStarted(0);
    hbRequest.setRouterId("test");
    hbRequest.setStatus(RouterServiceState.NONE);
    FederationTestUtils.verifyException(routerStore, "routerHeartbeat",
        StateStoreUnavailableException.class,
        new Class[] { RouterHeartbeatRequest.class },
        new Object[] { hbRequest });
  }

  //
  // Router
  //
  @Test
  public void testUpdateRouterStatus()
      throws IllegalStateException, IOException {

    long dateStarted = Time.now();
    String address = "testaddress";

    // Set
    assertTrue(FederationRouterStateStoreUtils.sendRouterHeartbeat(routerStore,
        address,
        RouterServiceState.RUNNING, dateStarted));

    // Verify

    RouterState record = FederationRouterStateStoreUtils
        .getRouterRegistration(routerStore, address);
    assertNotNull(record);
    assertEquals(RouterServiceState.RUNNING, record.getStatus());
    assertEquals(address, record.getAddress());
    assertEquals(FederationUtil.getCompileInfo(), record.getCompileInfo());
    // Build version may vary a bit
    assertTrue(record.getBuildVersion().length() > 0);
  }

  @Test
  public void testRouterStateExpired()
      throws IOException, InterruptedException {

    long dateStarted = Time.now();
    String address = "testaddress";

    // Set
    assertTrue(FederationRouterStateStoreUtils.sendRouterHeartbeat(routerStore,
        address,
        RouterServiceState.RUNNING, dateStarted));

    // Verify
    RouterState record = FederationRouterStateStoreUtils
        .getRouterRegistration(routerStore, address);
    assertNotNull(record);

    // Wait past expiration (set to 5 sec in config)
    Thread.sleep(6000);

    // Verify expired
    RouterState r = FederationRouterStateStoreUtils
        .getRouterRegistration(routerStore, address);

    assertEquals(RouterServiceState.EXPIRED, r.getStatus());
  }

  @Test
  public void testGetAllRouterStates()
      throws StateStoreUnavailableException, IOException {

    // Set 2 entries
    assertTrue(FederationRouterStateStoreUtils.sendRouterHeartbeat(routerStore,
        "testaddress1",
        RouterServiceState.RUNNING, Time.now()));
    assertTrue(FederationRouterStateStoreUtils.sendRouterHeartbeat(routerStore,
        "testaddress2",
        RouterServiceState.RUNNING, Time.now()));

    // Verify
    Collection<RouterState> entries =
        FederationRouterStateStoreUtils.getAllActiveRouters(routerStore);
    assertEquals(2, entries.size());
  }
}
