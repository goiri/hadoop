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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog.RebalancerOperationStatus;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog.RebalancerServiceState;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationRebalancerStore} functionality.
 */
public class TestStateStoreRebalancer extends TestStateStoreBase {

  private static FederationRebalancerStore reblancerStore;

  @Before
  public void setup() throws IOException, InterruptedException {

    reblancerStore =
        stateStore.getRegisteredInterface(FederationRebalancerStore.class);

    // Clear all rebalancer entries
    assertTrue(FederationStateStoreTestUtils.clearRecords(stateStore,
        RebalancerLog.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    stateStore.closeDriver();
    assertEquals(false, stateStore.isDriverReady());

    // Test all APIs that access the data store to ensure they throw the correct
    // exception.
    FederationTestUtils.verifyException(reblancerStore,
        "releaseRebalancerMountPoint", StateStoreUnavailableException.class,
        new Class[] { String.class, String.class,
            RebalancerOperationStatus.class, String.class },
        new Object[] { "test", "test", RebalancerOperationStatus.FAILED_COPY, "test" });

    FederationTestUtils.verifyException(reblancerStore,
        "reserveRebalancerMountPoint", StateStoreUnavailableException.class,
        new Class[] { String.class, String.class, String.class },
        new Object[] { "test", "test", "test" });

    FederationTestUtils.verifyException(reblancerStore,
        "forceReleaseMountPoint",
        StateStoreUnavailableException.class, new Class[] { String.class },
        new Object[] { "test" });

    FederationTestUtils.verifyException(reblancerStore,
        "clearRebalancerEntries",
        StateStoreUnavailableException.class, new Class[] { String.class },
        new Object[] { "test" });

    FederationTestUtils.verifyException(
        reblancerStore, "updateRebalancerJobId",
        StateStoreUnavailableException.class, new Class[] { String.class,
            String.class, String.class, String.class },
        new Object[] { "test", "test", "test", "test" });

    FederationTestUtils.verifyException(reblancerStore, "updateRebalancerState",
        StateStoreUnavailableException.class, new Class[] { String.class,
            String.class, RebalancerServiceState.class },
        new Object[] { "test", "test", RebalancerServiceState.COMPLETE });

    FederationTestUtils.verifyException(reblancerStore, "getRebalancerState",
        StateStoreUnavailableException.class,
        new Class[] { String.class, String.class },
        new Object[] { "test", "test" });

    FederationTestUtils.verifyException(reblancerStore,
        "getRebalancerOperations",
        StateStoreUnavailableException.class, new Class[] { String.class },
        new Object[] { "test" });

    FederationTestUtils.verifyException(reblancerStore,
        "updateRebalancerCheckpoint", StateStoreUnavailableException.class,
        new Class[] { String.class, String.class },
        new Object[] { "test", "test" });
  }

  private void validateRebalancerEntry(String mount, String clientId, String ns,
      String path, RebalancerServiceState state,
      RebalancerOperationStatus status, String message)
          throws IllegalStateException, IOException {

    Collection<RebalancerLog> entries =
        reblancerStore.getRebalancerOperations(mount);
    assertEquals(1, entries.size());

    RebalancerLog entry = entries.iterator().next();
    assertEquals(state, entry.getState());
    assertEquals(clientId, entry.getClientId());
    assertEquals(mount, entry.getMount());
    assertEquals(ns, entry.getNameserviceId());
    assertEquals(path, entry.getDstPath());
    assertEquals(message, entry.getOperationResult());
    assertEquals(status, entry.getOperationStatus());
  }

  @Test
  public void testReserveRebalancer()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");
  }

  @Test
  public void testReserveRebalancerFailure()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Reserve again - fails
    String clientId2 =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNull(clientId2);

    // Existing client still exists
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");
  }

  @Test
  public void testReleaseRebalancer()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";
    RebalancerOperationStatus status = RebalancerOperationStatus.SUCCESS;
    String message = "testmessage";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Release
    assertTrue(reblancerStore.releaseRebalancerMountPoint(mount,
        clientId, status,
        message));
    validateRebalancerEntry(mount, "", ns, path,
        RebalancerServiceState.COMPLETE,
        RebalancerOperationStatus.SUCCESS, message);
  }

  @Test
  public void testReleaseRebalancerFailure()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";
    RebalancerOperationStatus status = RebalancerOperationStatus.SUCCESS;
    String message = "testmessage";

    // Release non-existent entry - fails
    assertFalse(reblancerStore.releaseRebalancerMountPoint(mount,
        "does-not-exist",
        status, message));

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Release - invalid client ID, fails
    assertFalse(reblancerStore.releaseRebalancerMountPoint(mount,
        "does-not-exist", status, message));

    // Release - invalid mount, fails
    assertFalse(reblancerStore.releaseRebalancerMountPoint(
        "does-not-exist", clientId, status, message));
  }

  @Test
  public void testUpdateRebalancerState()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Update state and validate
    assertTrue(reblancerStore.updateRebalancerState(mount, clientId,
        RebalancerServiceState.INIIALIZING));
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.INIIALIZING, RebalancerOperationStatus.NONE, "");
  }

  @Test
  public void testUpdateRebalancerStateFailure()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Update state for non-existent mount - fails
    assertFalse(reblancerStore.updateRebalancerState("does-not-exist",
        clientId, RebalancerServiceState.INIIALIZING));

    // Update state for non-existent client - fails
    assertFalse(reblancerStore.updateRebalancerState(mount,
        "does-not-exist", RebalancerServiceState.INIIALIZING));
  }

  @Test
  public void testClearRebalancerEntry()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Delete
    assertTrue(reblancerStore.clearRebalancerEntries(mount));
    assertTrue(reblancerStore.getRebalancerOperations(mount).isEmpty());
  }

  @Test
  public void testForceReleaseRebalancerEntry()
      throws IllegalStateException, IOException {
    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    String clientId =
        reblancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);
    validateRebalancerEntry(mount, clientId, ns, path,
        RebalancerServiceState.NONE, RebalancerOperationStatus.NONE, "");

    // Force release
    assertTrue(reblancerStore.forceReleaseMountPoint(mount));
    validateRebalancerEntry(mount, "", ns, path, RebalancerServiceState.NONE,
        RebalancerOperationStatus.FORCED_COMPLETE,
        "Mount point has been force released.");
  }
}
