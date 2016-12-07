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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.records.PathLock;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationPathLockStore} functionality.
 */
public class TestStateStorePathLock extends TestStateStoreBase {

  private static FederationPathLockStore pathManager;
  private static FederationRouterStateStore routerManager;

  @BeforeClass
  public static void create() {
    // Reduce expirations to 5 seconds
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_PATHLOCK_EXPIRATION_SEC,
        5);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    // Clear data
    FederationStateStoreTestUtils.clearAllRecords(stateStore);
    pathManager =
        stateStore.getRegisteredInterface(FederationPathLockStore.class);
    routerManager =
        stateStore.getRegisteredInterface(FederationRouterStateStore.class);

    // Clear all path locks
    assertTrue(FederationStateStoreTestUtils.clearRecords(stateStore,
        PathLock.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    stateStore.closeDriver();
    assertEquals(false, stateStore.isDriverReady());

    // Test all APIs that access the data store to ensure they throw the correct
    // exception.
    FederationTestUtils.verifyException(pathManager, "unlockPath",
        StateStoreUnavailableException.class,
        new Class[] { String.class, String.class },
        new Object[] { "test", "test" });

    FederationTestUtils.verifyException(pathManager, "lockPath",
        StateStoreUnavailableException.class,
        new Class[] { String.class, String.class },
        new Object[] { "test", "test" });

    /*
    FederationTestUtils.verifyException(pathManager, "isLocked",
        StateStoreUnavailableException.class, new Class[] { String.class },
        new Object[] { "test" });
    */
  }

  @Test
  public void testLockUnlockPath() throws IOException {

    String mountPoint = "/testmount";
    String client = "testclient";

    // Default state is unlocked
    assertFalse(pathManager.isLocked(mountPoint));

    // Lock and verify
    assertNotNull(pathManager.lockPath(mountPoint, client));
    assertTrue(stateStore.loadCache(FederationPathLockStore.class));
    assertEquals(true, pathManager.isLocked(mountPoint));

    // Unlock and verify
    assertEquals(true, pathManager.unlockPath(mountPoint, client));
    assertTrue(stateStore.loadCache(FederationPathLockStore.class));
    assertEquals(false, pathManager.isLocked(mountPoint));
  }

  @Test
  public void testAcknowledgePathLock() throws IOException, TimeoutException,
      InterruptedException, ExecutionException {
    String mountPoint = "/testmount";
    String client = "testclient";

    // Default state is unlocked
    assertFalse(pathManager.isLocked(mountPoint));

    // Add a fake router
    String routerId = "router";
    FederationRouterStateStoreUtils.sendRouterHeartbeat(routerManager, routerId,
        RouterServiceState.RUNNING, Time.now());

    // Lock and verify
    final PathLock record = pathManager.lockPath(mountPoint, client);
    assertNotNull(record);
    assertTrue(stateStore.loadCache(FederationPathLockStore.class));
    assertEquals(true, pathManager.isLocked(mountPoint));

    List<String> barrierList = new ArrayList<String>();
    barrierList.add(routerId);

    // Wait for ack, timeout (no router acks)
    boolean exceptionCaught = false;
    try {
      stateStore.getDriver().waitForAcknowledgement(record, barrierList, 2000);
    } catch (TimeoutException ex) {
      exceptionCaught = true;
    }
    assertEquals(true, exceptionCaught);

    // Fake a router ack
    assertEquals(true,
        stateStore.getDriver().acknowledgeRecord(record, routerId));

    // Verify ack
    stateStore.getDriver().waitForAcknowledgement(record, barrierList,
        2000);
  }

  @Test
  public void testPathLockExpired()
      throws IOException, InterruptedException {

    // Create and lock
    String mount = "/testmount";
    String client = "testclient";

    // Lock
    assertNotNull(pathManager.lockPath(mount, client));
    assertTrue(stateStore.loadCache(FederationPathLockStore.class));
    assertEquals(true, pathManager.isLocked(mount));

    // Wait past expiration (set to 5 sec in config)
    Thread.sleep(6000);

    // Verify expired
    assertTrue(stateStore.loadCache(FederationPathLockStore.class));
    assertFalse(pathManager.isLocked(mount));
  }
}
