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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationMountTableStore} functionality.
 */
public class TestStateStoreMountTable extends TestStateStoreBase {

  private static List<String> nameservices;
  private static FederationMountTableStore mountStore;

  @BeforeClass
  public static void create() throws IOException {

    nameservices = new ArrayList<String>();
    nameservices.add(FederationTestUtils.NAMESERVICE1);
    nameservices.add(FederationTestUtils.NAMESERVICE2);

    // Reduce expirations to 5 seconds
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_MOUNTTABLE_EXPIRATION_SEC,
        5);
  }

  @Before
  public void setup() throws IOException, InterruptedException {

    mountStore =
          stateStore.getRegisteredInterface(FederationMountTableStore.class);
    // Clear Mount table registrations
    assertTrue(FederationStateStoreTestUtils.clearRecords(stateStore,
        MountTable.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    stateStore.closeDriver();
    assertEquals(false, stateStore.isDriverReady());

    // Test all APIs that access the data store to ensure they throw the correct
    // exception.
    AddMountTableEntryRequest addRequest = FederationProtocolFactory
.newInstance(AddMountTableEntryRequest.class);
    FederationTestUtils.verifyException(mountStore, "addMountTableEntry",
        StateStoreUnavailableException.class,
        new Class[] { AddMountTableEntryRequest.class },
        new Object[] { addRequest });

    UpdateMountTableEntryRequest updateRequest = FederationProtocolFactory
        .newInstance(UpdateMountTableEntryRequest.class);
    FederationTestUtils.verifyException(mountStore, "updateMountTableEntry",
        StateStoreUnavailableException.class,
        new Class[] { UpdateMountTableEntryRequest.class },
        new Object[] { updateRequest });

    RemoveMountTableEntryRequest removeRequest = FederationProtocolFactory
        .newInstance(RemoveMountTableEntryRequest.class);
    FederationTestUtils.verifyException(mountStore, "removeMountTableEntry",
        StateStoreUnavailableException.class,
        new Class[] { RemoveMountTableEntryRequest.class },
        new Object[] { removeRequest });

    GetMountTableEntriesRequest getRequest = FederationProtocolFactory
        .newInstance(GetMountTableEntriesRequest.class);
    FederationTestUtils.verifyException(mountStore, "getMountTableEntries",
        StateStoreUnavailableException.class,
        new Class[] { GetMountTableEntriesRequest.class },
        new Object[] { getRequest });
  }

  //
  // Mount Table
  //
  @Test
  public void testSynchronizeMountTable() throws IOException {
    // Synchronize and get mount table entries
    List<MountTable> entries =
        FederationStateStoreTestUtils.createMockMountTable(nameservices);
    stateStore.synchronizeRecords(entries, MountTable.class);
    for(MountTable e : entries) {
      MountTable entry = FederationMountTableStoreUtils
          .getMountTableEntry(mountStore, e.getSourcePath());
      assertNotNull(entry);
      assertEquals(e.getDefaultLocation().getDest(),
          entry.getDefaultLocation().getDest());
    }
  }

  @Test
  public void testAddMountTableEntry() throws IOException {

    // Add 1
    List<MountTable> entries =
        FederationStateStoreTestUtils.createMockMountTable(nameservices);
    assertEquals(0, FederationMountTableStoreUtils
        .getMountTableRecords(mountStore, "/").getRecords().size());
    assertTrue(FederationMountTableStoreUtils.addMountTableEntry(mountStore,
        entries.get(0)));
    assertEquals(1, FederationMountTableStoreUtils
        .getMountTableRecords(mountStore, "/").getRecords().size());
  }

  @Test
  public void testRemoveMountTableEntry() throws IOException {

    // Add many
    List<MountTable> entries =
        FederationStateStoreTestUtils.createMockMountTable(nameservices);
    stateStore.synchronizeRecords(entries, MountTable.class);
    assertEquals(entries.size(), FederationMountTableStoreUtils
        .getMountTableRecords(mountStore, "/").getRecords().size());

    // Remove 1
    assertTrue(FederationMountTableStoreUtils.removeMountTableEntry(mountStore,
        entries.get(0).getSourcePath()));

    // Verify remove
    assertEquals(entries.size() - 1, FederationMountTableStoreUtils
        .getMountTableRecords(mountStore, "/").getRecords().size());
  }


  @Test
  public void testUpdateMountTableEntry() throws IOException {

    // Add 1
    List<MountTable> entries =
        FederationStateStoreTestUtils.createMockMountTable(nameservices);
    String source = entries.get(0).getSourcePath();
    String nameservice = entries.get(0).getDefaultLocation().getNameserviceId();
    assertTrue(FederationMountTableStoreUtils.addMountTableEntry(mountStore,
        entries.get(0)));
    
    // Verify
    MountTable matchingEntry =
        FederationMountTableStoreUtils.getMountTableEntry(mountStore, source);
    assertNotNull(matchingEntry);
    assertEquals(nameservice,
        matchingEntry.getDefaultLocation().getNameserviceId());

    // Edit destination nameservice for source path
    Map<String, String> destMap = new HashMap<String, String>();
    destMap.put("testnameservice", "/");    
    MountTable replacement =
        MountTable.newInstance(entries.get(0).getSourcePath(), destMap);
    assertTrue(FederationMountTableStoreUtils.updateMountTableEntry(mountStore,
        replacement));

    // Verify
    matchingEntry = FederationMountTableStoreUtils
        .getMountTableEntry(mountStore,
        entries.get(0).getSourcePath());
    assertNotNull(matchingEntry);
    assertEquals("testnameservice",
        matchingEntry.getDefaultLocation().getNameserviceId());
  }
}
