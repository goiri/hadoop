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
import java.util.List;

import javax.management.MalformedObjectNameException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.RouterAdminProtocol;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMBean;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The the administrator interface of the {@link Router} implemented by
 * {@link RouterAdminServer}.
 */
public class TestRouterAdmin {

  private static StateStoreDFSCluster cluster;
  private static RouterContext router;
  public static final String RPCBean =
      "Hadoop:service=Router,name=FederationRPC";
  private static List<MountTable> mountTableFixture;
  private static FederationStateStoreService stateStore;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    // Build and start a router with statestore + admin + rpc
    cluster.addRouterOverrides(
        new RouterConfigBuilder().stateStore().admin().rpc().build());
    cluster.startRouters();
    router = cluster.getRandomRouter();
    mountTableFixture = cluster.generateMockMountTable();
    stateStore = router.getRouter().getStateStore();
  }

  @Before
  public void testSetup() throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    assertTrue(stateStore.synchronizeRecords(mountTableFixture,
        MountTable.class));
  }

  @After
  public void testCleanup() {
  }

  @AfterClass
  public static void tearDown() {
    cluster.stopRouter(router);
  }

  @Test
  public void testAddMountTable() throws IOException {
    MountTable newEntry = MountTable.newInstance("/testpath",
        "ns0:::/testdir", Time.now(), Time.now());

    RouterClient client = router.getAdminClient();

    // Existing mount table size
    List<MountTable> records = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(records.size(), mountTableFixture.size());

    // Add
    assertEquals(true, FederationMountTableStoreUtils
        .addMountTableEntry(client.getMountTableProtocol(), newEntry));

    // New mount table size
    records = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(records.size(), mountTableFixture.size() + 1);
  }

  @Test
  public void testAddDuplicateMountTable() throws IOException {
    MountTable newEntry = MountTable.newInstance("/testpath",
        "ns0:::/testdir", Time.now(), Time.now());

    RouterClient client = router.getAdminClient();

    // Existing mount table size
    List<MountTable> entries = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(entries.size(), mountTableFixture.size());

    // Add
    FederationMountTableStoreUtils
        .addMountTableEntry(client.getMountTableProtocol(),
        newEntry);

    // New mount table size
    entries = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(entries.size(), mountTableFixture.size() + 1);

    // Add again, should fail
    assertEquals(false, FederationMountTableStoreUtils
        .addMountTableEntry(client.getMountTableProtocol(), newEntry));
  }

  @Test
  public void testRemoveMountTable() throws IOException {

    RouterClient client = router.getAdminClient();

    // Existing mount table size
    List<MountTable> entries = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(entries.size(), mountTableFixture.size());

    // Remove an entry
    FederationMountTableStoreUtils
        .removeMountTableEntry(client.getMountTableProtocol(), "/");

    // New mount table size
    entries = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(entries.size(), mountTableFixture.size() - 1);
  }

  @Test
  public void testEditMountTable() throws IOException {
    RouterClient client = router.getAdminClient();

    // Verify starting condition
    MountTable entry = FederationMountTableStoreUtils
        .getMountTableEntry(client.getMountTableProtocol(), "/");
    assertEquals(entry.getDestPaths(), "ns0:::/");

    // Edit the entry for /
    MountTable updatedEntry =
        MountTable.newInstance("/", "ns1:::/", Time.now(), Time.now());
    FederationMountTableStoreUtils
        .updateMountTableEntry(client.getMountTableProtocol(), updatedEntry);

    // Verify edited condition
    entry = FederationMountTableStoreUtils
        .getMountTableEntry(client.getMountTableProtocol(), "/");
    assertEquals(entry.getDestPaths(), "ns1:::/");
  }

  @Test
  public void testGetMountTable() throws IOException {

    RouterClient client = router.getAdminClient();

    // Verify size of table
    List<MountTable> entries = FederationMountTableStoreUtils
        .getMountTableRecords(client.getMountTableProtocol(), "/").getRecords();
    assertEquals(mountTableFixture.size(), entries.size());

    // Verify all entries are present
    int matches = 0;
    for (MountTable e : entries) {
      for (MountTable entry : mountTableFixture) {
        assertEquals(e.getDestinations().size(), 1);
        assertNotNull(e.getDateCreated());
        assertNotNull(e.getDateModified());
        if (entry.getSourcePath().equals(e.getSourcePath())) {
          matches++;
        }
      }
    }
    assertEquals(matches, mountTableFixture.size());
  }

  @Test
  public void testGetSingleMountTableEntry() throws IOException {
    RouterClient client = router.getAdminClient();
    MountTable entry = FederationMountTableStoreUtils
        .getMountTableEntry(client.getMountTableProtocol(), "/ns0");
    assertNotNull(entry);
    assertEquals(entry.getSourcePath(), "/ns0");
  }

  @Test
  public void testResetPerformanceCounters()
      throws MalformedObjectNameException, IOException {

    // Add a dummy entry to the resolver which will timeout and generate
    // an error when queried.
    NamenodeStatusReport report = FederationTestUtils.createNamenodeReport(
        cluster.getNameservices().get(0), RouterDFSCluster.NAMENODE1,
        HAServiceState.ACTIVE);
    router.getRouter().getNamenodeResolver().registerNamenode(report);

    try {
      router.getFileSystem().listFiles(new Path("/"), true);
    } catch (Exception ex) {
    }
    FederationRPCMBean bean =
        FederationTestUtils.getBean(RPCBean, FederationRPCMBean.class);
    long failure = bean.getProxyOpFailureCommunicate();
    assertTrue(failure > 0);

    RouterClient routerClient = router.getAdminClient();
    RouterAdminProtocol routerAdmin = routerClient.getAdminProtocol();
    routerAdmin.resetPerfCounters(RouterAdmin.ROUTER_PERF_FLAG_RPC);

    bean = FederationTestUtils.getBean(RPCBean, FederationRPCMBean.class);
    failure = bean.getProxyOpFailureCommunicate();
    assertEquals(failure, 0);
  }
}
