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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.management.MalformedObjectNameException;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.FederationRebalancerStore;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the JMX interface for the {@link Router}.
 */
public class TestFederationMetrics extends TestMetricsBase {

  private static final Log LOG = LogFactory.getLog(TestFederationMetrics.class);

  public static final String FederationBean =
      "Hadoop:service=Router,name=FederationState";
  public static final String StateSoreBean =
      "Hadoop:service=Router,name=StateStore";
  public static final String RPCBean =
      "Hadoop:service=Router,name=FederationRPC";


  @BeforeClass
  public static void globalSetUp() throws Exception {

    // Build and start a router with statestore + metrics + http
    routerConfig =
        new RouterConfigBuilder().stateStore().metrics().http().build();
  }

  private void validateClusterStatsBean(FederationMBean bean)
      throws IOException {

    // Determine aggregates
    long numBlocks = 0;
    long numDead = 0;
    long numDecom = 0;
    long numLive = 0;
    long numFiles = 0;
    for (MembershipState mock : activeMemberships) {
      numBlocks += mock.getNumOfBlocks();
      numDead += mock.getNumberOfDeadDatanodes();
      numDecom += mock.getNumberOfDecomDatanodes();
      numLive += mock.getNumberOfActiveDatanodes();
    }

    assertEquals(numBlocks, bean.getNumBlocks());
    assertEquals(numDead, bean.getNumDeadNodes());
    assertEquals(numDecom, bean.getNumDecommissioningNodes());
    assertEquals(numLive, bean.getNumLiveNodes());
    assertEquals(numFiles, bean.getNumFiles());
    assertEquals(activeMemberships.size() + standbyMemberships.size(),
        bean.getNumNamenodes());
    assertEquals(nameservices.size(), bean.getNumNameservices());
    assertTrue(bean.getVersion().length() > 0);
    assertTrue(bean.getCompiledDate().length() > 0);
    assertTrue(bean.getCompileInfo().length() > 0);
    assertTrue(bean.getRouterStarted().length() > 0);
    assertTrue(bean.getHostAndPort().length() > 0);
  }

  @Test
  public void testClusterStatsJMX()
      throws MalformedObjectNameException, IOException {

    FederationMBean bean =
        FederationTestUtils.getBean(FederationBean, FederationMBean.class);
    validateClusterStatsBean(bean);
  }

  @Test
  public void testClusterStatsDataSource() throws IOException {
    try {
      validateClusterStatsBean(router.getMetrics());
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
  }

  @Test
  public void testMountTableStatsDataSource()
      throws IOException, JSONException {

    String jsonString = router.getMetrics().getMountTable();
    JSONArray jsonArray = new JSONArray(jsonString);
    assertEquals(jsonArray.length(), mockMountTable.size());

    int match = 0;
    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject json = jsonArray.getJSONObject(i);
      String src = json.getString("sourcePath");

      for (MountTable entry : mockMountTable) {
        if (entry.getSourcePath().equals(src)) {
          assertEquals(entry.getDefaultLocation().getNameserviceId(),
              json.getString("nameserviceId"));
          assertEquals(entry.getDefaultLocation().getDest(),
              json.getString("path"));
          assertNotNullAndNotEmpty(json.getString("dateCreated"));
          assertNotNullAndNotEmpty(json.getString("dateModified"));
          match++;
        }
      }
    }
    assertEquals(match, mockMountTable.size());
  }

  private MembershipState findMockNamenode(String nsId, String nnId) {

    @SuppressWarnings("unchecked")
    List<MembershipState> namenodes =
        ListUtils.union(activeMemberships, standbyMemberships);
    for (MembershipState nn : namenodes) {
      if (nn.getNamenodeId().equals(nnId)
          && nn.getNameserviceId().equals(nsId)) {
        return nn;
      }
    }
    return null;
  }

  @Test
  public void testNamenodeStatsDataSource() throws IOException, JSONException {

    String jsonString = router.getMetrics().getNamenodes();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    int nnsFound = 0;
    while (keys.hasNext()) {
      // Validate each entry against our mocks
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      String nameserviceId = json.getString("nameserviceId");
      String namenodeId = json.getString("namenodeId");

      MembershipState mockEntry =
          this.findMockNamenode(nameserviceId, namenodeId);
      assertNotNull(mockEntry);

      assertEquals(json.getString("state"), mockEntry.getState().toString());
      assertEquals(json.getLong("numberOfActiveDatanodes"),
          mockEntry.getNumberOfActiveDatanodes());
      assertEquals(json.getLong("numberOfDecomDatanodes"),
          mockEntry.getNumberOfDecomDatanodes());
      assertEquals(json.getLong("numberOfDeadDatanodes"),
          mockEntry.getNumberOfDeadDatanodes());
      assertEquals(json.getLong("numOfBlocks"), mockEntry.getNumOfBlocks());
      assertEquals(json.getString("rpcAddress"), mockEntry.getRpcAddress());
      assertEquals(json.getString("webAddress"), mockEntry.getWebAddress());
      nnsFound++;
    }
    // Validate all memberships are present
    assertEquals(activeMemberships.size() + standbyMemberships.size(),
        nnsFound);
  }

  @Test
  public void testNameserviceStatsDataSource()
      throws IOException, JSONException {

    String jsonString = router.getMetrics().getNameservices();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    int nameservicesFound = 0;
    while (keys.hasNext()) {
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      String nameserviceId = json.getString("nameserviceId");
      String namenodeId = json.getString("namenodeId");

      MembershipState mockEntry =
          this.findMockNamenode(nameserviceId, namenodeId);
      assertNotNull(mockEntry);

      // NS should report the active NN
      assertEquals(mockEntry.getState().toString(), json.getString("state"));
      assertEquals("ACTIVE", json.getString("state"));

      // Stats in the NS should reflect the stats for the most active NN
      assertEquals(mockEntry.getNumOfFiles(), json.getLong("numOfFiles"));
      assertEquals(mockEntry.getTotalSpace(), json.getLong("totalSpace"));
      assertEquals(mockEntry.getNumOfBlocksMissing(),
          json.getLong("numOfBlocksMissing"));
      assertEquals(mockEntry.getNumberOfActiveDatanodes(),
          json.getLong("numberOfActiveDatanodes"));
      assertEquals(mockEntry.getAvailableSpace(),
          json.getLong("availableSpace"));
      assertEquals(mockEntry.getNumberOfDecomDatanodes(),
          json.getLong("numberOfDecomDatanodes"));
      assertEquals(mockEntry.getNumberOfDeadDatanodes(),
          json.getLong("numberOfDeadDatanodes"));
      nameservicesFound++;
    }
    assertEquals(nameservices.size(), nameservicesFound);
  }

  private void assertNotNullAndNotEmpty(String field) {
    assertNotNull(field);
    assertTrue(field.length() > 0);
  }

  private RouterState findMockRouter(String routerId) {
    for (RouterState router : mockRouters) {
      if (router.getAddress().equals(routerId)) {
        return router;
      }
    }
    return null;
  }

  @Test
  public void testRouterStatsDataSource() throws IOException, JSONException {

    String jsonString = router.getMetrics().getRouters();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    int routersFound = 0;
    while (keys.hasNext()) {
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      assertNotNullAndNotEmpty(json.getString("address"));
      RouterState router = findMockRouter(json.getString("address"));
      assertNotNull(router);

      assertEquals(router.getStatus().toString(), json.getString("status"));
      assertEquals(router.getCompileInfo(), json.getString("compileInfo"));
      assertEquals(router.getDateStarted(), json.getLong("dateStarted"));
      assertEquals(router.getDateCreated(), json.getLong("dateCreated"));
      assertEquals(router.getDateModified(), json.getLong("dateModified"));
      assertTrue(json.getLong("lastPathLockUpdate") > 0);
      assertTrue(json.getLong("lastRegistrationsUpdate") > 0);
      assertEquals(router.getRegistrationTableVersion(),
          json.getLong("registrationTableVersion"));
      assertEquals(router.getPathLockVersion(),
          json.getLong("pathLockVersion"));
      routersFound++;
    }

    assertEquals(mockRouters.size(), routersFound);
  }

  @Test
  public void testRebalancerLogStatsDataSource()
      throws IOException, JSONException {

    String mount = "/testmount";
    String ns = "testns";
    String path = "/testpath";

    // Reserve
    FederationRebalancerStore rebalancerStore = router.getStateStore()
        .getRegisteredInterface(FederationRebalancerStore.class);
    String clientId =
        rebalancerStore.reserveRebalancerMountPoint(mount, ns, path);
    assertNotNull(clientId);

    String jsonString = router.getMetrics().getRebalancerHistory();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    while (keys.hasNext()) {
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      assertEquals(json.getString("nameserviceId"), ns);
      assertEquals(json.getString("mount"), mount);
      assertEquals(json.getString("dstPath"), path);
      assertEquals(json.getString("clientId"), clientId);
      assertNotNullAndNotEmpty(json.getString("state"));
      assertNotNullAndNotEmpty(json.getString("dateCreated"));
      assertNotNullAndNotEmpty(json.getString("dateModified"));
      assertNotNullAndNotEmpty(json.getString("id"));
    }
  }
}

