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


import static org.apache.hadoop.hdfs.DFSConfigKeys.FEDERATION_STATESTORE_CLIENT_CLASS;
import static org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl.FEDERATION_STATESTORE_FILE_DIRECTORY;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;

/**
 * Test utility to mimic a federated HDFS cluster with a router.
 */
public class FederationStateStoreTestUtils {

  public static RouterState createMockRouterRegistration(String routerId,
      RouterServiceState status) throws IOException {
    RouterState router =
        RouterState.newInstance(routerId, Time.now(), status);
    return router;
  }

  public static MembershipState createMockRegistrationForNamenode(
      String nameserviceId, String namenodeId,FederationNamenodeServiceState state) throws IOException {
    MembershipState entry = MembershipState.newInstance(
        "routerId",
        nameserviceId, namenodeId, "clusterId", "test", "0.0.0.0:0",
        "0.0.0.0:0", state, false);
    entry.setNumberOfActiveDatanodes(1);
    entry.setNumberOfDeadDatanodes(0);
    entry.setNumOfBlocks(10);
    entry.setNumberOfDecomDatanodes(0);
    return entry;
  }

  public static void setSqlConfiguration(Configuration conf) {

    conf.set(
        "dfs.federation.statestore.client.data.record.statestoresql.membershipstaterecord",
        "membership");
    conf.set(
        "dfs.federation.statestore.client.data.record.statestoresql.routerrecord",
        "router");
    conf.set(
        "dfs.federation.statestore.client.data.record.statestoresql.rebalancerrecord",
        "rebalancer");
    conf.set(
        "dfs.federation.statestore.client.data.record.statestoresql.mounttablerecord",
        "mounttable");
    conf.set(
        "dfs.federation.statestore.client.data.record.statestoresql.pathlockrecord",
        "pathlock");
  }

  public static void deleteStateStore() throws IOException {
    deleteStateStore(StateStoreFileImpl.class);
  }

  public static Class<? extends StateStoreDriver> defaultStateStoreDriver() {
    return StateStoreFileImpl.class;
  }

  public static void deleteStateStore(
      Class<? extends StateStoreDriver> driverClass) throws IOException {

    if (driverClass.getSimpleName().equals("StateStoreSql")) {
      // TODO
    } else if (driverClass.getSimpleName().equals("StateStoreFile")) {
      String workingDirectory = System.getProperty("user.dir");
      File dir = new File(workingDirectory + "/statestore");
      if (dir.exists()) {
        FileUtils.cleanDirectory(dir);
      }
    } else if (driverClass.getSimpleName().equals("StateStoreZooKeeper")) {
      // TODO
    }
  }

  public static void setFileConfiguration(Configuration conf) {

    String workingDirectory = System.getProperty("user.dir");
    conf.set(FEDERATION_STATESTORE_FILE_DIRECTORY,
        workingDirectory + "/statestore");

  }

  public static Configuration generateStateStoreConfiguration() {
    return generateStateStoreConfiguration(defaultStateStoreDriver());
  }

  public static Configuration generateStateStoreConfiguration(
      Class<? extends StateStoreDriver> driverClass) {
    Configuration conf = new HdfsConfiguration();

    conf.set("dfs.permissions.enabled", "true");
    conf.set("fs.defaultFS", "hdfs://test");

    conf.set(FEDERATION_STATESTORE_CLIENT_CLASS,
        driverClass.getCanonicalName());

    if (driverClass.getSimpleName().equals("StateStoreSql")) {
      setSqlConfiguration(conf);
    } else if (driverClass.getSimpleName().equals("StateStoreFile")) {
      setFileConfiguration(conf);
    }
    return conf;
  }

  public static FederationStateStoreService generateStateStore()
 throws IOException,
      InterruptedException, InstantiationException, IllegalAccessException {
    return generateStateStore(generateStateStoreConfiguration());
  }

  public static FederationStateStoreService generateStateStore(Configuration configuration)
 throws IOException, InterruptedException,
          InstantiationException, IllegalAccessException {
    FederationStateStoreService stateStore = FederationStateStoreService.createStateStore(configuration);
    assertNotNull(stateStore);
    // Wait for state store to connect
    FederationStateStoreService.waitStateStore(stateStore, 10000);
    return stateStore;
  }
  
  public static boolean clearAllRecords(FederationStateStoreService store) throws IOException {
    List<Class<? extends BaseRecord>> allRecords =
        store.getSupportedRecords();
    for (Class<? extends BaseRecord> recordType : allRecords) {
      if (!clearRecords(store, recordType)) {
        return false;
      }
    }
    return true;
  }

  public static <T extends BaseRecord> boolean clearRecords(
	      FederationStateStoreService stateStore, Class<T> recordType) throws IOException {
    List<T> emptyList = new ArrayList<T>();
    if(!stateStore.synchronizeRecords(emptyList, recordType)) {
      return false;
    }
    stateStore.refreshCaches();
    return true;
  }
  
  public static List<MountTable> createMockMountTable(
      List<String> nameservices) throws IOException {
    // create table entries
    List<MountTable> entries = new ArrayList<MountTable>();
    for (String ns : nameservices) {
      Map<String, String> destMap = new HashMap<String, String>();
      destMap.put(ns, "/target-" + ns);
      MountTable entry = MountTable.newInstance("/" + ns, destMap);
      entries.add(entry);
    }
    return entries;
  }
}
