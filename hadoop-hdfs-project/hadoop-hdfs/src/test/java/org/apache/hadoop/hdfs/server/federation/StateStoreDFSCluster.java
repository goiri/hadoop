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
package org.apache.hadoop.hdfs.server.federation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;

/**
 * Test utility to mimic a federated HDFS cluster with a router and a state
 * store.
 */
public class StateStoreDFSCluster extends RouterDFSCluster {

  public StateStoreDFSCluster(boolean ha, int numNameservices, int numNamenodes,
      int heartbeatInterval, int cacheFlushInterval)
          throws IOException, InterruptedException {
    super(ha, numNameservices, numNamenodes, heartbeatInterval,
        cacheFlushInterval);

    // Attach state store and resolvers to router
    Configuration stateStoreConfig =
        FederationStateStoreTestUtils.generateStateStoreConfiguration();
    // Use state store backed resolvers
    stateStoreConfig.set(
        DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MembershipNamenodeResolver.class.getCanonicalName());
    stateStoreConfig.set(
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MountTableResolver.class.getCanonicalName());
    this.addRouterOverrides(stateStoreConfig);
  }

  public StateStoreDFSCluster(boolean ha, int numNameservices)
      throws IOException, InterruptedException {
    this(ha, numNameservices, 2, DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_CACHE_INTERVAL_SEC);
  }

  public StateStoreDFSCluster(boolean ha, int numNameservices,
      int numNamnodes) throws IOException, InterruptedException {
    this(ha, numNameservices, numNamnodes, DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_CACHE_INTERVAL_SEC);
  }

  /////////////////////////////////////////////////////////////////////////////
  // State Store Test Fixtures
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Adds test fixtures for NN registation for each NN nameservice -> NS
   * namenode -> NN rpcAddress -> 0.0.0.0:0 webAddress -> 0.0.0.0:0 state ->
   * STANDBY safeMode -> false blockPool -> test.
   * 
   * @param router
   * @throws IOException
   */
  public void createTestRegistration(FederationStateStoreService stateStore)
      throws IOException {
    List<MembershipState> entries =
        new ArrayList<MembershipState>();
    for (NamenodeContext nn : this.getNamenodes()) {
      MembershipState entry = FederationStateStoreTestUtils
          .createMockRegistrationForNamenode(nn.getNameserviceId(),
              nn.getNamenodeId(), FederationNamenodeServiceState.STANDBY);
      entries.add(entry);
    }
    stateStore.synchronizeRecords(entries,
        MembershipState.class);
  }

  public void createTestMountTable(FederationStateStoreService stateStore)
      throws IOException {
    List<MountTable> mounts = generateMockMountTable();
    stateStore.synchronizeRecords(mounts, MountTable.class);
    stateStore.refreshCaches();
  }

  public List<MountTable> generateMockMountTable() throws IOException {
    // create table entries
    List<MountTable> entries = new ArrayList<MountTable>();
    for (String ns : this.getNameservices()) {
      Map<String, String> destMap = new HashMap<String, String>();
      destMap.put(ns, getNamenodePathForNameservice(ns));

      // Direct path
      MountTable entry = MountTable
          .newInstance(getFederatedPathForNameservice(ns), destMap);
      entries.add(entry);
    }

    // Root path goes to nameservice 1
    Map<String, String> destMap = new HashMap<String, String>();
    destMap.put(this.getNameservices().get(0), "/");
    MountTable entry = MountTable.newInstance("/", destMap);
    entries.add(entry);
    return entries;
  }
}
