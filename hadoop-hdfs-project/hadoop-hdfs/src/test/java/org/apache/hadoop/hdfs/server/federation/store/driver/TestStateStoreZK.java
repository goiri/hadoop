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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the ZooKeeper implementation of the State Store driver.
 */
public class TestStateStoreZK extends TestStateStoreDriverBase {

  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;

  @BeforeClass
  public static void setupCluster() throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(curatorTestingServer.getConnectString())
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();

    // Create the ZK State Store
    Configuration conf = FederationStateStoreTestUtils
        .generateStateStoreConfiguration(StateStoreZooKeeperImpl.class);
    conf.set(ZKFailoverController.ZK_QUORUM_KEY,
        curatorTestingServer.getConnectString());
    // Disable auto-repair of connection
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_CONNECTION_TEST_SECS, 3600);
    generateStateStore(conf);
  }

  @AfterClass
  public static void tearDownCluster() {
    curatorFramework.close();
    try {
      curatorTestingServer.stop();
    } catch (IOException e) {
    }
  }

  @Before
  public void startup() throws IOException {
    deleteAll(stateStore.getDriver());
  }

  @Test
  public void testInsert()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(stateStore.getDriver());
  }

  @Test
  public void testUpdate()
      throws IllegalArgumentException, IllegalAccessException, IOException,
      NoSuchFieldException, SecurityException {
    testUpdate(stateStore.getDriver());
  }

  @Test
  public void testDelete()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testDelete(stateStore.getDriver());
  }

  @Test
  public void testFetchErrors()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testFetchErrors(stateStore.getDriver());
  }

  @Test
  public void testMetrics()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testMetrics(stateStore.getDriver());
  }
}
