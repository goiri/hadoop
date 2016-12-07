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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileSystemImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the FileSystem (e.g., HDFS) implementation of the State Store driver.
 */
public class TestStateStoreFileSystem extends TestStateStoreDriverBase {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = FederationStateStoreTestUtils
        .generateStateStoreConfiguration(StateStoreFileSystemImpl.class);
    conf.set(StateStoreFileSystemImpl.FEDERATION_STATESTORE_FS_PATH,
        "/hdfs-federation/");

    // Create HDFS cluster to back the state tore
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.numDataNodes(1);
    dfsCluster = builder.build();
    dfsCluster.waitClusterUp();
    generateStateStore(conf);
  }

  @AfterClass
  public static void tearDownCluster() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
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
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(stateStore.getDriver());
  }

  @Test
  public void testDelete()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(stateStore.getDriver());
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
