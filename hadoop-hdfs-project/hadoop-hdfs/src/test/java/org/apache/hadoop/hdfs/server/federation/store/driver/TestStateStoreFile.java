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
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the FileSystem (e.g., HDFS) implementation of the State Store driver.
 */
public class TestStateStoreFile extends TestStateStoreDriverBase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = FederationStateStoreTestUtils
        .generateStateStoreConfiguration(StateStoreFileImpl.class);
    generateStateStore(conf);
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
  public void testMetrics()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testMetrics(stateStore.getDriver());
  }
}
