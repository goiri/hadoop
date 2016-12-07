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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Test the basic {@link FederationStateStoreService} {@link FederationMountTableStore} functionality.
 */
public class TestStateStoreBase {

  public static FederationStateStoreService stateStore;
  public static Configuration conf;

  @BeforeClass
  public static void createBase() throws IOException, InterruptedException {
    
    conf = FederationStateStoreTestUtils.generateStateStoreConfiguration();
    // Disable auto-reconnect to data store
    conf.setInt(DFSConfigKeys.FEDERATION_STATESTORE_CONNECTION_TEST_SECS, 3600);

  }

  @AfterClass
  public static void destroyBase() throws Exception {
    if (stateStore != null) {
      stateStore.stop();
      stateStore.close();
      stateStore = null;
    }
  }

  @Before
  public void setupBase() throws IOException, InterruptedException,
      InstantiationException, IllegalAccessException {
    if (stateStore == null) {
      stateStore = FederationStateStoreService.createStateStore(conf);
      assertNotNull(stateStore);
      // Set unique identifier, this is normally the router address
      String identifier = UUID.randomUUID().toString();
      stateStore.setIdentifier(identifier);
    }
    // Wait for state store to connect
    stateStore.loadDriver();
    FederationStateStoreService.waitStateStore(stateStore, 10000);
  }
}
