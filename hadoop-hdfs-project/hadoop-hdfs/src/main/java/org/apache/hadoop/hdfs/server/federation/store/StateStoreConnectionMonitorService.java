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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.PeriodicService;

/**
 * Service to periodically monitor the connection of the StateStore
 * {@link FederationStateStoreService} data store and to re-open the connection to the data store
 * if required.
 */
public class StateStoreConnectionMonitorService extends PeriodicService {
  private static final Log LOG =
      LogFactory.getLog(StateStoreConnectionMonitorService.class);

  private FederationStateStoreService stateStore;

  /**
   * Create a new service to monitor the connectivity of the state store driver.
   *
   * @param store Instance of the state store to be monitored.
   */
  public StateStoreConnectionMonitorService(FederationStateStoreService store) {
    super(StateStoreConnectionMonitorService.class.getName());
    this.stateStore = store;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.setIntervalSecs(
        conf.getInt(DFSConfigKeys.FEDERATION_STATESTORE_CONNECTION_TEST_SECS,
            DFSConfigKeys.FEDERATION_STATESTORE_CONNECTION_TEST_SECS_DEFAULT));
    this.setRunnable(new Runnable() {
      public void run() {
        LOG.debug("Checking state store connection");
        if (!stateStore.isDriverReady()) {
          LOG.info("Attempting to open state store driver.");
          stateStore.loadDriver();
        }
      }
    });
  }
}
