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

/**
 * State store operation used by the Rebalancer.
 *
 * @param <T>
 */
public abstract class StateStoreOperation<T> {

  private static final Log LOG = LogFactory.getLog(StateStoreOperation.class);

  private FederationStateStoreService stateStore;

  public StateStoreOperation(FederationStateStoreService store) {
    this.stateStore = store;
  }

  /**
   * Run the operation.
   *
   * @param store State store.
   * @return The result of the operation.
   * @throws Exception
   */
  public abstract T run(FederationStateStoreService store)
      throws Exception;

  /**
   * Retry a method that throws a StateStoreUnavailableException.
   *
   * @param numRetries Number of retries before giving up.
   * @param intervalMs Time in milliseconds to wait between retries.
   * @throws StateStoreUnavailableException If the state store not available.
   * @throws InterruptedException If we cannot wait.
   * @throws Exception The exception thrown by the method.
   */
  public T retryOperation(int numRetries, int intervalMs) throws Exception {
    for (int index = numRetries; index > 0; index--) {
      try {
        return run(this.stateStore);
      } catch (StateStoreUnavailableException ex) {
        LOG.info("Method generated a StateStoreUnavailableException," +
            " retrying.");
        Thread.sleep(intervalMs);
      }
    }
    throw new StateStoreUnavailableException(
        "Unable to access state store after " + numRetries + "attempts.");
  }
}