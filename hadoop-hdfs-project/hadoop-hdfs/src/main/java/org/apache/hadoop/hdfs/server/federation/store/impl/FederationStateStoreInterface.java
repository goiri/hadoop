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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

/**
 * Abstract parent class for state store API handler classes. Each extending
 * class is responsible for keeping internal state, by default the state store
 * will create a single instance of each interface handler.
 */
public abstract class FederationStateStoreInterface {

  /** State store driver backed by persistent storage. */
  private StateStoreDriver driver;

  /**
   * Set the fully initialized state store instance.
   *
   * @param driver The {@link StateStoreDriver} implementation in use.
   */
  public void setDriver(StateStoreDriver stateStoreDriver) {
    this.driver = stateStoreDriver;
  }

  /**
   * Get the state store instance.
   *
   * @return StateStore instance.
   */
  public StateStoreDriver getDriver() {
    return this.driver;
  }

  /**
   * Build a state store API implementation interface.
   *
   * @param interfaceClass The specific interface implementation to create
   * @param driver The {@link StateStoreDriver} implementation in use.
   * @return An initialized instance of the specified state store API
   *         implementation.
   */
  public static <T extends FederationStateStoreInterface> T createInterface(
      Class<T> clazz, StateStoreDriver stateStoreDriver) {

    try {
      T interfaceInstance = clazz.newInstance();
      interfaceInstance.setDriver(stateStoreDriver);
      interfaceInstance.init();
      return interfaceInstance;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Initialization function for the interface. The state store parent is fully
   * initialized at this point.
   */
  public abstract void init();

  /**
   * Report a required record to the data store. The data store uses this to
   * create/maintain storage for the record.
   *
   * @return The class of the required record or null if no record is required
   *         for this interface.
   */
  public abstract Class<? extends BaseRecord> requiredRecord();
}
