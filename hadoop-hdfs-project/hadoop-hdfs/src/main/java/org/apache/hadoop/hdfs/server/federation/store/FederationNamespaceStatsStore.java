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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;

/**
 * Management API for
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.NamespaceStats
 * NamespaceStats}
 * records in the state store. Accesses the data store via the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver}
 * interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationNamespaceStatsStore {

  /**
   * Upload the current namespace stats to the State Store.
   *
   * @param routerId Identifier of the Router.
   * @param timeWindowStart When the record starts counting.
   * @param stats Statistics about the accesses to the namespace.
   * @throws IOException if the data store cannot be queried.
   */
  boolean uploadPathStats(String routerId, long timeWindowStart,
      PathTree<Long> stats) throws IOException;

  /**
   * Get the current access stats of the federated namespace from the State
   * Store.
   *
   * @return Stats of the tree.
   * @throws IOException if the data store cannot be queried.
   */
  PathTree<Long> getPathStats() throws IOException;

  /**
   * Get the current access stats of the federated namespace from the State
   * Store.
   *
   * @param timeWindow Window Time in the past to query.
   * @param max If we report the maximum.
   * @return Stats of the tree.
   * @throws IOException if the data store cannot be queried.
   */
  PathTree<Long> getPathStats(long timeWindow, boolean max) throws IOException;
}