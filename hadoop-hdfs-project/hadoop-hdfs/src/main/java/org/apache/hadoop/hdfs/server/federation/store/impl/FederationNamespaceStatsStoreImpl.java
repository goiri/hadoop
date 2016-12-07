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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceStatsStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.NamespaceStats;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeUtils;
import org.apache.hadoop.util.Time;

/**
 * Implementation of the {@link FederationNamespaceStatsStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationNamespaceStatsStoreImpl
    extends FederationStateStoreInterface
    implements FederationNamespaceStatsStore {

  @Override
  public void init() {
  }

  /////////////////////////////////////////////////////////
  // Upload stats
  /////////////////////////////////////////////////////////

  @Override
  public boolean uploadPathStats(String routerId, long timeWindowStart,
      PathTree<Long> stats) throws IOException {
    NamespaceStats record =
        NamespaceStats.newInstance(routerId, timeWindowStart, stats);
    return getDriver().updateOrCreate(record, true,
        false);
  }

  @Override
  public PathTree<Long> getPathStats() throws StateStoreUnavailableException {
    return getPathStats(0, false);
  }

  @Override
  public PathTree<Long> getPathStats(long timeWindow, boolean max)
      throws StateStoreUnavailableException {

    PathTree<Long> tree = new PathTree<Long>();
    try {
      StateStoreDriver driver = getDriver();
      QueryResult<NamespaceStats> result = driver.get(NamespaceStats.class);
      List<NamespaceStats> routerStats = result.getRecords();

      // Merge the statistics within the desired time window
      for (NamespaceStats routerStat : routerStats) {
        if (timeWindow <= 0
            || routerStat.getTimeWindowStart() >= Time.now() - timeWindow) {
          String stats = routerStat.getStats();
          PathTree<Long> newTree = PathTreeUtils.toTreeLong(stats);
          if (max) {
            PathTreeUtils.max(tree, newTree);
          } else {
            PathTreeUtils.add(tree, newTree);
          }
        }
      }
    } catch (IOException e) {
      return null;
    }

    return tree;
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return NamespaceStats.class;
  }
}
