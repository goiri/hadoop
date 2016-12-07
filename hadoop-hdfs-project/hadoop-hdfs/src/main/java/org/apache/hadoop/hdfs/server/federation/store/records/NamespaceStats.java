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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;

/**
 * Entry to log the access stats of a
 * {@link org.apache.hadoop.hdfs.server.federation.router.Router Router} in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService
 * FederationStateStoreService}.
 */
public abstract class NamespaceStats extends BaseRecord {

  public abstract String getRouterId();

  public abstract void setRouterId(String routerId);

  public abstract long getTimeWindowStart();

  public abstract void setTimeWindowStart(long timeWindowStart);

  public abstract String getStats();

  public abstract void setStats(String stats);

  public NamespaceStats() {
    super();
  }

  /**
   * Statistics for a router.
   *
   * @param routerId Identifier of the router reporting statistics.
   * @param timeWindowStart Time window start for the records.
   * @param stats Path accesses.
   * @return New statistics.
   * @throws IOException
   */
  public static NamespaceStats newInstance(String routerId,
      long timeWindowStart, PathTree<Long> stats) throws IOException {
    NamespaceStats record =
        FederationProtocolFactory.newInstance(NamespaceStats.class);
    record.setRouterId(routerId);
    record.setTimeWindowStart(timeWindowStart);
    record.setStats(stats.toJSON());
    return record;
  }

  /**
   * Statistics for a router.
   *
   * @param routerId Identifier of the router reporting statistics.
   * @param timeWindowStart Time window start for the records.
   * @param stats Path accesses.
   * @return New statistics.
   * @throws IOException
   */
  public static NamespaceStats newInstance(String routerId,
      long timeWindowStart, String stats) throws IOException {
    NamespaceStats record =
        FederationProtocolFactory.newInstance(NamespaceStats.class);
    record.setRouterId(routerId);
    record.setTimeWindowStart(timeWindowStart);
    record.setStats(stats);
    return record;
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("routerId", getRouterId());
    map.put("timeWindowStart", String.valueOf(getTimeWindowStart()));
    return map;
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }
}