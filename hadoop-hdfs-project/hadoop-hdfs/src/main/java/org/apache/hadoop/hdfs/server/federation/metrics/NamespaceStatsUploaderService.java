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
package org.apache.hadoop.hdfs.server.federation.metrics;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.PeriodicService;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcMonitor;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceStatsStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeUtils;
import org.apache.hadoop.util.Time;

/**
 * Class that gets the accesses stats from the RPC monitor and uploads them to
 * the {@link FederationStateStoreService}.
 */
public class NamespaceStatsUploaderService extends PeriodicService {

  private static final Log LOG = LogFactory.getLog(
      NamespaceStatsUploaderService.class);

  /** Time window to track statistics: 5 minutes. */
  protected static final long STATS_TIME_WINDOW =
      TimeUnit.MINUTES.toMillis(5);

  /** We trim anything with less than 0.1% of the accesses. */
  protected static final float STATS_TRIM_PCT = 0.1f;


  /** Current time window we are monitoring. */
  private long curTimeWindow = getTimeWindow();

  /** RPC monitor that tracks the access stats. */
  private final RouterRpcMonitor monitor;

  /**
   * Initialize the stat uploader service.
   * @param router
   * @param monitor
   */
  public NamespaceStatsUploaderService(RouterRpcMonitor monitor) {
    super(NamespaceStatsUploaderService.class.getName());
    this.monitor = monitor;
  }

  /**
   * Get the current time window identifier.
   * @return Current time window start time.
   */
  private static long getTimeWindow() {
    long timeWindow = Time.now();
    timeWindow -= (timeWindow % STATS_TIME_WINDOW);
    return timeWindow;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Use same interval as cache update service
    this.setIntervalSecs(conf.getInt(
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY,
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_DEFAULT));

    this.setRunnable(new Runnable() {
      public void run() {

        FederationStateStoreService stateStore =
            FederationStateStoreService.getService();
        if (stateStore == null) {
          return;
        }

        FederationNamespaceStatsStore statsStore = stateStore
            .getRegisteredInterface(FederationNamespaceStatsStore.class);

        String routerId =
            FederationStateStoreService.getService().getIdentifier();

        if (statsStore != null) {
          // Trim the tree to at most 0.1%
          PathTree<Long> stats = monitor.getPathStats();
          PathTreeUtils.trim(stats, STATS_TRIM_PCT / 100f);

          // Upload data to the State Store if there's something to upload
          if (stats.getNumOfLeaves() > 0) {
            boolean success;
            try {
              success =
                  statsStore.uploadPathStats(routerId, curTimeWindow, stats);
            } catch (IOException e) {
              success = false;
            }
            if (!success) {
              LOG.error("Cannot upload load stats");
            }
          }

          // Check if we are moving to the next time window
          long newTimeWindow = getTimeWindow();
          if (newTimeWindow != curTimeWindow) {
            // Reset the statistics
            stats.clear();
            curTimeWindow = newTimeWindow;
          }
        }
      }
    });
  }
}