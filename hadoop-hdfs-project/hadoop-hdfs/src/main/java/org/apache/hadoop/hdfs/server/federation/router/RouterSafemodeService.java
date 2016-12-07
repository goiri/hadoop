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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.util.Time.now;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.util.Time;

/**
 * Service to periodically check if the {@link org.apache.hadoop.hdfs.server.
 * federation.store.FederationStateStoreService FederationStateStoreService}
 * cached information in the {@link Router} is up to date. This is for
 * performance and removes the {@link org.apache.hadoop.hdfs.server.
 * federation.store.FederationStateStoreService FederationStateStoreService}
 * from the critical path in common operations.
 */
public class RouterSafemodeService extends PeriodicService {

  private static final Log LOG =
      LogFactory.getLog(RouterSafemodeService.class);

  /** Router to manage safe mode. */
  private final Router router;

  /** Interval to wait post startup before allowing RPC requests. */
  private long startupInterval;
  /** Interval after which the State Store cache is too stale. */
  private long staleInterval;
  /** Start time of this service. */
  private long startupTime;

  /** The start time of the Router. */
  private final long startTime = now();


  /**
   * Create a new Cache update service.
   *
   * @param router Router containing the cache.
   */
  public RouterSafemodeService(Router router) {
    super(RouterSafemodeService.class.getName());
    this.router = router;
  }

  /**
   * Enter safe mode.
   */
  private void enter() {
    LOG.info("Entering safe mode");
    router.getRpcServer().setSafeMode(true);
    router.updateRouterState(RouterServiceState.SAFEMODE);
  }

  /**
   * Leave safe mode.
   */
  private synchronized void leave() {
    // Cache recently updated, leave safemode
    LOG.info("Leaving safe mode");
    long timeInSafemode = now() - startTime;
    Router.getRouterMetrics().setSafeModeTime((int) timeInSafemode);
    router.getRpcServer().setSafeMode(false);
    router.updateRouterState(RouterServiceState.RUNNING);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Use same interval as cache update service
    this.setIntervalSecs(conf.getInt(
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY,
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_DEFAULT));

    this.startupInterval = 1000 * conf.getInt(
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_STARTUP_INTERVAL,
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_STARTUP_INTERVAL_DEFAULT);

    this.staleInterval = 1000 * conf.getInt(
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_CACHE_EXPIRATION_SECS,
        DFSConfigKeys.DFS_ROUTER_SAFEMODE_CACHE_EXPIRATION_SECS_DEFAULT);

    this.startupTime = Time.now();

    // Initializing the RPC server in safe mode, it will disable it later
    RouterRpcServer rpcServer = this.router.getRpcServer();
    rpcServer.setSafeMode(true);
    this.router.updateRouterState(RouterServiceState.SAFEMODE);

    this.setRunnable(new Runnable() {
      public void run() {
        long now = Time.now();
        long delta = now - startupTime;
        if (delta < startupInterval) {
          LOG.info("Delaying safemode exit for " +
              ((startupInterval - delta) / 1000) + " seconds...");
          return;
        }
        RouterRpcServer rpcServer = router.getRpcServer();
        FederationStateStoreService stateStore = router.getStateStore();
        long cacheUpdateTime = stateStore.getCacheUpdateTime();
        boolean isCacheStale = (now - cacheUpdateTime) > staleInterval;
        // Always update to indicate our cache was updated
        if (isCacheStale) {
          if (!rpcServer.isSafeMode()) {
            enter();
          }
        } else if (rpcServer.isSafeMode()) {
          // Cache recently updated, leave safe mode
          leave();
        }
      }
    });
  }
}
