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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStoreUtils;
/**
 * Service to periodically update the router's current state in the state store.
 */
public class RouterHeartbeatService extends PeriodicService {
  private static final Log LOG =
      LogFactory.getLog(RouterHeartbeatService.class);

  private final Router router;

  /**
   * Create a new Router heartbeat service.
   *
   * @param router Router to heartbeat.
   */
  public RouterHeartbeatService(Router router) {
    super(RouterHeartbeatService.class.getName());
    this.router = router;
  }

  public void updateState() {
    String routerId = router.getRouterId();
    if (routerId == null) {
      LOG.error("Unable to update router state, router ID is unknown");
      return;
    }
    FederationRouterStateStore manager = router.getRouterStateManager();
    if (manager != null) {
      try {
        if (!FederationRouterStateStoreUtils.sendRouterHeartbeat(manager,
            routerId,
            router.getRouterState(), router.getStartTime())) {
          LOG.warn("Unable to record router heartbeat for router " + routerId);
        } else {
          LOG.debug("Recorded router heartbeat for router " + routerId);
        }
      } catch (IOException e) {
        LOG.error("Unable to record router heartbeat for router - " + routerId
            + " exception - " + e.getMessage());
      }
    } else {
      LOG.warn("Router state manager is not available, "
          + "unable to record router heartbeat");
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    // Use same interval as cache update service
    this.setIntervalSecs(conf.getInt(
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_KEY,
        DFSConfigKeys.FEDERATION_ROUTER_CACHE_TIME_TO_LIVE_SECS_DEFAULT));

    this.setRunnable(new Runnable() {
      public void run() {
        updateState();
      }
    });
  }
}
