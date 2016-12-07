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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * RouterAdminProtocol is used by the administrator to communicate with the
 * Router and perform admin operations to the mount table and the Router.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(DelegationTokenSelector.class)
public interface RouterAdminProtocol {


  /**
   * Get the stats for the accesses to the paths.
   * @param seconds Time window in the past in milliseconds.
   * @param max The maximum over the window or the aggregate if false.
   * @return Tree with the accesses.
   * @throws IOException
   */
  @Idempotent
  PathTree<Long> getPathStats(long timeWindow, boolean max) throws IOException;

  /**
   * Reset router performance counters.
   * @param flags one or more of ROUTER_PERF_FLAG_RPC,
   *          ROUTER_PERF_FLAG_STATESTORE, ROUTER_PERF_FLAG_ERRORS
   * @throws IOException
   */
  @Idempotent
  void resetPerfCounters(long flags) throws IOException;

  /**
   * Update the router's cached configuration. Only relavent for the current
   * run, on restart the configuration is reloaded.
   * @param key conf key
   * @param value conf value
   * @throws IOException
   */
  @Idempotent
  void setConfiguration(String key, String value) throws IOException;

  /**
   * Restart the ClientProtocol RPC server, enabling any new configuration
   * settings.
   * @throws IOException
   */
  @Idempotent
  void restartRpcServer() throws IOException;
}
