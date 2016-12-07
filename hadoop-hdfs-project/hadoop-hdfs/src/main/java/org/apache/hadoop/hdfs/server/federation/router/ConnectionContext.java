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

import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/**
 * Context to track metrics of the client connections of the pool.
 */
public class ConnectionContext {

  /** Client for the connection. */
  private ProxyAndInfo<ClientProtocol> client;
  /** IF the connection is busy. */
  private boolean busy;

  public ConnectionContext(ProxyAndInfo<ClientProtocol> connection) {
    this.client = connection;
    this.busy = false;
  }

  /**
   * Check if the connection is busy.
   *
   * @return If the connection is busy.
   */
  public boolean isBusy() {
    return this.busy;
  }

  /**
   * Get the connection client.
   *
   * @return Connection client.
   */
  public ProxyAndInfo<ClientProtocol> getClient() {
    this.busy = true;
    return this.client;
  }

  /**
   * Release the client.
   */
  public void releaseClient() {
    this.busy = false;
  }
}
