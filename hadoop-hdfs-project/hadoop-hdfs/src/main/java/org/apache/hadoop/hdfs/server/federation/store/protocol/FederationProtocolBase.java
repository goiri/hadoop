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
package org.apache.hadoop.hdfs.server.federation.store.protocol;

/**
 * Base class for all protocol API objects.
 */
public abstract class FederationProtocolBase {

  private static final long PROTOCOL_VERSION = 1L;

  /**
   * The version of this protocol object.
   *
   * @return Protocol version number.
   */
  public long getVersion() {
    return PROTOCOL_VERSION;
  }

  /**
   * Initialize the object.
   */
  public void initialize() {
    // Override if required
  }

  /**
   * Get the protocol object that backs this record.
   *
   * @return A protocol specific serialization of this record.
   */
  public abstract Object getProto();

  /**
   * Sets the protocol for this object.
   *
   * @param proto Protocol.
   */
  public abstract void setProto(Object proto);
}
