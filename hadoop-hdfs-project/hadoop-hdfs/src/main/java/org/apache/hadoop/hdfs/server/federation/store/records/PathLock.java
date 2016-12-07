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

/**
 * Data record indicating a client write lock on a specific mount path. Used for
 * rebalancing to prevent write operation to a file tree that is being moved.
 */
public abstract class PathLock extends BaseRecord {

  /** Expiration time in ms for this entry. */
  private static long expirationMs;

  public PathLock() {
    super();
  }

  public static PathLock newInstance(String path, String client)
      throws IOException {
    PathLock record = FederationProtocolFactory.newInstance(PathLock.class);
    record.setLockClient(client);
    record.setSourcePath(path);
    return record;
  }

  public abstract void setSourcePath(String sourcePath);

  public abstract void setLockClient(String lockClient);

  public abstract String getSourcePath();

  public abstract String getLockClient();

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> keyMap = new TreeMap<String, String>();
    keyMap.put("sourcePath", this.getSourcePath());
    return keyMap;
  }

  @Override
  public long getExpirationMs() {
    return PathLock.expirationMs;
  }

  public static void setExpirationMs(long time) {
    PathLock.expirationMs = time;
  }
}
