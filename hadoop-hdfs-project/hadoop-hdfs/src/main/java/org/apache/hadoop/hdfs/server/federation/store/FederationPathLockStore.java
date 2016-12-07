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
import org.apache.hadoop.hdfs.server.federation.store.records.PathLock;

/**
 * Management API for the HDFS path lock information stored in {@link PathLock}
 * records. Each path lock corresponds to the root of a file tree in the global
 * namespace that is currently write-protected.
 * <p>
 * Once fetched from the {@link org.apache.hadoop.hdfs.server.federation.store.
 * driver.StateStoreDriver StateStoreDriver}, PathLock records are cached for
 * faster access. The cache is periodically updated by the
 * @{link StateStoreCacheUpdateService}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationPathLockStore {

  /**
   * Request a lock on a path. Fails if the path is already locked by someone
   * else. Renews the lock expiration if already locked by the client.
   *
   * @param path Mount point to lock.
   * @param client unique client that owns this lock.
   * @return The path lock record if successful, null otherwise.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  PathLock lockPath(String path, String client) throws IOException;

  /**
   * Resets the expiration time for a path lock.
   *
   * @param path - The locked path
   * @param client - The client that locked the path
   * @return True if successful, false if failed or the lock was not found.
   * @throws IOException Throws if the data store is not available.
   */
  boolean renewPathLock(String path, String client) throws IOException;

  /**
   * Unlock a path. Fails if the mount is locked by a different client.
   *
   * @param path Mount point to unlock.
   * @param client Unique client that owns this lock.
   * @return True if the mount table was successfully unlocked.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  boolean unlockPath(String path, String client) throws IOException;

  /**
   * Check if a path is locked. Results are from the mount table cache.
   *
   * @param path Path to check.
   * @return If the path is currently locked.
   * @throws StateStoreUnavailableException Throws exception if the data store
   *           is not initialized.
   * @throws IOException if the data store could not be accessed
   */
  boolean isLocked(String path) throws IOException;
}