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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.FederationPathLockStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationCachedStateStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.PathLock;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeResult;

/**
 * Implementation of the {@link FederationPathLockStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationPathLockStoreImpl extends FederationStateStoreInterface
    implements FederationPathLockStore, FederationCachedStateStore {

  private static final Log LOG = LogFactory.getLog(FederationPathLockStoreImpl.class);

  /** Version of the currently loaded path lock table. */
  private long pathLockVersion;
  /** Cached mount table locks. */
  private PathTree<PathLock> pathLockTree;
  /** Lock for accessing the parh lock tree. */
  private final ReadWriteLock pathLockTreeLock = new ReentrantReadWriteLock();
  private boolean intialized = false;

  @Override
  public void init() {
    this.pathLockTree = new PathTree<PathLock>();
  }

  /////////////////////////////////////////////////////////
  // PathLock
  /////////////////////////////////////////////////////////

  private void verifyPathLocks() throws StateStoreUnavailableException {
    if (!intialized) {
      throw new StateStoreUnavailableException(
          "State store is not initialized, path lock records are not valid.");
    }
  }

  @Override
  public boolean loadData() throws IOException {

    // Fetch all
    QueryResult<PathLock> result;
    try {
      result = getDriver().get(PathLock.class);
    } catch (IOException e) {
      intialized = false;
      return false;
    }
    List<PathLock> records = result.getRecords();
    if (records != null) {
      // Delete overrides if expired
      List<PathLock> deleteList = new ArrayList<PathLock>();
      for (PathLock record : records) {
        LOG.info("Found path lock with mod time - " + record.getDateModified());
        long currentDriverTime = getDriver().getTime();
        if (record.checkExpired(currentDriverTime)) {
          LOG.info("Force unlocking expired path " + record.getSourcePath()
              + " for client " + record.getLockClient());
          if (!getDriver().delete(record)) {
            LOG.error("Unable to delete expired path lock record - " + record);
          }
          deleteList.add(record);
        }
      }
      for (PathLock record : deleteList) {
        records.remove(record);
      }

      try {
        pathLockTreeLock.writeLock().lock();
        this.pathLockTree.clear();
        for (PathLock record : records) {
          this.pathLockTree.add(record.getSourcePath(), record);
        }
      } finally {
        pathLockTreeLock.writeLock().unlock();
      }
      this.intialized = true;

      // Ack records that are in the path lock tree
      // TODO Wait for any RPC requests that are being proxied to complete first
      boolean incrementVersion = true;
      for (PathLock record : records) {
        if (record.getDateModified() >= this.pathLockVersion) {
          try {
            if (!getDriver().acknowledgeRecord(record,
                getDriver().getIdentifier())) {
              LOG.error(
                  "Unable to acknowledge receipt of path lock " + record);
              incrementVersion = false;
            }
          } catch (IOException ex) {
            LOG.error("Unable to acknowledge receipt of path lock " + record,
                ex);
            // Some acks failed, try again later
            incrementVersion = false;
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Loaded " + records.size()
            + " elements into the path lock table");
      }
      if (incrementVersion) {
        this.pathLockVersion = result.getTimestamp();
      }
      return true;
    }
    LOG.error("Failed to retrieve mount table records from the state store");
    return false;
  }

  @Override
  public boolean isLocked(String path) throws IOException {

    verifyPathLocks();
    try {
      this.pathLockTreeLock.readLock().lock();
      PathTreeResult<PathLock> result =
          this.pathLockTree.findDeepestReferencedNode(path);
      if (result == null) {
        // Nothing at or above this path is on the lock tree
        return false;
      } else {
        // Something at or above this path on the tree is locked
        return true;
      }
    } finally {
      this.pathLockTreeLock.readLock().unlock();
    }
  }

  @Override
  public boolean renewPathLock(String path, String client) throws IOException {

    Map<String, String> query = new HashMap<String, String>();
    query.put("sourcePath", path);
    query.put("lockClient", client);
    try {
      PathLock record = getDriver().get(PathLock.class, query);
      if (record != null) {
        return getDriver().updateOrCreate(record, true,
            false);
      } else {
        return false;
      }
    } catch (IOException ex) {
      LOG.error("Unable to renew path lock - " + query, ex);
      return false;
    }
  }

  @Override
  public PathLock lockPath(String path, String client)
      throws IOException {

    PathLock record = PathLock.newInstance(path, client);
    // Insert only, fail if exists
    if (getDriver().updateOrCreate(record, false, true)) {
      LOG.info("Path - " + path + " - has been locked by client - " + client);
      return record;
    } else {
      LOG.warn("Failed to lock path - " + path + " - for client - " + client);
      return null;
    }
  }

  @Override
  public boolean unlockPath(String path, String client)
      throws IOException {

    Map<String, String> query = new HashMap<String, String>();
    query.put("sourcePath", path);
    query.put("lockClient", client);
    if (getDriver().delete(PathLock.class,
        query) == 1) {
      LOG.info("Path - " + path + " - has been unlocked by client - " + client);
      return true;
    } else {
      LOG.warn("Failed to unlock path - " + path + " - for client - " + client);
      return false;
    }
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return PathLock.class;
  }
}
