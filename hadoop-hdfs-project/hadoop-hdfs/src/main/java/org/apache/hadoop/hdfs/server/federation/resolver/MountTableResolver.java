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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationCachedStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.impl.KeyValueCache;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeResult;

/**
 * Mount table to map between global paths and remote locations. This allows the
 * {@link org.apache.hadoop.hdfs.server.federation.router.Router Router} to map
 * the global HDFS view to the remote namespaces. This is similar to
 * {@link org.apache.hadoop.fs.viewfs.ViewFs ViewFs}.
 * This is implemented as a tree.
 */
public class MountTableResolver
    implements FileSubclusterResolver, FederationCachedStateStore {

  private static final Log LOG =
      LogFactory.getLog(MountTableResolver.class);

  /** Reference to the State Store. */
  private final FederationStateStoreService stateStore;

  /** Path -> Remote HDFS location. */
  private PathTree<String> tree = new PathTree<String>();
  private KeyValueCache<MountTable> cache;
  // TODO Add expiration for stale entries in the cache.
  private ConcurrentNavigableMap<String, PathLocation> locationCache;

  /** Default nameservice when no mount matches the math. */
  private String defaultNameService = "";

  /** Synchronization. */
  private Object treeUpdateLock = new Object();
  private Object cacheUpdateLock = new Object();

  public static final String PATH_SEPARATOR = "/";

  public MountTableResolver(
      Configuration conf, FederationStateStoreService store) {

    this.stateStore = store;
    this.cache = new KeyValueCache<MountTable>();
    this.locationCache =
        new ConcurrentSkipListMap<String, PathLocation>();

    // Request cache updates from the state store
    if (this.stateStore != null) {
      this.stateStore.registerRemoteCache(this);
    }

    // Nameservice for APIs that cannot be resolved to a specific one
    this.defaultNameService = conf.get(DFS_ROUTER_DEFAULT_NAMESERVICE,
        DFSUtil.getNamenodeNameServiceId(conf));
  }

  /**
   * Add a mount entry to the table.
   *
   * @param entry The mount table record to add from the state store.
   */
  public void addEntry(MountTable entry) {
    synchronized (treeUpdateLock) {
      cache.add(entry.getSourcePath(), entry);
      cache.setInitialized();
      this.tree.add(entry.getSourcePath(), entry.getSourcePath());
      invalidateCache(entry.getSourcePath());
    }
  }

  /**
   * Remove a mount table entry.
   *
   * @param src Source path for the entry to remove.
   */
  public void removeEntry(String src) {
    synchronized (treeUpdateLock) {
      this.tree.remove(src);
      cache.remove(src);
      invalidateCache(src);
    }
  }

  /**
   * Invalidates all cache entries below this path.
   *
   * @param src Source path.
   */
  private void invalidateCache(String src) {
    if (locationCache.isEmpty()) {
      return;
    }
    // TODO Determine next lexographic entry after source path
    String nextSrc = src + "ZZZZ";
    ConcurrentNavigableMap<String, PathLocation> subMap =
        locationCache.subMap(src, nextSrc);
    // TODO - ensure distributed locking model guarantees that the cache is
    // cleared and the tree updated at a certain checkpoint.
    for (String key : subMap.keySet()) {
      locationCache.remove(key);
    }
  }

  private PathTree<String> buildTree(KeyValueCache<MountTable> data) {
    PathTree<String> newTree = new PathTree<String>();
    for (String path : data.getKeys()) {
      newTree.add(path, path);
    }
    return newTree;
  }

  /**
   * Rebuilds the entire mount tree from the cache.
   */
  public void rebuildTree(KeyValueCache<MountTable> data) {
    PathTree<String> newTree = buildTree(data);
    synchronized (treeUpdateLock) {
      // Update tree
      this.tree = newTree;
      // Caches must match new tree
      LOG.info("Clearing all mount location caches.");
      this.locationCache.clear();
      this.cache = data;
      this.cache.setInitialized();
    }
  }

  /**
   * Replaces the mount path tree with a new set of mount table entries.
   * @param entries Full set of mount table entries
   */
  public void refreshEntries(Collection<MountTable> entries) {

    synchronized (treeUpdateLock) {
      // Only one update/refresh allowed at a time. The read/write
      // of the tree must be atomic
      List<String> addList = new ArrayList<String>();
      KeyValueCache<MountTable> data =
          new KeyValueCache<MountTable>();
      List<String> existingNodes = null;

      // Build a list of differences
      existingNodes = this.tree.getAllReferences("/");
      for (MountTable entry : entries) {
        String source = entry.getSourcePath();
        data.add(source, entry);
        if (!existingNodes.contains(source)) {
          // Add node, it does not exist
          addList.add(source);
        } else {
          // Node exists, check for updates
          MountTable existingEntry = this.cache.get(source);
          if (existingEntry != null && !existingEntry.equals(entry)) {
            // Entry has changed
            invalidateCache(source);
          }
        }
        existingNodes.remove(source);
      }

      if (addList.size() > 0 || existingNodes.size() > 0) {
        // Remove the nodes that were not found and invalidate
        // all cached children.
        for (String path : existingNodes) {
          this.tree.remove(path);
          invalidateCache(path);
          LOG.info("Removing stale mount path " + path + " from resolver.");
        }
        // Add new paths
        for (String path : addList) {
          LOG.info("Adding new mount path " + path + " to resolver.");
          this.tree.add(path, path);
          invalidateCache(path);
        }
      }
      // Update cached entries with the new data.
      this.cache = data;
      cache.setInitialized();
    }
  }

  /**
   * Replaces the current in-memory cached of the mount table with a new
   * version fetched from the data store.
   */
  @Override
  public boolean loadData() {
    try {
      FederationMountTableStore store = stateStore.getRegisteredInterface(
          FederationMountTableStore.class);
      QueryResult<MountTable> result =
          FederationMountTableStoreUtils.getMountTableRecords(store, "/");

      List<MountTable> records = result.getRecords();
      refreshEntries(records);
      for (MountTable record : records) {
        // Ack of each record now that they are loaded into the mount table
        // TODO Need to ensure no proxied requests in flight before ack
        try {
          if (!stateStore.getDriver().acknowledgeRecord(
              record, stateStore.getIdentifier())) {
            LOG.error("Unable to acknowledge receipt of mount table entry "
                + record);
          }
        } catch (IOException e) {
          LOG.error(
              "Unable to acknowledge receipt of mount table entry " + record,
              e);
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to fetch mount table entries from state store", e);
      return false;
    }
    return true;
  }

  /**
   * Clears all data.
   */
  public void clear() {
    synchronized (treeUpdateLock) {
      LOG.info("Clearing all mount location caches.");
      this.cache.clear();
      this.locationCache.clear();
      this.tree.clear();
    }
  }

  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    verifyMountTable();
    PathLocation ret = this.locationCache.get(path);
    if (ret == null) {
      synchronized (cacheUpdateLock) {
        // Only allow one thread to populate the cache after a cache miss.
        ret = this.locationCache.get(path);
        if (ret == null) {
          PathTreeResult<String> result =
              this.tree.findDeepestReferencedNode(path);
          if (result != null) {
            ret = this.buildLocation(result);
          } else {
            // Not found, use default location
            RemoteLocation remoteLocation =
                new RemoteLocation(this.defaultNameService, path);
            Set<String> namespaces = new HashSet<String>();
            namespaces.add(this.defaultNameService);
            LinkedList<RemoteLocation> locations =
                new LinkedList<RemoteLocation>();
            locations.add(remoteLocation);
            ret = new PathLocation(null, locations, namespaces);
          }
          if (ret != null) {
            this.locationCache.put(path, ret);
          }
        }
      }
    }
    return ret;
  }

  @Override
  public List<String> getMountPoints(String path) throws IOException {
    verifyMountTable();
    return this.tree.getChildNames(path);
  }

  /**
   * Get all the mount records at or beneath a given path.
   * @param path Path to get the mount points from.
   * @return List of mount table records under the path or null if the path is
   *         not found.
   * @throws StateStoreUnavailableException
   */
  public List<MountTable> getMounts(String path)
      throws StateStoreUnavailableException {
    verifyMountTable();
    List<String> subMounts = this.tree.getSubPaths(path);
    List<MountTable> records = null;
    if (subMounts != null) {
      records = new ArrayList<MountTable>();
      for (String item : subMounts) {
        MountTable record = this.cache.get(item);
        if (record != null) {
          records.add(record);
        }
      }
    }
    return records;
  }

  private void verifyMountTable() throws StateStoreUnavailableException {
    if (!this.cache.isInitialized()) {
      throw new StateStoreUnavailableException(
          "Mount table is not initialized.");
    }
  }

  @Override
  public String toString() {
    return this.tree.toString();
  }

  /**
   * Get a JSON representation of the cached mount table.
   * @return JSON representing the cached mount table.
   */
  public String toJSONString() {
    return "";
    /*
     * readLock.lock(); try { List<Object> ret = new ArrayList<Object>(); for
     * (MountTableRecord entry : this.mountTableMap.values()) {
     * ImmutableMap.Builder<String, Object> innerInfo = ImmutableMap.<String,
     * Object> builder(); innerInfo.put("src", entry.sourcePath);
     * innerInfo.put("locked", entry.locked); innerInfo.put("lockClient",
     * entry.lockClient); innerInfo.put("dateCreated", entry.dateCreated);
     * innerInfo.put("dateModified", entry.dateModified); RemoteLocation dst =
     * entry._destinations.getLast(); innerInfo.put("nameserviceId",
     * dst.namespace); innerInfo.put("path", dst.path);
     * ret.add(innerInfo.build()); } return JSON.toString(ret); } finally {
     * readLock.unlock(); }
     */
  }

  /**
   * Get the remote locations for a path in the name space.
   *
   * @param path Requested path.
   * @return Locations for this path.
   */
  public PathLocation get(String path) {
    PathTreeResult<String> result = this.tree.findDeepestReferencedNode(path);
    if (result != null) {
      PathLocation location = buildLocation(result);
      return location;
    }
    return null;
  }

  /**
   * Build a location for this result beneath the discovered mount point.
   *
   * @return PathLocation containing the namespace, local path and
   */
  public PathLocation buildLocation(PathTreeResult<String> result) {

    LinkedList<RemoteLocation> locations = new LinkedList<RemoteLocation>();
    String value = result.getValue();
    MountTable record = this.cache.get(value);
    if (record == null) {
      MountTableResolver.LOG.error(
          "No matching mount table record for path " + value);
      return null;
    }
    for (RemoteLocation oneDst : record.getDestinations()) {
      String dest = oneDst.getDest();
      StringBuilder sb = new StringBuilder(dest);
      if (!dest.endsWith(MountTableResolver.PATH_SEPARATOR)
          && result.getRemainingPath().length > 0) {
        sb.append(MountTableResolver.PATH_SEPARATOR);
      }
      for (int i = 0; i < result.getRemainingPath().length; i++) {
        if (i > 0) {
          sb.append(MountTableResolver.PATH_SEPARATOR);
        }
        sb.append(result.getRemainingPath()[i]);
      }
      String nsId = oneDst.getNameserviceId();
      RemoteLocation remoteLocation = new RemoteLocation(nsId, sb.toString());
      locations.add(remoteLocation);
    }
    String path = record.getSourcePath();
    Set<String> nss = record.getNamespaces();
    return new PathLocation(path, locations, nss);
  }

  @Override
  public String getDefaultNamespace() {
    return this.defaultNameService;
  }
}
