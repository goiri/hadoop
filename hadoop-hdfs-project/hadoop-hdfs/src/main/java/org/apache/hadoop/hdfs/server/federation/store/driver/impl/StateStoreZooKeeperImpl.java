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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.Barrier;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Preconditions;

/**
 * {@link StateStoreDriver} driver implementation that uses ZooKeeper as a
 * backend.
 * <p>
 * The structure of the znodes in the ensemble is:
 * PARENT_PATH
 * |--- MOUNT
 * |--- MEMBERSHIP
 * |--- REBALANCER
 * |--- ROUTERS
 */
public class StateStoreZooKeeperImpl extends StateStoreDriver {

  private static final Log LOG =
      LogFactory.getLog(StateStoreZooKeeperImpl.class);

  /** Configuration keys. */
  public static final String FEDERATION_STATESTORE_ZK_PARENT_PATH =
      "dfs.federation.statestore.client.zk.parent-path";
  public static final String FEDERATION_STATESTORE_ZK_PARENT_PATH_DEFAULT =
      "/hdfs-federation";

  /** Directory to store the state store data. */
  private String baseZNode;

  // ZooKeeper settings
  private int zkSessionTimeout = 15000;
  private int zkNumRetries = 1000;
  private int zkRetryInterval = 1000;
  private List<ACL> zkAcl;
  private List<ZKUtil.ZKAuthInfo> zkAuths;

  protected CuratorFramework curatorFramework;

  /** Mark for slashes in znode names. */
  private static final String SLASH_MARK = "0SLASH0";

  @Override
  public boolean initDriver() {
    LOG.info("Initializing ZooKeeper connection");

    Configuration conf = getConf();
    baseZNode = conf.get(
        FEDERATION_STATESTORE_ZK_PARENT_PATH,
        FEDERATION_STATESTORE_ZK_PARENT_PATH_DEFAULT);
    try {
      curatorFramework = createAndStartCurator(conf);
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
      return false;
    }
    return true;
  }

  /**
   * Create and start the ZooKeeper Curator.
   *
   * @param conf Configuration for the curator.
   * @return Curator framework.
   * @throws IOException If the initialization failed.
   */
  private CuratorFramework createAndStartCurator(Configuration conf)
      throws IOException {

    String zkHostPort = conf.get(ZKFailoverController.ZK_QUORUM_KEY);
    if (zkHostPort == null) {
      throw new RuntimeException(
          ZKFailoverController.ZK_QUORUM_KEY + " is not configured.");
    }

    // Setup ZK auths
    this.zkAuths = getZKAuths(conf);

    this.zkAcl = getZKAcls(conf);

    Builder builder = CuratorFrameworkFactory.builder()
    	.connectString(zkHostPort)
    	.sessionTimeoutMs(zkSessionTimeout)
    	.retryPolicy(new RetryNTimes(zkNumRetries, zkRetryInterval));
    for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
      builder.authorization(zkAuth.getScheme(), zkAuth.getAuth());
    }
    CuratorFramework client = builder.build();
    client.start();
    return client;
  }

  /**
   * Get the ZK authorization from the configuration.
   *
   * @param conf Configuration.
   * @return List of ZK authorizations.
   * @throws IOException If the Zookeeper ACLs configuration file
   *                     cannot be read
   */
  private List<ZKUtil.ZKAuthInfo> getZKAuths(Configuration conf)
      throws IOException {
    try {
      String zkAuthConf = conf.get(ZKFailoverController.ZK_AUTH_KEY);
      zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
      if (zkAuthConf != null) {
        return ZKUtil.parseAuth(zkAuthConf);
      } else {
        return Collections.emptyList();
      }
    } catch (ZKUtil.BadAuthFormatException e) {
      LOG.error("Couldn't read Auth based on " +
          ZKFailoverController.ZK_AUTH_KEY);
      throw e;
    } catch (IOException e) {
      LOG.error("Couldn't read Auth based on " +
          ZKFailoverController.ZK_AUTH_KEY);
      throw e;
    }
  }

  /**
   * Get the ZK ACLs from the configuration.
   *
   * @param conf Configuration.
   * @return List of ACLs.
   * @throws IOException
   */
  private static List<ACL> getZKAcls(Configuration conf) throws IOException {
    // Parse authentication from configuration.
    String zkAclConf  = conf.get(
        ZKFailoverController.ZK_ACL_KEY,
        "world:anyone:rwcda");
    try {
      zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
      return ZKUtil.parseACLs(zkAclConf);
    } catch (ZKUtil.BadAclFormatException e) {
      LOG.error("Couldn't read ACLs based on " + ZKFailoverController.ZK_ACL_KEY);
      throw e;
    } catch (IOException e) {
      LOG.error("Couldn't read ACLs based on " + ZKFailoverController.ZK_ACL_KEY);
      throw e;
    }
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> clazz) {
    try {
      String checkPath = getNodePath(baseZNode, className);
      createRootDirRecursively(checkPath);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot initialize ZK node for " + className, e);
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    if (curatorFramework != null) {
      curatorFramework.close();
    }
  }

  @Override
  public boolean isDriverReady() {
    return curatorFramework != null;
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    this.verifyDriverReady();
    long start = monotonicNow();
    List<T> ret = new ArrayList<T>();
    String znode = getZNodeForClass(clazz);
    if (znode == null) {
      LOG.error("Cannot find znode for " + clazz);
    } else {
      try {
        // Fetch all children
        List<String> children = getChildren(znode);
        for (String child : children) {
          try {
            String path = getNodePath(znode, child);
            Stat stat = new Stat();
            String data = getData(path, stat);
            if (data != null) {
              try {
                @SuppressWarnings("unchecked")
                T record = (T) createRecord(data, stat, clazz);
                ret.add(record);
              } catch (Exception e) {
                LOG.error("Failed to create record type \"" +
                    clazz.getSimpleName() + "\" from \"" + data + "\" error: " +
                    e.getMessage());
              }
            }
          } catch (Exception e) {
            LOG.error("Cannot get data  for " + child, e);
          }
        }
      } catch (Exception e) {
        LOG.error("Cannot get children for " + znode, e);
        getMetrics().addFailure(monotonicNow() - start);
        throw new IOException("Unable to query data store " + e.getMessage());
      }
    }
    long end = monotonicNow();
    getMetrics().addRead(end - start);
    return new QueryResult<T>(ret, getTime());
  }

  @Override
  public <T extends BaseRecord> boolean updateOrCreate(
      List<T> records, Class<T> clazz, boolean update, boolean error)
          throws IOException {
    this.verifyDriverReady();
    if (records.isEmpty()) {
      return true;
    }
    long start = monotonicNow();
    boolean status = true;
    String znode = getZNodeForClass(clazz);
    for (BaseRecord record : records) {
      if (znode == null) {
        znode = getZNodeForClass(record.getClass());
      }
      String primaryKey = getPrimaryKey(record);
      String recordZNode = getNodePath(znode, primaryKey);
      String data = record.serialize();
      if (!writeNode(recordZNode, data, update, error)){
        status = false;
      }
    }
    long end = monotonicNow();
    if (status) {
      getMetrics().addWrite(end - start);
    } else {
      getMetrics().addFailure(end - start);
    }
    return status;
  }

  @Override
  public <T extends BaseRecord> int delete(Class<T> clazz,
      Map<String, String> filter) throws IOException {
    if (filter.isEmpty()) {
      return 0;
    }
    long start = monotonicNow();
    // Read the current file
    QueryResult<T> result;
    try {
      result = this.get(clazz);
    } catch (IOException ex) {
      LOG.error("Unable to fetch existing records.", ex);
      getMetrics().addFailure(monotonicNow() - start);
      return 0;
    }

    String znode = getZNodeForClass(clazz);
    List<T> records = result.getRecords();
    List<T> recordsToDelete = FederationStateStoreUtils.filterMultiple(
        filter, records);
    if (recordsToDelete == null) {
      getMetrics().addFailure(Time.monotonicNow() - start);
      return 0;
    }
    int deleted = 0;
    for (BaseRecord existingRecord : recordsToDelete) {
      LOG.info("Removing " + existingRecord);
      try {
        String primaryKey = getPrimaryKey(existingRecord);
        String path = getNodePath(znode, primaryKey);
        delete(path);
        deleted++;
      } catch (Exception e) {
        LOG.error("Problem removing " + existingRecord, e);
        getMetrics().addFailure(Time.monotonicNow() - start);
      }
    }
    long end = monotonicNow();
    if (deleted > 0) {
      getMetrics().addDelete(end - start);
    }
    return deleted;
  }

  @Override
  public <T extends BaseRecord> boolean delete(Class<T> clazz)
      throws IOException {
    String znode = getZNodeForClass(clazz);
    long start = monotonicNow();
    boolean status = true;
    if (znode == null) {
      LOG.error("Cannot find znode for " + clazz);
      status = false;
    } else {
      LOG.info("Deleting all children under " + znode);
      try {
        List<String> children = getChildren(znode);
        for (String child : children) {
          String path = getNodePath(znode, child);
          LOG.info("Deleting " + path);
          delete(path);
        }
      } catch (Exception e) {
        LOG.error("Cannot delete " + znode, e);
        status = false;
      }
    }

    long time = Time.monotonicNow() - start;
    if (status) {
      getMetrics().addDelete(time);
    } else {
      getMetrics().addFailure(time);
    }
    return status;
  }

  @Override
  public boolean supportsBarriers() {
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean acknowledgeRecord(
      T record, String identifier) throws IOException {
    // Add a child barrier node to the record
    String znode = getPath(record) + "/" + identifier;
    boolean status = true;
    if (!writeNode(znode, identifier, false, false)) {
      LOG.warn("Unable to acknowledge record " + record.toString());
      status = false;
    }
    return status;
  }

  @Override
  public <T extends BaseRecord> void waitForAcknowledgement(
      T record, List<String> identifiers, long timeoutMs)
          throws IOException, TimeoutException {

    long startTime = Time.monotonicNow();
    String znode = getPath(record);
    while ((Time.monotonicNow() - startTime) < timeoutMs) {
      try {
        List<String> children = getChildren(znode);
        if (children.contains(Barrier.BARRIER_IDENTIFIER)) {
          LOG.info("Barrier has already been set for " + record);
          return;
        }
        for (String child : children) {
          if (identifiers.contains(child)) {
            identifiers.remove(child);
          }
        }
        if (identifiers.isEmpty()) {
          // All acks are present, add barrier
          LOG.info("Setting barrier for " + record);
          if (!acknowledgeRecord(record, Barrier.BARRIER_IDENTIFIER)) {
            LOG.warn("Unable to create barrier for record " + record);
          }
          return;
        }
      } catch (Exception e) {
        LOG.error("Unable to inspect acknowledgements for record " + record, e);
      }
      try {
        // Wait
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException("Unable to query for record " + record);
      }
    }
    throw new TimeoutException("Unable to locate barrier for record " + record
        + " in " + timeoutMs + "ms.");
  }

  /**
   * Get the ZK path for a record.
   *
   * @param record Record to get the path for.
   * @return Path for the record.
   */
  private <T extends BaseRecord> String getPath(T record) {
    String primaryKey = getPrimaryKey(record);
    String znode = getZNodeForClass(record.getClass());
    return getNodePath(znode, primaryKey);
  }

  /**
   * Write data into a ZNode.
   *
   * @param znode Path of the znode.
   * @param data Data to write.
   * @param update Allow update.
   * @param error Error if it exists.
   * @return If we were able to write
   */
  private boolean writeNode(
      String znode, String data, boolean update, boolean error) {
    try {
      boolean created = create(znode);
      if (!update && !created && error) {
        LOG.info(
            "Attempt to insert record " + znode + " that already exists.");
        return false;
      }
      setData(znode, data.getBytes(), -1);
      return true;
    } catch (Exception e) {
      LOG.error("Unable to write record " + znode + " to ZK", e);
    }
    return false;
  }

  /**
   * Utility function to ensure that the configured base znode exists.
   * This recursively creates the znode as well as all of its parents.
   */
  private void createRootDirRecursively(String path) throws Exception {
    String pathParts[] = path.split("/");
    Preconditions.checkArgument(pathParts.length >= 1 && pathParts[0].isEmpty(),
        "Invalid path: %s", path);
    StringBuilder sb = new StringBuilder();

    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
      create(sb.toString());
    }
  }

  /**
   * Create a ZNode.
   *
   * @param path Path of the ZNode.
   * @return If the ZNode was created.
   * @throws Exception
   */
  private boolean create(final String path) throws Exception {
    boolean created = false;
    if (!exists(path)) {
      curatorFramework.create()
          .withMode(CreateMode.PERSISTENT).withACL(zkAcl)
          .forPath(path, null);
      created = true;
    }
    return created;
  }

  /**
   * Delete a ZNode.
   *
   * @param path Path of the ZNode.
   * @throws Exception
   */
  private void delete(final String path) throws Exception {
    if (exists(path)) {
      curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  /**
   * Get children of a ZNode.
   *
   * @param path Path of the ZNode.
   * @return The list of children.
   * @throws Exception
   */
  private List<String> getChildren(final String path) throws Exception {
    return curatorFramework.getChildren().forPath(path);
  }

  /**
   * Check if a ZNode exists.
   *
   * @param path Path of the ZNode.
   * @return If the ZNode exists.
   * @throws Exception
   */
  private boolean exists(final String path) throws Exception {
    return curatorFramework.checkExists().forPath(path) != null;
  }

  /**
   * Get the data in a ZNode.
   *
   * @param path Path of the ZNode.
   * @param stat Output statistics of the ZNode.
   * @return The data in the ZNode.
   * @throws Exception
   */
  private String getData(final String path, Stat stat) throws Exception {
    byte[] data = curatorFramework.getData().storingStatIn(stat).forPath(path);
    return new String(data);
  }

  /**
   * Set data into a ZNode.
   *
   * @param path Path of the ZNode.
   * @param data Data to set.
   * @param version Version of the data to store.
   * @throws Exception
   */
  private void setData(String path, byte[] data, int version) throws Exception {
    curatorFramework.setData().withVersion(version).forPath(path, data);
  }

  /**
   * Get the ZNode for a class.
   *
   * @param clazz Record class to evaluate.
   * @return The ZNode for the class.
   */
  private <T extends BaseRecord> String getZNodeForClass(Class<T> clazz) {
    return getNodePath(baseZNode, getDataNameForRecord(clazz));
  }

  /**
   * Creates a record from a string returned by ZooKeeper.
   *
   * @param source Object from ZooKeeper.
   * @param clazz The data record type to create.
   * @return The created record.
   * @throws IOException
   */
  private BaseRecord createRecord(
      String data, Stat stat, Class<? extends BaseRecord> clazz)
          throws IOException {
    BaseRecord record = createRecord(data, clazz, false);
    record.setDateCreated(stat.getCtime());
    record.setDateModified(stat.getMtime());
    return record;
  }

  /**
   * Get the primary key for a record.
   *
   * @param record Record to get the primary key for.
   * @return Primary key for the record.
   */
  private <T extends BaseRecord> String getPrimaryKey(T record) {
    String primaryKey = record.getPrimaryKey();
    primaryKey = primaryKey.replaceAll("/", SLASH_MARK);
    return primaryKey;
  }

  /**
   * Get the path for a ZNode.
   *
   * @param root Root of the ZNode.
   * @param nodeName Name of the ZNode.
   * @return PAth for the ZNode.
   */
  private String getNodePath(String root, String nodeName) {
    return (root + "/" + nodeName);
  }
}
