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
import java.util.ArrayList;
import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMBean;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationDecommissionNamespaceStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationMembershipStateStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationMountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationNamespaceStatsStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationPathLockStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationRebalancerStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationRouterStateStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.FederationStateStoreInterface;
import org.apache.hadoop.hdfs.server.federation.store.records.Barrier;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.PathLock;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * A service to initialize a
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver} and maintain the connection to the data store. There are
 * multiple state store driver connections supported:
 * <ul>
 * <li>File
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.impl.
 * StateStoreFileImpl StateStoreFileImpl}
 * <li>ZooKeeper
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.impl.
 * StateStoreZooKeeperImpl StateStoreZooKeeperImpl}
 * </ul>
 * <p>
 * The service also supports the dynamic registration of data interfaces such as
 * the following:
 * <ul>
 * <li>{@link FederationMembershipStateStore}: state of the Namenodes in the
 * federation.
 * <li>{@link FederationMountTableStore}: Mount table between to subclusters.
 * See {@link org.apache.hadoop.fs.viewfs.ViewFs ViewFs}.
 * <li>{@link FederationRebalancerStore}: Log of the rebalancing operations.
 * <li>{@link RouterStateStore}: State of the routers in the federation.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationStateStoreService extends CompositeService {

  private static final Log LOG =
      LogFactory.getLog(FederationStateStoreService.class);

  /** Singleton for the service. */
  private static FederationStateStoreService sharedService = null;

  /** State Store configuration. */
  private Configuration conf;

  /** Identifier for the service. */
  private String identifier;

  /** Driver for the backend connection. */
  private StateStoreDriver driver;
  /** List of supported record types. */
  private List<Class<? extends BaseRecord>> supportedRecords;
  /** List of default supported interface types. */
  private List<FederationStateStoreInterface> supportedInterfaces;

  /** Service to maintain data store connection. */
  private StateStoreConnectionMonitorService monitorService;

  /** StateStore metrics. */
  private StateStoreMetrics metrics;

  /** Service to maintain state store caches. */
  private StateStoreCacheUpdateService cacheUpdater;
  /** Time the cache was last successfully updated. */
  private long cacheLastUpdateTime;
  /** List of caches to update. */
  private List<FederationCachedStateStore> cachesToUpdate;

  public FederationStateStoreService()
      throws InstantiationException, IllegalAccessException {
    super(FederationStateStoreService.class.getName());
    this.cachesToUpdate = new ArrayList<FederationCachedStateStore>();

    // List of interfaces and records supported by this implementation
    this.supportedRecords = new ArrayList<Class<? extends BaseRecord>>();
    this.supportedInterfaces = new ArrayList<FederationStateStoreInterface>();

    if (sharedService == null) {
      sharedService = this;
    }
  }

  /**
   * Singleton shared service instance.
   */
  public static FederationStateStoreService getService() {
    return sharedService;
  }

  /**
   * Static constructor of a standalone statestore service.
   *
   * @param conf Configuration supporting the statestore.
   * @return StateStore
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public static FederationStateStoreService createStateStore(Configuration conf)
      throws InstantiationException, IllegalAccessException {
    FederationStateStoreService stateStore = new FederationStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    return stateStore;
  }

  /**
   * Wait for the statestore to initialize its driver.
   *
   * @param stateStore
   * @param timeoutMs
   * @throws IOException
   * @throws InterruptedException
   */
  public static void waitStateStore(FederationStateStoreService stateStore,
      long timeoutMs) throws IOException, InterruptedException {
    long startingTime = Time.monotonicNow();
    while (!stateStore.isDriverReady()) {
      Thread.sleep(100);
      if (Time.monotonicNow() - startingTime > timeoutMs) {
        throw new IOException("Timeout waiting for state store to connect");
      }
    }
  }

  /**
   * Initialize the State Store and the connection to the backend.
   *
   * @param config Configuration for the State Store.
   * @throws IOException
   */
  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;

    // Create implementation of state store
    this.driver = (StateStoreDriver) FederationUtil.createInstance(conf,
        DFSConfigKeys.FEDERATION_STATESTORE_CLIENT_CLASS,
        DFSConfigKeys.FEDERATION_STATESTORE_CLIENT_CLASS_DEFAULT,
        StateStoreDriver.class);

    if (driver == null) {
      throw new IOException("Unable to create driver for state store.");
    }

    // Add supported interfaces
    this.supportedInterfaces.add(addInterface(
        FederationDecommissionNamespaceStoreImpl.class));
    this.supportedInterfaces.add(addInterface(
        FederationMembershipStateStoreImpl.class));
    this.supportedInterfaces.add(addInterface(
        FederationRouterStateStoreImpl.class));
    this.supportedInterfaces.add(addInterface(
        FederationPathLockStoreImpl.class));
    this.supportedInterfaces.add(addInterface(
        FederationNamespaceStatsStoreImpl.class));
    this.supportedInterfaces.add(addInterface(
        FederationMountTableStoreImpl.class));
    this.supportedInterfaces.add(addInterface(
        FederationRebalancerStoreImpl.class));

    if (!driver.supportsBarriers()) {
      // Driver supports barriers/acknowledgments
      supportedRecords.add(Barrier.class);
    }

    // Create metrics
    this.metrics = StateStoreMetrics.create(conf);

    this.monitorService = new StateStoreConnectionMonitorService(this);
    this.addService(monitorService);

    // Adding JMX interface
    try {
      StandardMBean bean =
          new StandardMBean(this.metrics, StateStoreMBean.class);
      ObjectName registeredObject =
          MBeans.register("Router", "StateStore", bean);
      LOG.info("Registered StateStoreMBean: " + registeredObject);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad StateStoreMBean setup", e);
    } catch (MetricsException e) {
      LOG.info("Failed to register statestore bean - " + e.getMessage());
    }

    // Set expirations
    MembershipState.setExpirationMs(1000 * conf.getLong(
        DFSConfigKeys.FEDERATION_STATESTORE_REGISTRATION_EXPIRATION_SEC,
        DFSConfigKeys.FEDERATION_STATESTORE_REGISTRATION_EXPIRATION_SEC_DEFAULT));

    // Set expirations
    RouterState.setExpirationMs(1000 * conf.getLong(
        DFSConfigKeys.FEDERATION_STATESTORE_ROUTER_EXPIRATION_SEC,
        DFSConfigKeys.FEDERATION_STATESTORE_ROUTER_EXPIRATION_SEC_DEFAULT));

    // Set expirations
    PathLock.setExpirationMs(1000 * conf.getLong(
        DFSConfigKeys.FEDERATION_STATESTORE_PATHLOCK_EXPIRATION_SEC,
        DFSConfigKeys.FEDERATION_STATESTORE_PATHLOCK_EXPIRATION_SEC_DEFAULT));

    Barrier.setExpirationMs(1000 * conf.getLong(
        DFSConfigKeys.FEDERATION_STATESTORE_PATHLOCK_EXPIRATION_SEC,
        DFSConfigKeys.FEDERATION_STATESTORE_PATHLOCK_EXPIRATION_SEC_DEFAULT));

    // Cache update service
    this.startCacheUpdateService();

    super.serviceInit(this.conf);
  }

  /**
   * Add an interface to the state store.
   * @param clazz Class of the interface to track.
   * @return New interface.
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private <T extends FederationStateStoreInterface> T addInterface(
      Class<T> clazz) throws InstantiationException, IllegalAccessException {

    T newInterface =
        FederationStateStoreInterface.createInterface(clazz, this.getDriver());

    // Add the record required by this interface, driver will create storage
    Class<? extends BaseRecord> clazzRecord = newInterface.requiredRecord();
    this.supportedRecords.add(clazzRecord);

    // Subscribe for cache updates
    if (newInterface instanceof FederationCachedStateStore) {
      FederationCachedStateStore cachedStore =
          (FederationCachedStateStore) newInterface;
      this.cachesToUpdate.add(cachedStore);
    }
    return newInterface;
  }

  @Override
  protected void serviceStart() throws Exception {
    loadDriver();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeDriver();
    stopCacheUpdateService();

    super.serviceStop();
  }

  /**
   * Check if the driver is ready to be used.
   *
   * @return If the driver is ready.
   */
  public boolean isDriverReady() {
    return this.driver.isDriverReady();
  }

  /**
   * Reset state store metrics.
   */
  public void resetMetrics() {
    this.metrics = StateStoreMetrics.create(conf);
  }

  /**
   * Get the metrics for the state store.
   *
   * @return Metrics for the state store.
   */
  public StateStoreMetrics getMetrics() {
    return this.metrics;
  }

  /**
   * List of records supported by this state store.
   *
   * @return List of supported record classes.
   */
  public List<Class<? extends BaseRecord>> getSupportedRecords() {
    return this.supportedRecords;
  }

  /**
   * Driver has been closed or connection has been lost.
   */
  public void driverClosed() {
    LOG.info("Connection to state store driver has been closed.");
  }

  /**
   * Manaually shuts down the driver.
   *
   * @throws Exception
   */
  @VisibleForTesting
  public void closeDriver() throws Exception {
    if (this.driver != null) {
      this.driver.close();
      driverClosed();
    }
  }

  /**
   * Load the data store driver. If successful, refresh cached data tables.
   */
  public void loadDriver() {
    synchronized (this.driver) {
      if (!isDriverReady()) {
        if (this.driver.init(conf, this)) {
          LOG.info("Connection to state store driver "
              + this.driver.getClass().getName() + " is open and ready.");
          this.refreshCaches();
        } else {
          LOG.error("Unable to initialize state store driver "
              + this.driver.getClass().getSimpleName());
        }
      }
    }
  }

  /**
   * Get the state store driver.
   *
   * @return State store driver.
   */
  public StateStoreDriver getDriver() {
    return this.driver;
  }

  /**
   * Fetch a unique identifier for this state store instance. Typically it is
   * the address of the router.
   *
   * @return Unique identifier for this store.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * Set a unique synchronization identifier for this store.
   *
   * @param id Unique identifier, typically the router's RPC address.
   */
  public void setIdentifier(String id) {
    this.identifier = id;
  }

  //
  // Cached state store data
  //
  /**
   * The last time the state store cache was fully updated.
   *
   * @return Timestamp.
   */
  public long getCacheUpdateTime() {
    return this.cacheLastUpdateTime;
  }

  /**
   * Starts the cache update service.
   */
  @VisibleForTesting
  public void startCacheUpdateService() {
    this.cacheUpdater = new StateStoreCacheUpdateService(this);
    addService(this.cacheUpdater);
  }

  /**
   * Stops the cache update service.
   */
  @VisibleForTesting
  public void stopCacheUpdateService() {
    if (this.cacheUpdater != null) {
      this.cacheUpdater.stop();
      removeService(this.cacheUpdater);
      this.cacheUpdater = null;
    }
  }

  /**
   * Register an interface for automatic periodic cache updates.
   *
   * @param client Client to the state store.
   */
  public void registerRemoteCache(FederationCachedStateStore client) {
    this.cachesToUpdate.add(client);
  }

  /**
   * Refresh the cache with information from the State Store. Called
   * periodically by the CacheUpdateService to maintain data caches and
   * versions.
   */
  public void refreshCaches() {
    boolean success = true;
    if (isDriverReady()) {
      // Remote updates
      for (FederationCachedStateStore cachedStore : this.cachesToUpdate) {
        boolean result;
        try {
          result = cachedStore.loadData();
        } catch (IOException e) {
          LOG.error("Unable to update remote cache for cache "
              + cachedStore.getClass().toString(), e);
          result = false;
        }
        if (!result) {
          success = false;
          LOG.error("Cache update failed for cache "
              + cachedStore.getClass().toString());
        }
      }
    } else {
      success = false;
      LOG.info("Skipping state store cache update, driver is not ready.");
    }
    if (success) {
      // Uses local time, not driver time.
      this.cacheLastUpdateTime = Time.now();
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getRegisteredInterface(Class<T> interfaceType) {
    for (FederationStateStoreInterface callable : this.supportedInterfaces) {
      if (interfaceType.isInstance(callable)) {
        return (T) callable;
      }
    }
    return null;
  }

  /**
   * Update the cache for a specific interface type.
   *
   * @param clazz Class of the interface.
   * @throws IOException if the cache update failed.
   */
  public boolean loadCache(Class<?> clazz) throws IOException {
    for (FederationCachedStateStore cachedStore : this.cachesToUpdate) {
      if (clazz.isInstance(cachedStore)) {
        return cachedStore.loadData();
      }
    }
    throw new IOException("Registered cache was not found for " + clazz);
  }

  /**
   * Synchronize a set of records.
   *
   * @param records
   * @param recordType
   * @return
   * @throws IOException
   */
  public <T extends BaseRecord> boolean synchronizeRecords(
      List<T> records, Class<T> clazz) throws IOException {
    this.driver.verifyDriverReady();
    if (this.driver.delete(clazz)) {
      if (this.driver.updateOrCreate(records, clazz, true, false)) {
        return true;
      }
    }
    return false;
  }
}
