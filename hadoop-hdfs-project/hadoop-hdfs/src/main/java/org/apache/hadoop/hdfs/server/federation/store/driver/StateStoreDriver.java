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

package org.apache.hadoop.hdfs.server.federation.store.driver;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.records.Barrier;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.Time;

/**
 * Driver class for an implementation of a {@link FederationStateStoreService}
 * provider. Driver implementations will extend this class and implement some of
 * the default methods.
 * <p>
 * All drivers must implement these methods:
 * <ul>
 * <li>initDriver
 * <li>initRecordStorage
 * <li>close
 * <li>get
 * <li>updateOrCreate
 * <li>delete
 * </ul>
 * Drivers may optionally override additional routines for performance
 * optimization, such as custom get/update/insert/delete queries, depending on
 * the capabilities of the data store.
 * <p>
 * This file contains default implementations for the optional functions. These
 * implementations use an uncached read/write all algorithm for all changes. In
 * most cases it is recommended to override the optional functions.
 */
public abstract class StateStoreDriver {

  private static final Log LOG = LogFactory.getLog(StateStoreDriver.class);

  /** State Store configuration. */
  private Configuration conf;

  /** State Store manager that can be used by the implementations. */
  private FederationStateStoreService manager;

  /** Configuration keys for the records. */
  private static final String FEDERATION_STATESTORE_RECORD_BASE =
      "dfs.federation.statestore.client.data.record.";


  /**
   * Get the State Store configuration.
   *
   * @return Configuration for the State Store.
   */
  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * Get the State Store service manager.
   *
   * @return State Store service manager singleton instance.
   */
  public FederationStateStoreService getManager() {
    return this.manager;
  }

  /**
   * Get the metrics for the State Store.
   *
   * @return Metrics for the State Store.
   */
  protected StateStoreMetrics getMetrics() {
    return this.manager.getMetrics();
  }

  /**
   * Initialize the state store connection.
   *
   * @param conf Configuration.
   * @param manager instance implementing the StateStore interface.
   * @return If initialized and ready, false if failed to initialize driver.
   */
  public boolean init(
      Configuration config, FederationStateStoreService service) {
    this.conf = config;
    this.manager = service;

    boolean result = initDriver();
    if (!result) {
      LOG.error("Unable to intialize State Store driver for "
          + this.getClass().getSimpleName());
      return false;
    }

    for (Class<? extends BaseRecord> cls : getSupportedRecords()) {
      String recordString = getDataNameForRecord(cls);
      if (!initRecordStorage(recordString, cls)) {
        LOG.error("Unable to intialize State Store storage for "
            + cls.getSimpleName());
        return false;
      }
    }
    return true;
  }

  /**
   * Prepare the driver to access data storage.
   *
   * @return True if the driver was successfully initialized. If false is
   *         returned, the state store will periodically attempt to
   *         re-initialize the driver and the router will remain in safe mode
   *         until the driver is initialized.
   */
  public abstract boolean initDriver();

  /**
   * Initialize storage for a single record class.
   *
   * @param name String reference of the record class to initialize, used to
   *             construct paths and file names for the record. Determined by
   *             configuration settings for the specific driver.
   * @param clazz Record type corresponding to the provided name.
   * @return True if successful, false otherwise.
   */
  public abstract <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> clazz);

  /**
   * Check if the driver is currently running and the data store connection is
   * valid.
   *
   * @return True if the driver is initialized and the data store is ready.
   */
  public abstract boolean isDriverReady();

  /**
   * Check if the driver is ready to be used and throw an exception otherwise.
   *
   * @throws StateStoreUnavailableException If the driver is not ready.
   */
  public void verifyDriverReady() throws StateStoreUnavailableException {
    if (!isDriverReady()) {
      String driverName = this.getClass().getName();
      String hostname = "Unknown";
      try {
        hostname = InetAddress.getLocalHost().getHostName();
      } catch (Exception e) {
        LOG.error("Cannot get local address", e);
      }
      throw new StateStoreUnavailableException("State Store driver "
          + driverName + " in " + hostname + " is not ready.");
    }
  }

  /**
   * Close the State Store driver connection.
   */
  public abstract void close() throws Exception;

  /**
   * Connection to the state store has been closed or lost.
   */
  protected void closeConnection() {
    this.manager.driverClosed();
  }

  /**
   * Get a list of supported data records.
   * @return List of supported records.
   */
  protected Collection<Class<? extends BaseRecord>> getSupportedRecords() {
    return this.manager.getSupportedRecords();
  }

  /**
   * Gets a unique identifier for the running task/process. Typically the
   * router address.
   *
   * @return Unique identifier for the running task.
   */
  public String getIdentifier() {
    return this.manager.getIdentifier();
  }

  /**
   * Returns the current time synchronization from the underlying store.
   * Override for stores that supply a current date. The data store driver is
   * responsible for maintaining the official synchronization time/date for all
   * distributed components.
   *
   * @return Current time stamp, used for all synchronization dates.
   */
  public long getTime() {
    return Time.now();
  }

  /**
   * Maps an record class to the appropriate file/reference string.
   *
   * @param clazz Class of the data record
   * @return File based name of the specified record.
   */
  protected String getDataNameForRecord(Class<?> clazz) {
    String keyName = FEDERATION_STATESTORE_RECORD_BASE
        + this.getClass().getSimpleName().toLowerCase() + "."
        + clazz.getSimpleName().toLowerCase();
    return this.conf.get(keyName, clazz.getSimpleName());
  }

  /**
   * Check if the driver supports native barriers or requires using a custom
   * barrier table.
   *
   * @return If the driver supports barriers.
   */
  public abstract boolean supportsBarriers();

  /**
   * Get all records of the requested record class from the data store. To use
   * the default implementations in this class, getAll must return new instances
   * of the records on each call. It is recommended to override the default
   * implementations for better performance.
   *
   * @param clazz Class of record to fetch.
   * @return List of all records that match the clazz.
   * @throws IOException Throws exception if unable to query the data store
   */
  public abstract <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException;

  /**
   * Get a single record from the store that matches the query.
   *
   * @param clazz Class of record to fetch.
   * @param query Map of field names and objects to filter results.
   * @return A single record matching the query. Null if there are no matching
   *         records or more than one matching record in the store.
   * @throws IOException If multiple records match or if the data store cannot
   *           be queried.
   */
  public <T extends BaseRecord> T get(
      Class<T> clazz, Map<String, String> query) throws IOException {
    List<T> records = getMultiple(clazz, query);
    if (records.size() > 1) {
      throw new IOException("Found more than one object in collection");
    } else if (records.size() == 1) {
      return records.get(0);
    } else {
      return null;
    }
  }

  /**
   * Get multiple records from the store that match a query.
   *
   * @param clazz Class of record to fetch.
   * @param query Map of field names and objects to filter results.
   * @return Records of type clazz that match the query or empty list if none
   *         are found.
   * @throws IOException Throws exception if unable to query the data store.
   */
  public <T extends BaseRecord> List<T> getMultiple(
      Class<T> clazz, Map<String, String> query) throws IOException  {
    QueryResult<T> result = get(clazz);
    List<T> records = result.getRecords();
    List<T> ret = FederationStateStoreUtils.filterMultiple(query, records);
    if (ret == null) {
      throw new IOException("Unable to fetch records from the store");
    }
    return ret;
  }

  /**
   * Creates a single record. Optionally updates an existing record with same
   * primary key.
   *
   * @param record The record to insert or update.
   * @param allowUpdate True if update of exiting record is allowed.
   * @param errorIfExists True if an error should be returned when inserting
   *          an existing record. Only used if allowUpdate = false.
   * @return True if the operation was successful.
   *
   * @throws IOException Throws exception if unable to query the data store.
   */
  public <T extends BaseRecord> boolean updateOrCreate(
      T record, boolean allowUpdate, boolean errorIfExists) throws IOException {
    List<T> singletonList = new ArrayList<T>();
    singletonList.add(record);
    Class<T> clazz = FederationProtocolFactory.getReferenceClass(record);
    return updateOrCreate(singletonList, clazz, allowUpdate, errorIfExists);
  }

  /**
   * Creates multiple records. Optionally updates existing records that have
   * the same primary key.
   *
   * @param records List of data records to update or create. All records must
   *                be of class clazz.
   * @param clazz Record class of records.
   * @param allowUpdate True if update of exiting record is allowed.
   * @param errorIfExists True if an error should be returned when inserting
   *          an existing record. Only used if allowUpdate = false.
   * @return true if all operations were successful.
   *
   * @throws IOException Throws exception if unable to query the data store.
   */
  public abstract <T extends BaseRecord> boolean updateOrCreate(
      List<T> records, Class<T> clazz, boolean allowUpdate,
      boolean errorIfExists) throws IOException;

  /**
   * Acknowledges receipt and/or processing of a record. Used for barrier-like
   * synchronization. Override if the data back-end supports barrier
   * synchronization, by default uses a separate record table.
   *
   * @param record Record record to acknowledge
   * @param identifier Unique primary key or identifier for the record.
   * @return True if successful, false if the record has already been
   *         acknowledged.
   * @throws IOException Throws if unable to commit to the data store.
   */
  public <T extends BaseRecord> boolean acknowledgeRecord(
      T record, String identifier) throws IOException {
    String uniqueId = record.getUniqueIdentifier();
    Barrier barrier = Barrier.newInstance(uniqueId, identifier);
    // Insert only
    return this.updateOrCreate(barrier, false, false);
  }

  /**
   * Wait for a number of services to acknowledge an record. Override if the
   * data back-end supports barrier synchronization, by default uses a separate
   * record table.
   *
   * @param record Record record that is being acknowledged.
   * @param identifiers List of identifiers that must acknowledge the record
   * @param timeoutMs number of ms to wait before giving up.
   *
   * @throws IOException Throws if unable to query to the data store.
   * @throws TimeoutException if the desired number of acknowledgments is not
   *           received within timeoutMs.
   */
  public <T extends BaseRecord> void waitForAcknowledgement(
      T record, List<String> clientIds, long timeoutMs)
          throws IOException, TimeoutException {

    long startTime = Time.monotonicNow();
    while ((Time.monotonicNow() - startTime) < timeoutMs) {
      Map<String, String> query = new HashMap<String, String>();
      query.put("recordIdentifier", record.getUniqueIdentifier());
      List<Barrier> barriers = getMultiple(Barrier.class, query);
      for (Barrier barrier : barriers) {
        String clientId = barrier.getClientIdentifier();
        if (clientIds.contains(clientId)) {
          clientIds.remove(clientId);
        }
      }
      if (clientIds.isEmpty()) {
        // All identifiers are present
        return;
      }
      try {
        // Wait
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException("Unable to query for record " + record);
      }
    }
    throw new TimeoutException("Unable to locate barrier for record " + record
        + " in " + timeoutMs + "ms");
  }

  /**
   * Delete a single record.
   *
   * @param record Record to be deleted.
   * @return true If the record was successfully deleted.
   * @throws IOException Throws exception if unable to query the data store.
   */
  public boolean delete(BaseRecord record) throws IOException {
    SortedMap<String, String> primaryKeys = record.getPrimaryKeys();
    Class<? extends BaseRecord> clazz =
        FederationProtocolFactory.getReferenceClass(record);
    return delete(clazz, primaryKeys) == 1;
  }

  /**
   * Delete multiple records of a specific class that match a query. Requires
   * the getAll implementation to fetch fresh records on each call.
   *
   * @param clazz Class of record to delete.
   * @param filter matching filter to delete.
   * @return The number of records deleted.
   * @throws IOException Throws exception if unable to query the data store.
   */
  public abstract <T extends BaseRecord> int delete(
      Class<T> clazz, Map<String, String> filter) throws IOException;

  /**
   * Deletes all records of this class from the store.
   *
   * @param clazz Class of records to delete.
   * @return True if successful.
   * @throws IOException Throws exception if unable to query the data store.
   */
  public abstract <T extends BaseRecord> boolean delete(Class<T> clazz)
      throws IOException;

  /**
   * Creates a record from a test string.
   *
   * @param data Serialized text of the record.
   * @param clazz Record class.
   * @param includeDates If dateModified and dateCreated are serialized.
   * @return The created record
   * @throws IOException
   */
  protected static <T extends BaseRecord> T createRecord(
      String data, Class<T> clazz, boolean includeDates) throws IOException {
    T record = FederationProtocolFactory.newInstance(clazz);
    record.deserialize(data);
    return record;
  }

}
