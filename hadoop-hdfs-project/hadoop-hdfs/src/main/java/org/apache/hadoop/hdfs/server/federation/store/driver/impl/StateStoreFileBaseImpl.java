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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.Time;

/**
 * {@link StateStoreDriver} implementation based on a local file.
 */
public abstract class StateStoreFileBaseImpl extends StateStoreDriver {

  private static final Log LOG = LogFactory.getLog(StateStoreFileBaseImpl.class);

  /** If it is initialized. */
  private boolean initialized = false;

  /** Name of the file containing the data. */
  private static final String DATA_FILE_NAME = "records.data";


  /**
   * Lock reading records.
   *
   * @param clazz Class of the record.
   */
  protected abstract <T extends BaseRecord> void lockRecordRead(Class<T> clazz);

  /**
   * Unlock reading records.
   *
   * @param clazz Class of the record.
   */
  protected abstract <T extends BaseRecord> void unlockRecordRead(
      Class<T> clazz);

  /**
   * Lock writing records.
   *
   * @param clazz Class of the record.
   */
  protected abstract <T extends BaseRecord> void lockRecordWrite(
      Class<T> clazz);

  /**
   * Unlock writing records.
   *
   * @param clazz Class of the record.
   */
  protected abstract <T extends BaseRecord> void unlockRecordWrite(
      Class<T> clazz);

  /**
   * Get the reader for the file system.
   *
   * @param clazz Class of the record.
   */
  protected abstract <T extends BaseRecord> BufferedReader getReader(
      Class<T> clazz);

  /**
   * Get the writer for the file system.
   *
   * @param clazz Class of the record.
   */
  protected abstract <T extends BaseRecord> BufferedWriter getWriter(
      Class<T> clazz);

  /**
   * Check if a path exists.
   *
   * @param path Path to check.
   * @return If the path exists.
   */
  protected abstract boolean exists(String path);

  /**
   * Make a directory.
   *
   * @param path Path of the directory to create.
   * @return If the directory was created.
   */
  protected abstract boolean mkdir(String path);

  /**
   * Get root directory.
   *
   * @return Root directory.
   */
  protected abstract String getRootDir();

  /**
   * Set the driver as initialized.
   *
   * @param ini If the driver is initialized.
   */
  public void setInitialized(boolean ini) {
    this.initialized = ini;
  }

  @Override
  public boolean initDriver() {
    String rootDirectory = getRootDir();
    try {
      if (rootDirectory == null) {
        LOG.error("Invalid root directory, unable to initialize driver.");
        return false;
      }

      // Check root path
      if (!exists(rootDirectory)) {
        if (!mkdir(rootDirectory)) {
          LOG.error(
              "Unable to create state store root directory - " + rootDirectory);
          return false;
        }
      }
    } catch (Exception ex) {
      LOG.info("Cannot initialize filesystem using root directory "
          + rootDirectory, ex);
      return false;
    }
    setInitialized(true);
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> recordClass) {

    String dataDirPath = getRootDir() + "/" + className;
    try {
      // Create data directories for files
      if (!exists(dataDirPath)) {
        LOG.info(dataDirPath + " data directory doesn't exist, creating it");
        if (!mkdir(dataDirPath)) {
          LOG.error("Cannot create data directory " + dataDirPath);
          return false;
        }
        String dataFilePath = dataDirPath + "/" + DATA_FILE_NAME;
        if (!exists(dataFilePath)) {
          // Create empty file
          List<T> emtpyList = new ArrayList<T>();
          if(!writeAll(emtpyList, recordClass)) {
            LOG.error("Cannot create data file " + dataFilePath);
            return false;
          }
        }
      }
    } catch (Exception ex) {
      LOG.error("Cannot create data directory " + dataDirPath, ex);
      return false;
    }
    return true;
  }

  /**
   * Read all lines from a file and deserialize into the desired record type.
   *
   * @param reader Open handle for the file.
   * @param recordClass Record class to create.
   * @param includeDates True if dateModified/dateCreated are serialized.
   * @return List of records.
   * @throws IOException
   */
  private <T extends BaseRecord> List<T> getAllFile(BufferedReader reader,
      Class<T> clazz, boolean includeDates) throws IOException {

    List<T> ret = new ArrayList<T>();
    String line;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("#") && line.length() > 0) {
        try {
          T record = createRecord(line, clazz, includeDates);
          ret.add(record);
        } catch (Exception ex) {
          LOG.error("Cannot parse line in data source file: " + line, ex);
        }
      }
    }
    return ret;
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    verifyDriverReady();
    long start = Time.monotonicNow();
    BufferedReader reader = null;
    lockRecordRead(clazz);
    try {
      reader = getReader(clazz);
      List<T> data = getAllFile(reader, clazz, true);
      getMetrics().addRead(Time.monotonicNow() - start);
      return new QueryResult<T>(data, getTime());
    } catch (Exception ex) {
      LOG.error("Unable to fetch records " + clazz.getSimpleName());
      getMetrics().addFailure(Time.monotonicNow() - start);
      throw new IOException(
          "Unable to read from data store " + ex.getMessage());
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Failed closing file", e);
        }
      }
      unlockRecordRead(clazz);
    }
  }

  /**
   * Overwrite the existing data with a new data set.
   *
   * @param list List of records to write.
   * @param writer BufferedWriter stream to write to.
   * @return If the records were succesfully written.
   */
  private <T extends BaseRecord> boolean writeAllFile(
      Collection<T> records, BufferedWriter writer) {

    try {
      for (BaseRecord record : records) {
        try {
          String data = record.serialize();
          writer.write(data);
          writer.newLine();
        } catch (IllegalArgumentException ex) {
          LOG.error("Unable to write record " + record + " to file", ex);
        }
      }
      writer.flush();
      return true;
    } catch (IOException e) {
      LOG.error("Unable to commit records to file.", e);
      return false;
    }
  }

  /**
   * Overwrite the existing data with a new data set. Replaces all records in
   * the data store for this record class. If all records in the data store are
   * not successfully committed, this function must return false and leave the
   * data store unchanged.
   *
   * @param records List of records to write. All records must be of type
   *                recordClass.
   * @param recordClass Class of record to replace.
   * @return true if all operations were successful, false otherwise.
   * @throws StateStoreUnavailableException
   */
  public <T extends BaseRecord> boolean writeAll(Collection<T> records,
      Class<T> recordClass) throws StateStoreUnavailableException {
    verifyDriverReady();
    lockRecordWrite(recordClass);
    BufferedWriter writer = null;
    try {
      writer = getWriter(recordClass);
      return writeAllFile(records, writer);
    } catch (Exception e) {
      LOG.error(
          "Unable to add records to file for " + recordClass.getSimpleName(),
          e);
      return false;
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOG.error(
              "Unable to close writer for - " + recordClass.getSimpleName(), e);
        }
      }
      unlockRecordWrite(recordClass);
    }
  }

  /**
   * Get the data file name.
   *
   * @return Data file name.
   */
  protected String getDataFileName() {
    return DATA_FILE_NAME;
  }

  @Override
  public boolean supportsBarriers() {
    return false;
  }

  @Override
  public boolean isDriverReady() {
    return this.initialized;
  }

  @Override
  public <T extends BaseRecord> boolean updateOrCreate(List<T> records,
      Class<T> clazz, boolean allowUpdate, boolean errorIfExists)
      throws StateStoreUnavailableException {
    verifyDriverReady();
    if (records.isEmpty()) {
      return true;
    }
    long start = Time.monotonicNow();
    QueryResult<T> result;
    try {
      result = get(clazz);
    } catch (IOException e) {
      getMetrics().addFailure(Time.monotonicNow() - start);
      return false;
    }
    HashMap<Object, T> writeList = new HashMap<Object, T>();

    // Write all of the existing records
    for (T existingRecord : result.getRecords()) {
      writeList.put(existingRecord.getPrimaryKey(), existingRecord);
    }

    // Add inserts and updates, overwrite any existing values
    for (T updatedRecord : records) {
      try {
        if (!clazz.isInstance(updatedRecord)) {
          LOG.error("Updated record " + updatedRecord + " is not of type "
              + clazz.toString());
          getMetrics().addFailure(Time.monotonicNow() - start);
          return false;
        }
        updatedRecord.validate();
        if (writeList.containsKey(updatedRecord.getPrimaryKey())
            && allowUpdate) {
          // Update
          writeList.put(updatedRecord.getPrimaryKey(), updatedRecord);
          // Update the mod time stamp. Many backends will use their
          // own timestamp for the mod time.
          updatedRecord.setDateModified(this.getTime());
        } else if (!writeList.containsKey(updatedRecord.getPrimaryKey())) {
          // Insert
          // Create/Mod timestamps are already initialized
          writeList.put(updatedRecord.getPrimaryKey(), updatedRecord);
        } else if (errorIfExists) {
          LOG.error("Attempt to insert record " + updatedRecord
              + " that already exists");
          getMetrics().addFailure(Time.monotonicNow() - start);
          return false;
        }
      } catch (IllegalArgumentException ex) {
        LOG.error("Unable to write invalid record to state store", ex);
        getMetrics().addFailure(Time.monotonicNow() - start);
        return false;
      }
    }

    // Write all
    boolean status = writeAll(writeList.values(), clazz);
    if (status) {
      getMetrics().addWrite(Time.monotonicNow() - start);
    } else {
      getMetrics().addFailure(Time.monotonicNow() - start);
    }
    return status;
  }

  @Override
  public <T extends BaseRecord> int delete(Class<T> clazz,
      Map<String, String> filter) throws StateStoreUnavailableException {
    verifyDriverReady();
    LOG.info("deleteMultiple " + clazz + " " + filter);
    if (filter.isEmpty()) {
      return 0;
    }
    long start = Time.monotonicNow();
    int deleted = 0;
    // Get the current records
    try {
      QueryResult<T> existingRecords = get(clazz);
      // Write all of the existing records except those to be deleted
      List<? extends BaseRecord> recordsToDelete =
          FederationStateStoreUtils.filterMultiple(
              filter, existingRecords.getRecords());
      if (recordsToDelete == null) {
        getMetrics().addFailure(Time.monotonicNow() - start);
        throw new IOException("Unable to delete record " + clazz
            + " filter " + filter.toString());
      }
      deleted = recordsToDelete.size();
      existingRecords.getRecords().removeAll(recordsToDelete);
      if (!writeAll(existingRecords.getRecords(), clazz)) {
        getMetrics().addFailure(Time.monotonicNow() - start);
        throw new IOException("Unable to delete record " + clazz
            + " filter " + filter.toString());
      }
    } catch (IOException e) {
      LOG.error("Unable to delete records " + clazz + " filter "
          + filter.toString(), e);
      getMetrics().addFailure(Time.monotonicNow() - start);
    }

    if (deleted > 0) {
      getMetrics().addDelete(Time.monotonicNow() - start);
    }
    return deleted;
  }

  @Override
  public <T extends BaseRecord> boolean delete(Class<T> clazz)
      throws StateStoreUnavailableException {
    verifyDriverReady();
    LOG.info("deleteAll " + clazz);
    long start = Time.monotonicNow();
    List<T> emptyList = new ArrayList<T>();
    boolean status = writeAll(emptyList, clazz);
    long time = Time.monotonicNow() - start;
    if (status) {
      getMetrics().addDelete(time);
    } else {
      getMetrics().addFailure(time);
    }
    return status;
  }
}