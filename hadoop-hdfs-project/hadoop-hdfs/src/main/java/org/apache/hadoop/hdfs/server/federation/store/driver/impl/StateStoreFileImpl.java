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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

import com.google.common.io.Files;

/**
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver}
 * implementation based on a local file.
 */
public class StateStoreFileImpl extends StateStoreFileBaseImpl {

  private static final Log LOG = LogFactory.getLog(StateStoreFileImpl.class);

  /** Configuration keys. */
  public static final String FEDERATION_STATESTORE_FILE_DIRECTORY =
      "dfs.statestore.client.file.directory";

  /** Synchronization. */
  private static final ReadWriteLock READ_WRITE_LOCK =
      new ReentrantReadWriteLock();

  /** Root directory for the state store. */
  private String rootDirectory;


  @Override
  protected boolean exists(String path) {
    File test = new File(path);
    return test.exists();
  }

  @Override
  protected boolean mkdir(String path) {
    File dir = new File(path);
    return dir.mkdirs();
  }

  @Override
  protected String getRootDir() {
    if (this.rootDirectory == null) {
      String dir = getConf().get(FEDERATION_STATESTORE_FILE_DIRECTORY);
      if (dir == null) {
        File tempDir = Files.createTempDir();
        dir = tempDir.getAbsolutePath();
      }
      this.rootDirectory = dir;
    }
    return this.rootDirectory;
  }

  @Override
  protected <T extends BaseRecord> void lockRecordWrite(Class<T> recordClass) {
    // TODO - Synchronize via FS
    READ_WRITE_LOCK.writeLock().lock();
  }

  @Override
  protected <T extends BaseRecord> void unlockRecordWrite(
      Class<T> recordClass) {
    // TODO - Synchronize via FS
    READ_WRITE_LOCK.writeLock().unlock();
  }

  @Override
  protected <T extends BaseRecord> void lockRecordRead(Class<T> recordClass) {
    // TODO - Synchronize via FS
    READ_WRITE_LOCK.readLock().lock();
  }

  @Override
  protected <T extends BaseRecord> void unlockRecordRead(Class<T> recordClass) {
    // TODO - Synchronize via FS
    READ_WRITE_LOCK.readLock().unlock();
  }

  @Override
  protected <T extends BaseRecord> BufferedReader getReader(
      Class<T> recordClass) {
    String filename =
        getDataNameForRecord(recordClass) + "/" + getDataFileName();
    try {
      LOG.debug("Loading file: " + filename);
      File file = new File(getRootDir(), filename);
      FileReader fileReader = new FileReader(file);
      BufferedReader reader = new BufferedReader(fileReader);
      return reader;
    } catch (Exception ex) {
      LOG.error("Unable to open read stream for record - "
          + recordClass.getSimpleName(), ex);
      return null;
    }
  }

  @Override
  protected <T extends BaseRecord> BufferedWriter getWriter(
      Class<T> clazz) {
    BufferedWriter writer = null;
    String filename =
        getDataNameForRecord(clazz) + "/" + getDataFileName();
    try {
      writer = new BufferedWriter(
          new FileWriter(new File(getRootDir(), filename), false));
      return writer;
    } catch (IOException ex) {
      LOG.error("Unable to open write stream for record "
          + clazz.getSimpleName(), ex);
      return null;
    }
  }

  @Override
  public void close() throws Exception {
    setInitialized(false);
  }

  @Override
  public boolean supportsBarriers() {
    return false;
  }
}