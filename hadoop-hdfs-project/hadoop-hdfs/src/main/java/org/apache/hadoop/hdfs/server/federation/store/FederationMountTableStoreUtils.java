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
import java.util.List;

import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;

/**
 * Utility wrapper around HDFS Federation state store API requests.
 */
public final class FederationMountTableStoreUtils {

  private FederationMountTableStoreUtils() {
    // Utility class
  }

  /**
   * Adds a new mount table record to the state store.
   *
   * @param store FederationMountTableStore instance to commit the data.
   * @param record A fully populated data record.
   *
   * @return True if successful, false if failure.
   *
   * @throws IOException If the state store could not be accessed.
   */
  public static boolean addMountTableEntry(FederationMountTableStore store,
      MountTable record) throws IOException {
    AddMountTableEntryRequest request = FederationProtocolFactory.newInstance(
        AddMountTableEntryRequest.class);
    request.setEntry(record);
    AddMountTableEntryResponse response = store.addMountTableEntry(request);
    return response.getStatus();
  }

  /**
   * Updated an existing mount table record in the state store.
   *
   * @param store FederationMountTableStore instance to commit the data.
   * @param record A fully populated data record that has the same mount point
   *          as an existing record.
   *
   * @return True if successful, false if failure.
   *
   * @throws IOException If the state store could not be accessed.
   */
  public static boolean updateMountTableEntry(FederationMountTableStore store,
      MountTable record) throws IOException {
    UpdateMountTableEntryRequest request = FederationProtocolFactory
        .newInstance(UpdateMountTableEntryRequest.class);
    request.setEntry(record);
    UpdateMountTableEntryResponse response =
        store.updateMountTableEntry(request);
    return response.getStatus();
  }

  /**
   * Removes an existing mount table record in the state store.
   *
   * @param store FederationMountTableStore instance to commit the data.
   * @param mount The mount point of the record to remove.
   *
   * @return True if successful, false if failure.
   *
   * @throws IOException If the state store could not be accessed.
   */
  public static boolean removeMountTableEntry(FederationMountTableStore store,
      String mount) throws IOException {
    RemoveMountTableEntryRequest request = FederationProtocolFactory
        .newInstance(RemoveMountTableEntryRequest.class);
    request.setSrcPath(mount);
    RemoveMountTableEntryResponse response =
        store.removeMountTableEntry(request);
    return response.getStatus();
  }

  /**
   * Removes an existing mount table record in the state store.
   *
   * @param store FederationMountTableStore instance to commit the data.
   * @param mount The mount point of the record to remove.
   *
   * @return The matching record if found, null if it is not found.
   *
   * @throws IOException If the state store could not be accessed.
   */
  public static MountTable getMountTableEntry(
      FederationMountTableStore store, String mount) throws IOException {
    GetMountTableEntriesRequest request =
        FederationProtocolFactory.newInstance(
            GetMountTableEntriesRequest.class);
    request.setSrcPath(mount);
    GetMountTableEntriesResponse response = store.getMountTableEntries(request);
    List<MountTable> results = response.getEntries();
    if (results.size() > 0) {
      // First result is sorted to have the shortest mount string length
      return results.get(0);
    }
    return null;
  }

  /**
   * Fetch all mount table records beneath a root path.
   *
   * @param store FederationMountTableStore instance to commit the data.
   * @param mount The root search path, enter "/" to return all mount table
   *          records.
   *
   * @return A list of all mount table records found below the root mount.
   *
   * @throws IOException If the state store could not be accessed.
   */
  public static QueryResult<MountTable> getMountTableRecords(
      FederationMountTableStore store, String mount) throws IOException {
    if (mount == null) {
      throw new IOException("Please specify a root search path");
    }
    GetMountTableEntriesRequest request =
        FederationProtocolFactory.newInstance(
            GetMountTableEntriesRequest.class);
    request.setSrcPath(mount);
    GetMountTableEntriesResponse response =
        store.getMountTableEntries(request);
    List<MountTable> records = response.getEntries();
    long timestamp = response.getTimestamp();
    return new QueryResult<MountTable>(records, timestamp);
  }
}
