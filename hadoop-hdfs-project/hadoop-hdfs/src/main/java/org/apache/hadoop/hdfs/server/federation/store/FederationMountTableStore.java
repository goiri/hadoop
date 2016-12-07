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
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;

/**
 * Management API for the HDFS mount table information stored in
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.MountTable
 * MountTable} records. The mount table contains entries that map a particular
 * global namespace path one or more HDFS nameservices (NN) + target path. It is
 * possible to map mount locations for root folders, directories or individual
 * files.
 * <p>
 * Once fetched from the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver}, MountTable records are cached in a tree for faster access.
 * Each path in the global namespace is mapped to a nameserivce ID and local
 * path upon request. The cache is periodically updated by the @{link
 * StateStoreCacheUpdateService}.
 * <p>
 * TODO add support for some regular expressions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationMountTableStore {

  /**
   * Add an entry to the mount table.
   *
   * @param request Fully populated request object.
   * @return True if the mount table entry was successfully committed to the
   *         data store.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException;

  /**
   * Updates an existing entry in the mount table.
   *
   * @param request Fully populated request object.
   * @return True if the mount table entry was successfully committed to the
   *         data store.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException;

  /**
   * Remove an entry from the mount table.
   *
   * @param request Fully populated request object.
   * @return True the mount table entry was removed from the data store.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException;

  /**
   * List all mount table entries present at or below the path. Fetches from the
   * state store.
   *
   * @param request Fully populated request object.
   *
   * @return List of all mount table entries under the path. Zero-length list if
   *         none are found.
   * @throws IOException Throws exception if the data store cannot be queried.
   */
  GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request) throws IOException;

}