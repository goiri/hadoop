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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationCachedStateStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;

/**
 * Implementation of the {@link FederationMountTableStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationMountTableStoreImpl
    extends FederationStateStoreInterface
    implements FederationMountTableStore, FederationCachedStateStore {

  @Override
  public void init() {
  }

  /////////////////////////////////////////////////////////
  // MountTable
  /////////////////////////////////////////////////////////

  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException {
    boolean status = getDriver().updateOrCreate(
        request.getEntry(), false, true);
    AddMountTableEntryResponse response =
        FederationProtocolFactory.newInstance(
            AddMountTableEntryResponse.class);
    response.setStatus(status);
    return response;
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException {
    boolean status = getDriver().updateOrCreate(
        request.getEntry(), true, true);
    UpdateMountTableEntryResponse response =
        FederationProtocolFactory.newInstance(
            UpdateMountTableEntryResponse.class);
    response.setStatus(status);
    return response;
  }

  @Override
  public RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException {
    boolean status = (getDriver().delete(MountTable.class,
        MountTable.generatePrimaryKeys(request.getSrcPath())) == 1);
    RemoveMountTableEntryResponse response = FederationProtocolFactory
        .newInstance(RemoveMountTableEntryResponse.class);
    response.setStatus(status);
    return response;
  }

  @Override
  public GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request) throws IOException {
    GetMountTableEntriesResponse response = FederationProtocolFactory
        .newInstance(GetMountTableEntriesResponse.class);
    QueryResult<MountTable> result = getDriver().get(MountTable.class);
    if (request.getSrcPath() == null || request.getSrcPath().length() == 0) {
      List<MountTable> data = result.getRecords();
      Collections.sort(data);
      response.setEntries(data);
      response.setTimestamp(response.getTimestamp());
    } else {
      // Return entries beneath this path
      List<MountTable> filteredResults =
          new ArrayList<MountTable>();
      for (MountTable record : result.getRecords()) {
        if (record.getSourcePath().startsWith(request.getSrcPath())) {
          filteredResults.add(record);
        }
      }
      Collections.sort(filteredResults, new Comparator<MountTable>() {
        public int compare(MountTable m1, MountTable m2) {
          return m1.compareMountLength(m2);
        }
      });
      response.setEntries(filteredResults);
      response.setTimestamp(0);
    }
    return response;
  }

  @Override
  public boolean loadData() {
    return true;
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return MountTable.class;
  }
}
