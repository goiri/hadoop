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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.FederationRebalancerStore;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog.RebalancerOperationStatus;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog.RebalancerServiceState;
import org.apache.hadoop.util.Time;

/**
 * Implementation of the {@link FederationRebalancerStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationRebalancerStoreImpl extends FederationStateStoreInterface
    implements FederationRebalancerStore {

  private static final Log LOG = LogFactory.getLog(FederationRebalancerStoreImpl.class);

  @Override
  public void init() {
  }

  /////////////////////////////////////////////////////////
  // FederationReblanacer
  /////////////////////////////////////////////////////////

  @Override
  public String reserveRebalancerMountPoint(String mountPoint,
      String destNameservice, String destPath) throws IOException {

    // Generate unique id
    String clientId = UUID.randomUUID().toString();

    // TODO This needs to be atomic and a more efficient query
    // Get all rebalancer operations
    List<RebalancerLog> records =
        getDriver().get(RebalancerLog.class).getRecords();

    for (RebalancerLog record : records) {
      // Check for related mount points that are reserved
      if ((record.getMount().startsWith(mountPoint)
          || mountPoint.startsWith(record.getMount())
          || mountPoint.equals(record.getMount()))
          && record.getClientId() != null
          && !record.getClientId().isEmpty()) {

        LOG.info("Unable to reserve rebalancer for mount " + mountPoint
            + ", an existing record is active " + record.getMount());
        return null;
      }
    }

    // Reserve with new record, this will overwrite the last record if it exists
    RebalancerLog updatedRecord = RebalancerLog.newInstance(
        mountPoint, clientId, destNameservice, destPath);
    if (getDriver().updateOrCreate(updatedRecord, true, false)) {
      return clientId;
    } else {
      LOG.error("Unable to save rebalancer reservation to the state store");
      return null;
    }
  }

  @Override
  public boolean releaseRebalancerMountPoint(String mountPoint, String clientId,
      RebalancerOperationStatus status, String message) throws IOException {

    RebalancerLog record = getRebalancerRecord(mountPoint, clientId);
    if (record == null) {
      LOG.error("Unable to fetch a matching rebalancer item for mount point - "
          + mountPoint + " clientId - " + clientId);
      return false;
    }
    record.setClientId("");
    record.setState(RebalancerServiceState.COMPLETE);
    record.setOperationStatus(status);
    record.setOperationResult(message);
    record.setDateStateUpdated(Time.now());
    return getDriver().updateOrCreate(record, true, false);
  }

  @Override
  public boolean forceReleaseMountPoint(String mountPoint) throws IOException {

    List<RebalancerLog> records = getRebalancerOperations(mountPoint);
    List<RebalancerLog> updateRecords = new ArrayList<RebalancerLog>();
    for (RebalancerLog record : records) {
      if (record.isReserved()) {
        record.setClientId("");
        record.setOperationResult("Mount point has been force released.");
        record.setOperationStatus(RebalancerOperationStatus.FORCED_COMPLETE);
        updateRecords.add(record);
      }
    }
    if (updateRecords.size() > 0) {
      return getDriver().updateOrCreate(
          updateRecords, RebalancerLog.class, true, false);
    }
    return false;
  }

  @Override
  public boolean clearRebalancerEntries(String mountPoint) throws IOException {
    if(mountPoint == null) {
      return getDriver().delete(RebalancerLog.class);
    } else {
      Map<String, String> queryMap = new HashMap<String, String>();
      queryMap.put("mount", mountPoint);
      return (getDriver()
          .delete(RebalancerLog.class,
          queryMap) == 1);
    }
  }

  @Override
  public boolean updateRebalancerJobId(String mountPoint, String clientId,
      String jobId, String trackingUrl) throws IOException {

    RebalancerLog record = getRebalancerRecord(mountPoint, clientId);
    if (record == null) {
      LOG.error("Failed to update the job id, no record found.");
      return false;
    }
    record.setJobId(jobId);
    record.setTrackingUrl(trackingUrl);
    return getDriver().updateOrCreate(record, true, false);
  }

  @Override
  public boolean updateRebalancerState(String mountPoint, String clientId,
      RebalancerServiceState state) throws IOException {

    RebalancerLog record = getRebalancerRecord(mountPoint, clientId);
    if (record == null) {
      LOG.error("Failed to update the Rebalancer state, no record found.");
      return false;
    }
    record.setDateStateUpdated(Time.now());
    record.setState(state);
    return getDriver().updateOrCreate(record, true, false);
  }

  @Override
  public RebalancerServiceState getRebalancerState(String mountPoint,
      String clientId) throws IOException {

    RebalancerLog record = getRebalancerRecord(mountPoint, clientId);
    if (record == null) {
      LOG.error("Failed to get the Rebalancer state, no record found.");
      return RebalancerServiceState.NONE;
    } else {
      return record.getState();
    }
  }

  @Override
  public boolean updateRebalancerProgress(String mountPoint, String clientId,
      float progress) throws IOException {
    RebalancerLog record = getRebalancerRecord(mountPoint, clientId);
    if (record == null) {
      LOG.error("Failed to update the job progress, no record found.");
      return false;
    }
    record.setDateStateUpdated(Time.now());
    record.setProgress(progress);
    return getDriver().updateOrCreate(record, true, false);
  }

  @Override
  public List<RebalancerLog> getRebalancerOperations(String mountPoint)
      throws IOException {
    return getRebalancerRecords(mountPoint, null);
  }

  @Override
  public boolean updateRebalancerCheckpoint(String mountPoint, String clientId)
      throws IOException {

    // Check for an existing client
    RebalancerLog record = getRebalancerRecord(mountPoint, clientId);
    if (record == null) {
      LOG.error("getRebalancerOperations failed, no matching record was found.");
      return false;
    }
    // Issue an update to refresh dateModified
    return getDriver().updateOrCreate(record, true, false);
  }

  /**
   * Get a particular rebalancer record.
   * @param mountPoint Mount point currently reserved by client for operation.
   * @param clientId Client ID for reservation.
   * @return Rebalancer record or null if no matching the query are found.
   * @throws IOException If the state store cannot be queried.
   */
  private RebalancerLog getRebalancerRecord(
      String mountPoint, String clientId) throws IOException {

    Map<String, String> queryMap = new HashMap<String, String>();
    if (mountPoint != null) {
      queryMap.put("mount", mountPoint);
    }
    if (clientId != null) {
      queryMap.put("clientId", clientId);
    }
    if (queryMap.isEmpty()) {
      LOG.error("No filter specificed for getRebalancerRecord");
      return null;
    }

    // Check for an existing client
    RebalancerLog record = getDriver().get(RebalancerLog.class, queryMap);
    return record;
  }

  /**
   * Get multiple rebalancer records.
   * @param mountPoint Mount point currently reserved by client for operation.
   * @param clientId Client ID for reservation.
   * @return Rebalancer records or null if no matching the query are found.
   * @throws IOException If the state store cannot be queried.
   */
  private List<RebalancerLog> getRebalancerRecords(
      String mountPoint, String clientId) throws IOException {

    Map<String, String> queryMap = new HashMap<String, String>();
    if (mountPoint != null) {
      queryMap.put("mount", mountPoint);
    }
    if (clientId != null) {
      queryMap.put("clientId", clientId);
    }

    List<RebalancerLog> ret = null;
    if (queryMap.isEmpty()) {
      QueryResult<RebalancerLog> result = getDriver().get(RebalancerLog.class);
      ret = result.getRecords();
    } else {
      ret = getDriver().getMultiple(RebalancerLog.class, queryMap);
    }
    if (ret != null) {
      Collections.sort(ret);
    }
    return ret;
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return RebalancerLog.class;
  }
}
