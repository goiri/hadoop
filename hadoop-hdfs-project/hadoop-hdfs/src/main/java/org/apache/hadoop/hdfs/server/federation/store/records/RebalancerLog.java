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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;

/**
 * Entry to log the state of a rebalancer operation in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService
 * FederationStateStoreService}.
 */
public abstract class RebalancerLog extends BaseRecord {

  /**
   * State of the rebalancing operation for this mount point.
   */
  public enum RebalancerServiceState {
    NONE,
    INIIALIZING,
    COPYING,
    VERIFYING,
    LOCKING,
    COMPLETE;
  }

  /**
   * Status of a completed rebalancing operation.
   */
  public enum RebalancerOperationStatus {
    NONE,
    SUCCESS,
    FAILED_INITILIZATION,
    FAILED_RESERVE_MOUNT,
    FAILED_RELEASE_MOUNT,
    FAILED_INITIAL_VERIFICATION,
    FAILED_MOUNT_POINT_NOT_ELIGIBLE,
    FAILED_COPY,
    FAILED_COPY_VERIFICATION,
    FAILED_MOUNT_TABLE_UPDATE,
    FAILED_MOUNT_TABLE_CACHE_FLUSH,
    FAILED_FINAL_VERIFICATION,
    FAILED_PATH_LOCK,
    FAILED_PATH_LOCK_CACHE_FLUSH,
    FAILED_PATH_UNLOCK,
    FAILED_DELETE_SOURCE_FILES,
    FORCED_COMPLETE,
    UNKNOWN;
  }

  /**
   * Constructors.
   *
   * @throws IOException
   */
  public static RebalancerLog newInstance(String mount, String client,
      String destNameservice, String destPath) throws IOException {
    RebalancerLog record =
        FederationProtocolFactory.newInstance(RebalancerLog.class);
    record.setId(UUID.randomUUID().toString());
    record.setState(RebalancerServiceState.NONE);
    record.setOperationStatus(RebalancerOperationStatus.NONE);
    record.setClientId(client);
    record.setJobId("");
    record.setTrackingUrl("");
    record.setOperationResult("");
    record.setMount(mount);
    record.setDstPath(destPath);
    record.setNameserviceId(destNameservice);
    record.validate();
    return record;
  }

  public RebalancerLog() {
    super();
  }

  public abstract String getId();

  public abstract void setId(String value);

  public abstract RebalancerServiceState getState();

  public abstract void setState(RebalancerServiceState newState);

  public abstract RebalancerOperationStatus getOperationStatus();

  public abstract void setOperationStatus(RebalancerOperationStatus newStatus);

  public abstract String getMount();

  public abstract void setMount(String newMount);

  public abstract String getNameserviceId();

  public abstract void setNameserviceId(String id);

  public abstract String getDstPath();

  public abstract void setDstPath(String path);

  public abstract String getClientId();

  public abstract void setClientId(String client);

  public abstract long getDateReserved();

  public abstract void setDateReserved(long date);

  public abstract String getOperationResult();

  public abstract void setOperationResult(String result);

  public abstract String getJobId();

  public abstract void setJobId(String job);

  public abstract String getTrackingUrl();

  public abstract void setTrackingUrl(String url);

  public abstract float getProgress();

  public abstract void setProgress(float p);

  public abstract long getDateStateUpdated();

  public abstract void setDateStateUpdated(long date);

  @Override
  public String toString() {
    return getMount() + "->" + getNameserviceId() + ":" + getDstPath() + "-"
        + getState();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("id", getId());
    return map;
  }

  @Override
  public void validate() {
    super.validate();
    if (getNameserviceId() == null || getNameserviceId().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, no nameservice specified " + this);
    }
    if (getDstPath() == null || getDstPath().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, no destination path specified " + this);
    }
    if (!getDstPath().startsWith("/")) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, destination path must begin with a /");
    }
    if (getMount() == null || getMount().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, no mount point specified " + this);
    }
    if (!getMount().startsWith("/")) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, mount path must begin with a /");
    }
    if (getClientId() != null && getClientId().length() > 0
        && getState() == RebalancerServiceState.COMPLETE) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, client reference on completed operation "
              + this);
    }
    if (getState() == RebalancerServiceState.COMPLETE
        && getOperationStatus() == RebalancerOperationStatus.NONE) {
      throw new IllegalArgumentException(
          "Invalid rebalancer entry, operation status must be set " + this);
    }
  }

  /**
   * Check if the record is reserved.
   *
   * @return If the record is reserved.
   */
  public boolean isReserved() {
    return getClientId() != null && getClientId().length() > 0;
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }
}
