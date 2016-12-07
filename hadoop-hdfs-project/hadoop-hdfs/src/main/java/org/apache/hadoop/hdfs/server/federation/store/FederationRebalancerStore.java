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
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog.RebalancerOperationStatus;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog.RebalancerServiceState;

/**
 * Management API for {@link RebalancerLog} records in the state store. Accesses
 * the data store via the {@link org.apache.hadoop.hdfs.server.federation.store.
 * driver.StateStoreDriver StateStoreDriver} interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationRebalancerStore {

  /**
   * Reserves a mount table path for use by the rebalancer.
   *
   * @param mountPoint The mount point that is being rebalanced.
   * @param destNameservice The nameservice ID to copy to.
   * @param destPath The local path to copy to.
   * @return A client ID to reference the reservation or null if unavailable.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  String reserveRebalancerMountPoint(String mountPoint, String destNameservice,
      String destPath) throws IOException;

  /**
   * Releases a previously reserved mount point for rebalancing. Fails if the
   * current client did not reserve it.
   *
   * @param mountPoint Mount point to be released.
   * @param clientId Client holding the lease on this mount point.
   * @param status Final status of the operation.
   * @param message Detailed description of status.
   * @return true if successful.
   * @throws IOException If the data store cannot be queried.
   */
  boolean releaseRebalancerMountPoint(String mountPoint, String clientId,
      RebalancerOperationStatus status, String message) throws IOException;

  /**
   * Releases a mount point without altering the state. A reserved mount point
   * may indicate a pending operation and this API should be used with caution.
   *
   * @param mountPoint Mount point to release, all entries for this mount will
   *          be released.
   * @return True if successful.
   * @throws IOException If the data store cannot be queried.
   */
  boolean forceReleaseMountPoint(String mountPoint) throws IOException;

  /**
   * Returns the state of the specified operation or NONE if the operation does
   * not exist.
   *
   * @param mountPoint mount point currently reserved by client for operation.
   * @param clientId client ID for reservation returned by
   *          reserveRebalancerMountPoint.
   * @return The state of the operation.
   * @throws IOException If the data store cannot be queried.
   */
  RebalancerServiceState getRebalancerState(String mountPoint, String clientId)
      throws IOException;

  /**
   * Returns all operations that match the mount point.
   *
   * @param mountPoint Mount point to filter operations or null for all
   *          operations.
   * @return List of operations for the mount point or zero-length list if none
   *         exists.
   * @throws IOException If the data store cannot be queried.
   */
  Collection<RebalancerLog> getRebalancerOperations(String mountPoint)
      throws IOException;

  /**
   * Updates the state of an operation on a mount point.
   *
   * @param mountPoint Mount point currently reserved by client for operation.
   * @param clientId Client ID for reservation returned by
   *          reserveRebalancerMountPoint.
   * @param state
   * @return True if update was successfully committed to the data store.
   * @throws StateStoreUnavailableException Throws exception if the data store
   *           is not initialized.
   * @throws IOException If the data store cannot be queried.
   */
  boolean updateRebalancerState(String mountPoint, String clientId,
      RebalancerServiceState state) throws IOException;

  /**
   * Adds a job ID to the task tracker.
   *
   * @param mountPoint mount point currently reserved by client for operation.
   * @param clientId client ID for reservation returned by
   *          reserveRebalancerMountPoint
   * @param jobId Job identifier.
   * @param trackingUrl URL to track the job identifier.
   * @return True if update was successfully committed to the data store.
   * @throws IOException If the data store cannot be queried.
   */
  boolean updateRebalancerJobId(String mountPoint, String clientId,
      String jobId, String trackingUrl) throws IOException;

  /**
   * Update the progress of the rebalancer.
   *
   * @param mountPoint Mount point currently reserved by client for operation.
   * @param clientId Client ID for reservation.
   * @param progress Progress of the DistCp job.
   * @return True if update was successfully committed to the data store.
   * @throws IOException If the data store cannot be queried.
   */
  boolean updateRebalancerProgress(String mountPoint, String clientId,
      float progress) throws IOException;

  /**
   * Removes all rebalancer history/state information from the state store.
   *
   * @param mountPoint mount point of entries to delete, or null to delete all
   *          entires.
   * @return True if removal was successfully commited to the data store.
   * @throws IOException If the data store cannot be queried.
   */
  boolean clearRebalancerEntries(String mountPoint) throws IOException;

  /**
   * Updates the checkpoint date to the current store date. Used to validate
   * rebalancer operation is still running. Operation must be currently reserved
   * for given mount point and client.
   *
   * @param mountPoint mount point currently reserved by client for operation.
   * @param clientId client ID for reservation returned by
   *          reserveRebalancerMountPoint.
   * @return True if update was successfully committed to the data store.
   * @throws IOException If the data store cannot be queried.
   */
  boolean updateRebalancerCheckpoint(String mountPoint, String clientId)
      throws IOException;
}
