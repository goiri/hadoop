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
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationResponse;

/**
 * Management API for NameNode registrations stored in
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.MembershipState
 * MembershipState} records. The {@link org.apache.hadoop.hdfs.server.
 * federation.router.RouterHeartbeatService RouterHeartbeatService} periodically
 * polls each NN to update the NameNode metadata(addresses, operational) and HA
 * state(active, standby). Each NameNode may be polled by multiple
 * {@link org.apache.hadoop.hdfs.server.federation.router.Router Router}
 * instances.
 * <p>
 * Once fetched from the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver}, NameNode registrations are cached until the next query.
 * The fetched registration data is aggregated using a quorum to determine the
 * best/most accurate state for each NameNode. The cache is periodically updated
 * by the @{link StateStoreCacheUpdateService}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationMembershipStateStore {

  /**
   * Queries for a single cached registration entry matching the given
   * parameters. Possible keys are the names of data structure elements Possible
   * values are matching SQL "LIKE" targets.
   *
   * @param request Fully populated GetNamenodeRegistrationsRequest request.
   * @return Single matching FederationMembershipStateEntry or null if not found
   *         or more than one entry matches.
   * @throws StateStoreUnavailableException Throws exception if the data store
   *           is not initialized.
   * @throws IOException if the data store could not be queried or the query is
   *           invalid.
   */
  GetNamenodeRegistrationsResponse getNamenodeRegistrations(
      GetNamenodeRegistrationsRequest request) throws IOException;


  /**
   * Get the expired registrations from the registration cache.
   *
   * @return Expired registrations or zero-length list if none are found.
   * @throws StateStoreUnavailableException Throws exception if the data store
   *           is not initialized.
   * @throws IOException if the data store could not be queried or the query is
   *           invalid.
   */
  GetNamenodeRegistrationsResponse getExpiredNamenodeRegistrations()
      throws IOException;


  /**
   * Retrieves a list of registered nameservices and their associated info.
   *
   * @return Collection of information for each registered nameservice.
   * @throws IOException if the data store could not be queried or the query is
   *           invalid.
   */
  GetNamespaceInfoResponse getNamespaceInfo() throws IOException;

  /**
   * Inserts or updates a namenode membership entry into the table.
   *
   * @param request Fully populated NamenodeHeartbeatRequest request.
   * @return True if successful, false otherwise.
   * @throws StateStoreUnavailableException Throws exception if the data store
   *           is not initialized.
   * @throws IOException if the data store could not be queried or the query is
   *           invalid.
   */
  NamenodeHeartbeatResponse namenodeHeartbeat(
      NamenodeHeartbeatRequest request) throws IOException;

  /**
   * Overrides a cached namenode state with an updated state.
   *
   * @param request Fully populated OverrideNamenodeRegistrationRequest request.
   * @return OverrideNamenodeRegistrationResponse
   * @throws StateStoreUnavailableException if the data store is not
   *           initialized.
   * @throws IOException if the data store could not be queried or the query is
   *           invalid.
   */
  OverrideNamenodeRegistrationResponse overrideNamenodeRegistration(
      OverrideNamenodeRegistrationRequest request) throws IOException;
}
