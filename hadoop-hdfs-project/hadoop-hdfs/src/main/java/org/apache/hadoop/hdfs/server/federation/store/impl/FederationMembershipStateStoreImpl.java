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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.FederationCachedStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;

/**
 * Implementation of the {@link FederationMembershipStateStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationMembershipStateStoreImpl
    extends FederationStateStoreInterface
    implements FederationMembershipStateStore, FederationCachedStateStore {

  private static final Log LOG =
      LogFactory.getLog(FederationMembershipStateStoreImpl.class);

  /** Reported namespaces that are not decommissioned. */
  private Set<FederationNamespaceInfo> activeNamespaces;

  /** Namenodes (after evaluating the quorum) that are active in the cluster. */
  private KeyValueCache<MembershipState> activeRegistrations;
  /** Namenode status reports (raw) that were discarded for being too old. */
  private KeyValueCache<MembershipState> expiredRegistrations;
  private boolean initialized = false;

  /** Lock to access the memory cache (cachedRegistrations for now). */
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  @Override
  public void init() {
    this.activeRegistrations = new KeyValueCache<MembershipState>();
    this.expiredRegistrations = new KeyValueCache<MembershipState>();
    this.activeNamespaces = new TreeSet<FederationNamespaceInfo>();
  }

  /////////////////////////////////////////////////////////
  // MembershipState
  /////////////////////////////////////////////////////////

  private void verifyRegistrations() throws StateStoreUnavailableException {
    if (!initialized) {
      throw new StateStoreUnavailableException(
          "State store is not initialized, membership entries are not valid.");
    }
  }

  @Override
  public GetNamenodeRegistrationsResponse getNamenodeRegistrations(
      GetNamenodeRegistrationsRequest request) throws IOException {
    verifyRegistrations();

    // TODO Cache some common queries and sorts
    List<MembershipState> ret = null;
    Map<String, String> queryMap = request.getQuery();
    GetNamenodeRegistrationsResponse response = FederationProtocolFactory
        .newInstance(GetNamenodeRegistrationsResponse.class);

    readLock.lock();
    try {
      Collection<MembershipState> registrations =
          activeRegistrations.getValues();
      if (queryMap == null || queryMap.size() == 0) {
        ret = new ArrayList<MembershipState>(registrations);
      } else {
        ret = FederationStateStoreUtils.filterMultiple(
            queryMap, registrations);
      }
    } finally {
      readLock.unlock();
    }

    // Sort in ascending update date order
    Collections.sort(ret);
    response.setNamenodeMemberships(ret);
    return response;
  }

  @Override
  public NamenodeHeartbeatResponse namenodeHeartbeat(
      NamenodeHeartbeatRequest request) throws IOException {

    MembershipState record = request.getNamenodeMembership();
    NamenodeHeartbeatResponse response = FederationProtocolFactory.newInstance(
        NamenodeHeartbeatResponse.class);

    String nnId = record.getNamenodeKey();
    MembershipState existingEntry = this.activeRegistrations.get(nnId);
    if (existingEntry != null) {
      if (existingEntry.getState() != record.getState()) {
        LOG.info("NN registration state has changed: " +
            existingEntry + " -> " + record);
      } else {
        LOG.debug(
            "Updating NN registration: " + existingEntry + " -> " + record);
      }
    } else {
      LOG.info("Inserting new NN registration: " + record);
    }

    boolean status = getDriver().updateOrCreate(record, true, false);
    response.setResult(status);
    return response;
  }

  @Override
  public GetNamespaceInfoResponse getNamespaceInfo() throws IOException {
    verifyRegistrations();

    GetNamespaceInfoResponse response =
        FederationProtocolFactory.newInstance(GetNamespaceInfoResponse.class);
    Set<FederationNamespaceInfo> namespaces =
        new HashSet<FederationNamespaceInfo>();

    try {
      readLock.lock();
      namespaces.addAll(activeNamespaces);
    } finally {
      readLock.unlock();
    }
    response.setNamespaceInfo(namespaces);
    return response;
  }

  /**
   * Picks the most recent entry in the subset that is most agreeable on the
   * specified field. 1) If a majority of the collection has the same value for
   * the field, the first sorted entry within the subset the matches the
   * majority value 2) Otherwise the first sorted entry in the set of all
   * entries
   *
   * @param entries - Collection of state store record objects of the same type
   * @param fieldName - Field name for the value to compare
   * @return record that is most representative of the field name
   */
  private BaseRecord getRepresentativeQuorum(
      Collection<? extends BaseRecord> entries, String fieldName) {

    // Collate objects by field value: field value -> order set of records
    Map<Object, TreeSet<BaseRecord>> occurenceMap =
        new HashMap<Object, TreeSet<BaseRecord>>();
    for (BaseRecord entry : entries) {
      try {
        Object data = entry.getField(fieldName);
        TreeSet<BaseRecord> matchingSet = occurenceMap.get(data);
        if (matchingSet == null) {
          // TreeSet orders elements by descending date via comparators
          matchingSet = new TreeSet<BaseRecord>();
          occurenceMap.put(data, matchingSet);
        }
        matchingSet.add(entry);
      } catch (Exception ex) {
        LOG.error("Unable to match data field \"" + fieldName
            + "\" on record \"" + entry + "\"", ex);
      }
    }

    // Select largest group
    TreeSet<BaseRecord> largestSet = new TreeSet<BaseRecord>();
    for (TreeSet<BaseRecord> matchingSet : occurenceMap.values()) {
      if (largestSet.size() < matchingSet.size()) {
        largestSet = matchingSet;
      }
    }
    // If quorum, use the newest element here
    if (largestSet.size() > entries.size() / 2) {
      return largestSet.first();
      // Otherwise, return most recent by class comparator
    } else if (entries.size() > 0) {
      TreeSet<BaseRecord> sortedList = new TreeSet<BaseRecord>(entries);
      LOG.debug("Quorum failed, using most recent: " + sortedList.first());
      return sortedList.first();
    } else {
      return null;
    }
  }

  @Override
  public boolean loadData() throws IOException {
    QueryResult<MembershipState> result = null;
    try {
      // Fetch memberships
      result = getDriver().get(MembershipState.class);
      // Commit expired entries that were detected.
      FederationStateStoreUtils.overrideExpiredRecords(
          getDriver(), result, MembershipState.class);
    } catch (IOException e) {
      initialized = false;
      return false;
    }

    List<MembershipState> memberships = result.getRecords();
    if (memberships == null) {
      LOG.error("Failed to fetch registration info from the state store");
      initialized = false;
      return false;
    }

    // Update cache atomically
    this.writeLock.lock();
    try {
      this.activeRegistrations.clear();
      this.expiredRegistrations.clear();
      this.activeNamespaces.clear();

      // Build list of NN registrations: nnId -> registration list
      Map<String, List<MembershipState>> nnRegistrations =
          new HashMap<String, List<MembershipState>>();
      for (MembershipState membership : memberships) {
        String nnId = membership.getNamenodeKey();
        if (membership.getState() == FederationNamenodeServiceState.EXPIRED) {
          // Expired, RPC service does not use these
          expiredRegistrations.add(membership.getPrimaryKey(), membership);
        } else {
          // This is a valid NN registration, build a list of all registrations
          // using the NN id to use for the quorum calculation.
          List<MembershipState> nnRegistration =
              nnRegistrations.get(nnId);
          if (nnRegistration == null) {
            nnRegistration = new LinkedList<MembershipState>();
            nnRegistrations.put(nnId, nnRegistration);
          }
          nnRegistration.add(membership);
          FederationNamespaceInfo nsInfo =
              new FederationNamespaceInfo(membership.getBlockPoolId(),
                  membership.getClusterId(), membership.getNameserviceId());
          this.activeNamespaces.add(nsInfo);
        }
      }

      // Calculate most representative entry for each active NN id
      for (List<MembershipState> nnRegistration : nnRegistrations.values()) {
        // Run quorum based on NN state
        MembershipState representativeRecord =
            (MembershipState) getRepresentativeQuorum(
                nnRegistration, "state");
        this.activeRegistrations.add(
            representativeRecord.getNamenodeKey(), representativeRecord);
      }
      this.initialized = true;
      LOG.debug("Refreshed " + memberships.size() +
          " NN registrations from State Store");
    } finally {
      this.writeLock.unlock();
    }
    return true;
  }

  @Override
  public GetNamenodeRegistrationsResponse getExpiredNamenodeRegistrations()
      throws IOException {
    verifyRegistrations();
    GetNamenodeRegistrationsResponse response =
        FederationProtocolFactory.newInstance(
            GetNamenodeRegistrationsResponse.class);
    this.readLock.lock();
    try {
      Collection<MembershipState> vals = this.expiredRegistrations.getValues();
      response.setNamenodeMemberships(new ArrayList<MembershipState>(vals));
    } finally {
      this.readLock.unlock();
    }
    return response;
  }

  @Override
  public OverrideNamenodeRegistrationResponse overrideNamenodeRegistration(
      OverrideNamenodeRegistrationRequest request) throws IOException {

    verifyRegistrations();
    boolean status = false;
    this.writeLock.lock();
    try {
      String namenode = FederationUtil.generateNamenodeId(
          request.getNameserviceId(),
          request.getNamenodeId());
      MembershipState member = this.activeRegistrations.get(namenode);
      if (member != null) {
        member.setState(request.getState());
        status = true;
      }
    } finally {
      this.writeLock.unlock();
    }
    OverrideNamenodeRegistrationResponse response =
        FederationProtocolFactory.newInstance(
            OverrideNamenodeRegistrationResponse.class);
    response.setResult(status);
    return response;
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return MembershipState.class;
  }
}
