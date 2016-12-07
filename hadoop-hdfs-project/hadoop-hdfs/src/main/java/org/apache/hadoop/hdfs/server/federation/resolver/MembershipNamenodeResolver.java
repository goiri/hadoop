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
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.FederationCachedStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.util.Time;

/**
 * Implements a cached lookup of the most recently active namenode for a
 * particular nameservice. Relies on the {@link FederationStateStoreService} to
 * discover available nameservices and namenodes.
 */
public class MembershipNamenodeResolver
    implements ActiveNamenodeResolver, FederationCachedStateStore {

  private static final Log LOG =
      LogFactory.getLog(MembershipNamenodeResolver.class);

  /** Reference to the state store. */
  private FederationStateStoreService stateStore;
  private FederationMembershipStateStore membershipInterface;

  /** Parent router ID. */
  private String routerId;

  /**
   * Cached lookup of NN for nameservice/blockpool. Invalidated on cache
   * refresh.
   */
  private ConcurrentHashMap<String, List<? extends FederationNamenodeContext>>
  cachedNameserviceRegistrations;

  private ConcurrentHashMap<String, List<? extends FederationNamenodeContext>>
  cachedBlockPoolRegistrations;

  public MembershipNamenodeResolver(Configuration conf,
      FederationStateStoreService store) {
    this.stateStore = store;
    this.cachedBlockPoolRegistrations =
        new ConcurrentHashMap<String, List<? extends FederationNamenodeContext>>();
    this.cachedNameserviceRegistrations =
        new ConcurrentHashMap<String, List<? extends FederationNamenodeContext>>();

    // Request cache updates from the state store
    this.stateStore.registerRemoteCache(this);
  }

  private FederationMembershipStateStore getMembershipStore()
      throws IOException {
    if (membershipInterface == null) {
      this.membershipInterface = stateStore
          .getRegisteredInterface(FederationMembershipStateStore.class);
      if (this.membershipInterface == null) {
        throw new IOException(
            "State store service does not contain a registered interface for "
                + "FederationMembershipStateStore");
      }
    }
    return membershipInterface;
  }

  /**
   * Called by the cache update service when the NN registration data has been
   * reloaded.
   */
  @Override
  public boolean loadData() {
    // Force refresh of active NN cache
    cachedBlockPoolRegistrations.clear();
    cachedNameserviceRegistrations.clear();
    return true;
  }

  @Override
  public void updateActiveNamenode(String ns,
      InetSocketAddress successfulAddress) throws IOException {
    // called when we have an RPC miss and successful hit on an alternate NN.
    // Temporarily update our cache, it will be overwritten on the next update.
    Map<String, String> queryMap = new HashMap<String, String>();
    queryMap.put("rpcAddress",
        successfulAddress.getHostName() + ":" + successfulAddress.getPort());
    queryMap.put("nameserviceId", ns);
    try {
      GetNamenodeRegistrationsRequest request =
          FederationProtocolFactory.newInstance(
              GetNamenodeRegistrationsRequest.class);
      request.setQuery(queryMap);
      GetNamenodeRegistrationsResponse response =
          getMembershipStore().getNamenodeRegistrations(request);
      List<MembershipState> records = response.getNamenodeMemberships();
      if (records != null && records.size() == 1) {
        MembershipState record = records.get(0);
        OverrideNamenodeRegistrationRequest overrideRequest =
            FederationProtocolFactory.newInstance(
                OverrideNamenodeRegistrationRequest.class);
        overrideRequest.setNamenodeId(record.getNamenodeId());
        overrideRequest.setNameserviceId(record.getNameserviceId());
        overrideRequest.setState(FederationNamenodeServiceState.ACTIVE);
        getMembershipStore().overrideNamenodeRegistration(overrideRequest);
      }
    } catch (StateStoreUnavailableException e) {
      LOG.error("Unable to mark registration " + successfulAddress
          + " as active, state store is not available");
    }
  }

  @Override
  public List<? extends FederationNamenodeContext>
  getPrioritizedNamenodesForNameserviceId(
      String nameserviceId) throws IOException {

    List<? extends FederationNamenodeContext> cachedResult =
          cachedNameserviceRegistrations.get(nameserviceId);
    if (cachedResult != null) {
      return cachedResult;
    }

    Map<String, String> queryMap = new HashMap<String, String>();
    queryMap.put("nameserviceId", nameserviceId);
    List<? extends FederationNamenodeContext> result;
    try {
      result = getRecentRegistrationForQuery(queryMap, true, false);
    } catch (StateStoreUnavailableException e) {
      LOG.error("Unable to determine active NN for nameservice ID "
          + nameserviceId + ", statestore is not ready.");
      return null;
    }
    if (result == null) {
      LOG.error("Unable to located elibible NNs for nameservice ID "
          + nameserviceId);
      return null;
    }

    cachedNameserviceRegistrations.put(nameserviceId, result);
    return result;
  }

  @Override
  public List<? extends FederationNamenodeContext>
  getPrioritizedNamenodesForBlockPoolId(
      String blockPoolId) throws IOException {

    List<? extends FederationNamenodeContext> cachedResult =
        cachedBlockPoolRegistrations.get(blockPoolId);
    if (cachedResult != null) {
      return cachedResult;
    }

    Map<String, String> queryMap = new HashMap<String, String>();
    queryMap.put("blockPoolId", blockPoolId);
    List<? extends FederationNamenodeContext> result;
    try {
      result = getRecentRegistrationForQuery(queryMap, true, false);
    } catch (StateStoreUnavailableException e) {
      LOG.error("Unable to determine active NN for blockPoolId " + blockPoolId
          + ", statestore is not ready.");
      return null;
    }
    if (result == null) {
      LOG.error(
          "Unable to located elibible NNs for blockPoolId " + blockPoolId);
      return null;
    }

    cachedBlockPoolRegistrations.put(blockPoolId, result);
    return result;
  }

  @Override
  public boolean registerNamenode(NamenodeStatusReport report)
      throws IOException {

    if(this.routerId == null) {
      LOG.warn("Unable to register namenode, router ID is not known "
          + report.toString());
      return false;
    }

    MembershipState record = MembershipState.newInstance(
        routerId, report.getNameserviceId(), report.getNamenodeId(),
        report.getClusterId(), report.getBlockPoolId(),
        report.getServiceAddress(), report.getWebAddress(), report.getState(),
        report.getSafemode());

    if (report.statsValid()) {
      record.setNumOfFiles(report.getNumFiles());
      record.setNumOfBlocks(report.getNumBlocks());
      record.setNumOfBlocksMissing(report.getNumBlocksMissing());
      record.setNumOfBlocksPendingReplication(
          report.getNumOfBlocksPendingReplication());
      record.setNumOfBlocksUnderReplicated(
          report.getNumOfBlocksUnderReplicated());
      record.setNumOfBlocksPendingDeletion(
          report.getNumOfBlocksPendingDeletion());
      record.setAvailableSpace(report.getAvailableSpace());
      record.setTotalSpace(report.getTotalSpace());
      record.setNumberOfDecomDatanodes(report.getNumDecomDatanodes());
      record.setNumberOfActiveDatanodes(report.getNumLiveDatanodes());
      record.setNumberOfDeadDatanodes(report.getNumDeadDatanodes());
    }

    if (report.getState() != FederationNamenodeServiceState.UNAVAILABLE) {
      // Set/update our last contact time
      record.setLastContact(Time.now());
    }

    NamenodeHeartbeatRequest request =
        FederationProtocolFactory.newInstance(NamenodeHeartbeatRequest.class);
    request.setNamenodeMembership(record);
    return getMembershipStore().namenodeHeartbeat(request).getResult();
  }

  @Override
  public Set<FederationNamespaceInfo> getNamespaces() throws IOException {
    GetNamespaceInfoResponse response = getMembershipStore().getNamespaceInfo();
    return response.getNamespaceInfo();
  }

  /**
   * Picks the most relevant record registration that matches the query. Return
   * registrations matching the query in this preference: 1) Most recently
   * updated ACTIVE registration 2) Most recently updated STANDBY registration
   * (if showStandby) 3) Most recently updated UNAVAILABLE registration (if
   * showUnavailable). EXPIRED registrations are ignored.
   *
   * @param query The select query for NN registrations.
   * @param excludes List of NNs to exclude from matching results.
   * @param includeUnavailable include UNAVAILABLE registrations.
   * @param includeExpired include EXPIRED registrations.
   * @return List of memberships or null if no registrations that
   *         both match the query AND the selected states.
   * @throws IOException
   */
  private List<MembershipState> getRecentRegistrationForQuery(
      Map<String, String> query, boolean includeUnavailable,
      boolean includeExpired) throws IOException {

    // Retrieve a list of all registrations that match this query.
    // This may include all NN records for a namespace/blockpool, including
    // duplicate records for the same NN from different routers.
    GetNamenodeRegistrationsRequest request = FederationProtocolFactory
        .newInstance(GetNamenodeRegistrationsRequest.class);
    request.setQuery(query);
    GetNamenodeRegistrationsResponse response =
        getMembershipStore().getNamenodeRegistrations(request);

    List<MembershipState> memberships = response.getNamenodeMemberships();
    if (!includeExpired || !includeUnavailable) {
      Iterator<MembershipState> iterator = memberships.iterator();
      while (iterator.hasNext()) {
        MembershipState membership = iterator.next();
        if (membership.getState() == FederationNamenodeServiceState.EXPIRED
            && !includeExpired) {
          iterator.remove();
        } else if (membership
            .getState() == FederationNamenodeServiceState.UNAVAILABLE
            && !includeUnavailable) {
          iterator.remove();
        }
      }
    }

    List<MembershipState> priorityList = new ArrayList<MembershipState>();
    priorityList.addAll(memberships);
    Collections.sort(priorityList, new NamenodePriorityComparator());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Selected most recent NN " + priorityList + " for query.");
    }
    return priorityList;
  }

  @Override
  public void setRouterId(String router) {
    this.routerId = router;
  }
}
