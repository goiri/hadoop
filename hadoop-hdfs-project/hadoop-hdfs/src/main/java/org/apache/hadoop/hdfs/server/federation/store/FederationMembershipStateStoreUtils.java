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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.OverrideNamenodeRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;

/**
 * Utility wrapper around HDFS Federation state store API requests.
 */
public final class FederationMembershipStateStoreUtils {

  private FederationMembershipStateStoreUtils() {
    // Utility class
  }

  /**
   * Build a set of unique values found in all namespaces.
   *
   * @param store FederationMembershipStateStore instance
   * @param getterName String name of the appropraite FederationNamespaceInfo
   *          getter function
   *
   * @return Set of unique string values found in all discovered namespaces.
   *
   * @throws IOException if the query could not be executed.
   */
  public static Set<String> aggregateFederationNamespaceInfo(
      FederationMembershipStateStore store, String getterName)
          throws IOException {

    Set<String> builder = new HashSet<String>();
    GetNamespaceInfoResponse response = store.getNamespaceInfo();
    for (FederationNamespaceInfo namespace : response.getNamespaceInfo()) {
      try {
        Method m = FederationNamespaceInfo.class.getDeclaredMethod(getterName);
        String data = (String) m.invoke(namespace);
        builder.add(data);
      } catch (Exception ex) {
        throw new IOException(
            "Unable to invoke namepsace getter for property - " + getterName
                + " from - " + namespace);
      }
    }
    return builder;
  }

  /**
   * Fetch a single namenode membership enitty object from the store.
   *
   * @param store FederationMembershipStateStore instance
   * @param nameserviceId The HDFS nameservcie ID to search for
   * @param namenodeId The HDFS namenode ID to search for
   *
   * @return The single NamenodeMembershipRecord that matches the query or null
   *         if not found.
   *
   * @throws IOException if the query could not be executed.
   */
  public static MembershipState getSingleMembership(
      FederationMembershipStateStore store, String nameserviceId,
      String namenodeId) throws IOException {

    GetNamenodeRegistrationsRequest request =
        FederationProtocolFactory.newInstance(
            GetNamenodeRegistrationsRequest.class);
    Map<String, String> queryMap = new HashMap<String, String>();
    queryMap.put("nameserviceId", nameserviceId);
    queryMap.put("namenodeId", namenodeId);
    request.setQuery(queryMap);

    GetNamenodeRegistrationsResponse response;
    response = store.getNamenodeRegistrations(request);

    List<MembershipState> results = response.getNamenodeMemberships();
    if (results != null && results.size() == 1) {
      MembershipState record = results.get(0);
      return record;
    }
    return null;
  }

  /**
   * Fetches the most active namenode memberships for all known nameservices.
   * The fetched membership may not or may not be active. Excludes expired
   * memberships.
   *
   * @param resolver ActiveNamenodeResolver instance to determine the most
   *          active instance.
   * @param store FederationMembershipStateStore instance to retrive the
   *          membership data records.
   *
   * @return List of the most active NNs from each known nameservice.
   *
   * @throws IOException if the query could not be performed.
   */
  public static List<MembershipState> getActiveMemberships(
      ActiveNamenodeResolver resolver, FederationMembershipStateStore store)
          throws IOException {

    List<MembershipState> resultList = new ArrayList<MembershipState>();
    GetNamespaceInfoResponse response = store.getNamespaceInfo();
    for (FederationNamespaceInfo nsInfo : response.getNamespaceInfo()) {
      // Fetch the most recent namenode registration
      String nsId = nsInfo.getNameserviceId();
      List<? extends FederationNamenodeContext> lookup =
          resolver.getPrioritizedNamenodesForNameserviceId(nsId);
      FederationNamenodeContext firstItem = lookup.get(0);
      if (firstItem != null && firstItem instanceof MembershipState) {
        resultList.add((MembershipState) firstItem);
      }
    }
    return resultList;
  }

  /**
   * Fetches all non-expired namenode memberships in the data store.
   *
   * @param store FederationMembershipStateStore instance to retrive the
   *          membership data records.
   *
   * @return List of all non-expired namenode memberships.
   *
   * @throws IOException if the query could not be performed.
   */
  public static List<MembershipState> getAllCurrentMemberships(
      FederationMembershipStateStore store) throws IOException {

    GetNamenodeRegistrationsRequest request =
        FederationProtocolFactory.newInstance(
            GetNamenodeRegistrationsRequest.class);
    request.setQuery(null);
    GetNamenodeRegistrationsResponse response =
        store.getNamenodeRegistrations(request);
    return response.getNamenodeMemberships();
  }

  /**
   * Fetches all expired namenode memberships in the data store.
   *
   * @param store FederationMembershipStateStore instance to retrive the
   *          membership data records.
   *
   * @return List of all expired namenode memberships.
   *
   * @throws IOException if the query could not be performed.
   */
  public static List<MembershipState> getAllExpiredMemberships(
      FederationMembershipStateStore store) throws IOException {

    GetNamenodeRegistrationsResponse response =
        store.getExpiredNamenodeRegistrations();
    return response.getNamenodeMemberships();
  }

  /**
   * Aggregate a namenode data element from the most active namenode in each
   * registered nameservice.
   *
   * @param resolver ActiveNamenodeResolver instance to determine the most
   *          active instance.
   * @param store FederationMembershipStateStore instance to retrieve the
   *          membership data records.
   * @param getter String name of the getter function to invoke on the
   *          discovered NamenodeMembershipRecord object.
   *
   * @return Aggregated getter return values from all registered nameservices,
   *         one per nameservice.
   *
   * @throws IOException if the query could not be performed.
   */
  public static Collection<Object> collectMetricsForAllNameservices(
      ActiveNamenodeResolver resolver, FederationMembershipStateStore store,
      String getter) throws IOException {

    List<Object> resultList = new ArrayList<Object>();
    Method metricsGetter;
    try {
      metricsGetter = MembershipState.class.getDeclaredMethod(getter);
    } catch (Exception e) {
      throw new IOException(
          "Unable to get property " + getter + " from membership record");
    }
    List<MembershipState> namenodes = getActiveMemberships(resolver, store);
    for (MembershipState namenode : namenodes) {
      try {
        Object data = metricsGetter.invoke(namenode);
        resultList.add(data);
      } catch (Exception e) {
        throw new IOException("Unable to invoke getter for property " + getter
            + " from " + namenode);
      }
    }
    return resultList;
  }

  /**
   * Register a namenode heartbeat with the state store.
   *
   * @param store FederationMembershipStateStore instance to retrieve the
   *          membership data records.
   * @param namenode A fully populated namenode membership record to be
   *          committed to the data store.
   * @return True if successful, false otherwise.
   * @throws IOException if the state store query could not be performed.
   */
  public static boolean sendNamenodeHeartbeat(
      FederationMembershipStateStore store, MembershipState namenode)
          throws IOException {

    NamenodeHeartbeatRequest request =
        FederationProtocolFactory.newInstance(NamenodeHeartbeatRequest.class);
    request.setNamenodeMembership(namenode);
    NamenodeHeartbeatResponse response;
    response = store.namenodeHeartbeat(request);
    return response.getResult();
  }

  /**
   * Override the state of a namenode registration in the store. The router uses
   * this API to update the cached most-active NN for a particular nameservice
   * when a failover/transition is empirically discovered.
   *
   * @param store FederationMembershipStateStore instance to retrieve the
   *          membership data records.
   * @param nameserviceId nameservice ID of the NN to update
   * @param namenodeId namenode ID of the NN to update
   * @param state The updated state of the NN, will override existing cached
   *          state.
   * @return True if successful, false otherwise
   * @throws IOException if the state store query could not be performed.
   */
  public static boolean overrideNamenodeHeartbeat(
      FederationMembershipStateStore store, String nameserviceId,
      String namenodeId, FederationNamenodeServiceState state)
      throws IOException {

    OverrideNamenodeRegistrationRequest request = FederationProtocolFactory
        .newInstance(OverrideNamenodeRegistrationRequest.class);
    request.setNamenodeId(namenodeId);
    request.setNameserviceId(nameserviceId);
    request.setState(state);
    OverrideNamenodeRegistrationResponse response =
        store.overrideNamenodeRegistration(request);
    return response.getResult();
  }
}
