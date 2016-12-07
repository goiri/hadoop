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

package org.apache.hadoop.hdfs.server.federation.metrics;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.FederationDecommissionNamespaceStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationMembershipStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.FederationRebalancerStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import com.google.common.collect.ImmutableMap;

/**
 * Implementation of the Router metrics collector.
 */
public class FederationMetrics implements FederationMBean {

  private static final Log LOG = LogFactory.getLog(FederationMetrics.class);

  /** Router interface. */
  private Router router;

  /** FederationState JMX bean. */
  private ObjectName beanName;

  /** Resolve the namenode for each namespace. */
  private ActiveNamenodeResolver namenodeResolver;

  /** Membership state store. */
  private FederationMembershipStateStore membershipStore;
  /** Router state store. */
  private FederationRouterStateStore routerStore;
  /** Mount table store. */
  private FederationMountTableStore mountTableStore;
  /** Decommission namespace store. */
  private FederationDecommissionNamespaceStore decommissionStore;
  /** Rebalancer operation store. */
  private FederationRebalancerStore rebalancerStore;


  public FederationMetrics(Router router) throws IOException {
    this.router = router;

    try {
      StandardMBean bean = new StandardMBean(this, FederationMBean.class);
      this.beanName = MBeans.register("Router", "FederationState", bean);
      LOG.info("Registered Router MBean: " + this.beanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad Router MBean setup", e);
    }

    // Resolve namenode for each nameservice
    this.namenodeResolver = this.router.getNamenodeResolver();

    // State store interfaces
    FederationStateStoreService stateStore = this.router.getStateStore();
    if (stateStore == null) {
      LOG.error("State store not available");
    } else {
      this.membershipStore = stateStore.getRegisteredInterface(
          FederationMembershipStateStore.class);
      this.routerStore = stateStore.getRegisteredInterface(
          FederationRouterStateStore.class);
      this.mountTableStore = stateStore.getRegisteredInterface(
          FederationMountTableStore.class);
      this.decommissionStore = stateStore.getRegisteredInterface(
          FederationDecommissionNamespaceStore.class);
      this.rebalancerStore = stateStore.getRegisteredInterface(
          FederationRebalancerStore.class);
    }
  }

  /**
   * Unregister the JMX beans.
   */
  public void close() {
    if (this.beanName != null) {
      MBeans.unregister(beanName);
    }
  }

  @Override
  public String getNamenodes() {
    final Map<String, Map<String, Object>> info =
        new LinkedHashMap<String, Map<String, Object>>();
    try {
      // Get the values from the store
      GetNamenodeRegistrationsRequest request =
          FederationProtocolFactory.newInstance(
              GetNamenodeRegistrationsRequest.class);
      request.setQuery(null);
      GetNamenodeRegistrationsResponse response =
          membershipStore.getNamenodeRegistrations(request);

      // Order the namenodes
      final List<MembershipState> namenodes =
          response.getNamenodeMemberships();
      if (namenodes == null || namenodes.size() == 0) {
        return JSON.toString(info);
      }
      List<MembershipState> namenodesOrder =
          new ArrayList<MembershipState>(namenodes);
      Collections.sort(namenodesOrder, MembershipState.getNameComparator());

      // Dump namenodes information into JSON
      for (MembershipState namenode : namenodesOrder) {
        ImmutableMap.Builder<String, Object> innerInfo =
            ImmutableMap.<String, Object> builder();
        innerInfo.putAll(namenode.getJson());
        long dateModified = namenode.getDateModified();
        long lastHeartbeat = FederationUtil.getSecondsSince(dateModified);
        innerInfo.put("lastHeartbeat", lastHeartbeat);
        innerInfo.put("used",
            (Long) BaseRecord.getJson(namenode.getTotalSpace()) -
            (Long) BaseRecord.getJson(namenode.getAvailableSpace()));
        info.put(namenode.getNamenodeKey(), innerInfo.build());
      }
    } catch (IOException e) {
      LOG.error("Enable to fetch json representation of namenodes "
          + e.getMessage());
      return "{}";
    }
    return JSON.toString(info);
  }

  @Override
  public String getNameservices() {
    final Map<String, Map<String, Object>> info =
        new LinkedHashMap<String, Map<String, Object>>();
    try {
      final List<MembershipState> namenodes =
          FederationMembershipStateStoreUtils.getActiveMemberships(
              namenodeResolver, membershipStore);
      List<MembershipState> namenodesOrder =
          new ArrayList<MembershipState>(namenodes);
      Collections.sort(namenodesOrder, MembershipState.getNameComparator());

      // Dump namenodes information into JSON
      for (MembershipState namenode : namenodesOrder) {
        ImmutableMap.Builder<String, Object> innerInfo =
            ImmutableMap.<String, Object> builder();
        innerInfo.putAll(namenode.getJson());
        long dateModified = namenode.getDateModified();
        long lastHeartbeat = FederationUtil.getSecondsSince(dateModified);
        innerInfo.put("lastHeartbeat", lastHeartbeat);
        innerInfo.put("used",
            (Long) BaseRecord.getJson(namenode.getTotalSpace()) -
            (Long) BaseRecord.getJson(namenode.getAvailableSpace()));
        info.put(namenode.getNamenodeKey(), innerInfo.build());
      }
    } catch (IOException e) {
      LOG.error("Unable to retrieve nameservices for JMX " + e.getMessage());
      return "{}";
    }
    return JSON.toString(info);
  }

  @Override
  public String getRouters() {

    final Map<String, Map<String, Object>> info =
        new LinkedHashMap<String, Map<String, Object>>();

    try {
      // Get all the routers in order
      final List<RouterState> routers =
          FederationRouterStateStoreUtils.getAllActiveRouters(routerStore);
      List<RouterState> routersOrder = new ArrayList<RouterState>(routers);
      Collections.sort(routersOrder);

      // Dump router information into JSON
      for (RouterState record : routersOrder) {
        ImmutableMap.Builder<String, Object> innerInfo =
            ImmutableMap.<String, Object> builder();
        innerInfo.putAll(record.getJson());
        long dateModified = record.getDateModified();
        long lastHeartbeat = FederationUtil.getSecondsSince(dateModified);
        innerInfo.put("lastHeartbeat", lastHeartbeat);
        long pathLockVersion = record.getPathLockVersion();
        long lastPathLockUpdate =
            FederationUtil.getSecondsSince(pathLockVersion);
        innerInfo.put("lastPathLockUpdate", lastPathLockUpdate);
        long registrationTableVersion = record.getRegistrationTableVersion();
        long lastRegistrationsUpdate =
            FederationUtil.getSecondsSince(registrationTableVersion);
        innerInfo.put("lastRegistrationsUpdate", lastRegistrationsUpdate);
        info.put(record.getPrimaryKey(), innerInfo.build());
      }
    } catch (IOException e) {
      LOG.error(
          "Unable to fetch json representation of routers from data store - "
              + e.getMessage());
      return "{}";
    }
    return JSON.toString(info);
  }

  @Override
  public String getMountTable() {
    final List<Map<String, Object>> info =
        new LinkedList<Map<String, Object>>();

    // Get all the mount points in order
    try {
      QueryResult<MountTable> res =
          FederationMountTableStoreUtils.getMountTableRecords(
              mountTableStore, "/");

      final List<MountTable> mounts = res.getRecords();
      List<MountTable> orderedMounts = new ArrayList<MountTable>(mounts);
      Collections.sort(orderedMounts, MountTable.getSourceComparator());

      // Dump mount table entries information into JSON
      for (MountTable entry : orderedMounts) {
        for (RemoteLocation location : entry.getDestinations()) {
          ImmutableMap.Builder<String, Object> innerInfo =
              ImmutableMap.<String, Object> builder();
          innerInfo.putAll(entry.getJson());
          innerInfo.put("nameserviceId",
              BaseRecord.getJson(location.getNameserviceId()));
          innerInfo.put("path", BaseRecord.getJson(location.getDest()));
          info.add(innerInfo.build());
        }
      }
    } catch (IOException e) {
      LOG.error(
          "Cannot generate JSON of mount table from store " + e.getMessage());
      return "[]";
    }
    return JSON.toString(info);
  }

  @Override
  public long getTotalCapacity() {
    return getNameserviceAggregatedLong("getTotalSpace");
  }

  @Override
  public long getRemainingCapacity() {
    return getNameserviceAggregatedLong("getAvailableSpace");
  }

  @Override
  public long getUsedCapacity() {
    return getTotalCapacity() - getRemainingCapacity();
  }

  @Override
  public int getNumNameservices() {
    try {
      Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
      return nss.size();
    } catch (IOException e) {
      LOG.error(
          "Cannot fetch number of expired registrations from state store "
              + e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumDecommissionedNameservices() {
    try {
      Set<String> disabledNamespaces =
          decommissionStore.getDisabledNamespaces();
      return disabledNamespaces.size();
    } catch (IOException e) {
      LOG.error(
          "Cannot fetch number of expired registrations from state store "
              + e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumNamenodes() {
    try {
      List<MembershipState> memberships =
          FederationMembershipStateStoreUtils.getAllCurrentMemberships(
              membershipStore);
      return memberships.size();
    } catch (IOException e) {
      LOG.error("Unable to retrieve numNamenodes for JMX " + e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumExpiredNamenodes() {
    try {
      List<MembershipState> expiredMemberships =
          FederationMembershipStateStoreUtils.getAllExpiredMemberships(
              membershipStore);
      return expiredMemberships.size();
    } catch (IOException e) {
      LOG.error(
          "Unable to retrieve numExpiredNamenodes for JMX " + e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumLiveNodes() {
    return getNameserviceAggregatedInt("getNumberOfActiveDatanodes");
  }

  @Override
  public int getNumDeadNodes() {
    return getNameserviceAggregatedInt("getNumberOfDeadDatanodes");
  }

  @Override
  public int getNumDecommissioningNodes() {
    return getNameserviceAggregatedInt("getNumberOfDecomDatanodes");
  }

  @Override
  public long getNumBlocks() {
    return getNameserviceAggregatedLong("getNumOfBlocks");
  }

  @Override
  public long getNumOfMissingBlocks() {
    return getNameserviceAggregatedLong("getNumOfBlocksMissing");
  }

  @Override
  public long getNumOfBlocksPendingReplication() {
    return getNameserviceAggregatedLong("getNumOfBlocksPendingReplication");
  }

  @Override
  public long getNumOfBlocksUnderReplicated() {
    return getNameserviceAggregatedLong("getNumOfBlocksUnderReplicated");
  }

  @Override
  public long getNumOfBlocksPendingDeletion() {
    return getNameserviceAggregatedLong("getNumOfBlocksPendingDeletion");
  }

  @Override
  public long getNumFiles() {
    return getNameserviceAggregatedLong("getNumOfFiles");
  }

  @Override
  public String getRouterStarted() {
    long startTime = this.router.getStartTime();
    return new Date(startTime).toString();
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override
  public String getCompiledDate() {
    return FederationUtil.getCompiledDate();
  }

  @Override
  public String getCompileInfo() {
    return FederationUtil.getCompileInfo();
  }

  @Override
  public String getHostAndPort() {
    InetSocketAddress address = this.router.getHttpServerAddress();
    try {
      return InetAddress.getLocalHost().getHostName() + ":" + address.getPort();
    } catch (UnknownHostException e) {
      return "Unknown";
    }
  }

  @Override
  public String getRouterId() {
    return this.router.getRouterId();
  }

  @Override
  public String getClusterId() {
    try {
      Set<String> clusterIds =
          FederationMembershipStateStoreUtils.aggregateFederationNamespaceInfo(
              membershipStore, "getClusterId");
      return clusterIds.toString();
    } catch (IOException e) {
      LOG.error("Unable to fetch cluster ID metrics " + e.getMessage());
      return "";
    }
  }

  @Override
  public String getBlockPoolId() {
    try {
      Set<String> blockpoolIds =
          FederationMembershipStateStoreUtils.aggregateFederationNamespaceInfo(
              membershipStore, "getBlockPoolId");
      return blockpoolIds.toString();
    } catch (IOException e) {
      LOG.error("Unable to fetch block pool ID metrics " + e.getMessage());
      return "";
    }
  }

  @Override
  public String getRebalancerHistory() {
    final Map<String, Map<String, Object>> info =
        new LinkedHashMap<String, Map<String, Object>>();

    try {
      // Get all the routers in desc dateModified order
      Collection<RebalancerLog> orderedLogs =
          this.rebalancerStore.getRebalancerOperations(null);

      // Dump rebalancer log information into JSON
      for (RebalancerLog log : orderedLogs) {
        if (log != null) {
          ImmutableMap.Builder<String, Object> innerInfo =
              ImmutableMap.<String, Object> builder();
          innerInfo.putAll(log.getJson());
          info.put(log.getPrimaryKey(), innerInfo.build());
        } else {
          LOG.error("We got a null rebalance log");
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to fetch rebalancer history from state store.", e);
      return null;
    }

    return JSON.toString(info);
  }

  @Override
  public String getRouterStatus() {
    return router.getRouterState().toString();
  }

  /**
   * Get the aggregated value for a method for all nameservices.
   * @param methodName Name of the method.
   * @return Aggregated long.
   */
  private long getNameserviceAggregatedLong(String methodName) {
    long total = 0;
    try {
      Collection<Object> data =
          FederationMembershipStateStoreUtils.collectMetricsForAllNameservices(
              namenodeResolver, membershipStore, methodName);
      for (Object o : data) {
        Long l = (Long) o;
        total += l;
      }
    } catch (IOException e) {
      LOG.error("Unable to " + methodName + " for JMX " + e.getMessage());
      return 0;
    }
    return total;
  }

  /**
   * Get the aggregated value for a method for all nameservices.
   * @param methodName Name of the method.
   * @return Aggregated integer.
   */
  private int getNameserviceAggregatedInt(String methodName) {
    int total = 0;
    try {
      Collection<Object> data =
          FederationMembershipStateStoreUtils.collectMetricsForAllNameservices(
              namenodeResolver, membershipStore, methodName);
      for (Object o : data) {
        Integer l = (Integer) o;
        total += l;
      }
    } catch (IOException e) {
      LOG.error("Unable to " + methodName + " for JMX " + e.getMessage());
      return 0;
    }
    return total;
  }

}
