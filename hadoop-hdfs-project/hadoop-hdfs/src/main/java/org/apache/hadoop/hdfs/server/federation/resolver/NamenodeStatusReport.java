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

import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * Status of the namenode.
 */
public class NamenodeStatusReport {

  /** Namenode information. */
  private String nameserviceId = "";
  private String namenodeId = "";
  private String clusterId = "";
  private String blockPoolId = "";
  private String serviceAddress = "";
  private String webAddress = "";

  /** Namenode state. */
  private HAServiceState status = HAServiceState.STANDBY;
  private boolean safeMode = false;

  /** Datanodes stats. */
  private int liveDatanodes = -1;
  private int deadDatanodes = -1;
  private int decomDatanodes = -1;

  /** Space stats. */
  private long availableSpace = -1;
  private long numOfFiles = -1;
  private long numOfBlocks = -1;
  private long numOfBlocksMissing = -1;
  private long numOfBlocksPendingReplication = -1;
  private long numOfBlocksUnderReplicated = -1;
  private long numOfBlocksPendingDeletion = -1;
  private long totalSpace = -1;

  /** If the fields are valid. */
  private boolean registrationValid = false;
  private boolean statsValid = false;
  private boolean haStateValid = false;

  public NamenodeStatusReport(String ns, String nn, String rpc, String web) {
    this.nameserviceId = ns;
    this.namenodeId = nn;
    this.serviceAddress = rpc;
    this.webAddress = web;
  }

  /**
   * If the statistics are valid.
   *
   * @return If the statistics are valid.
   */
  public boolean statsValid() {
    return this.statsValid;
  }

  /**
   * If the registration is valid.
   *
   * @return If the registration is valid.
   */
  public boolean registrationValid() {
    return this.registrationValid;
  }

  /**
   * If the HA state is valid.
   *
   * @return If the HA state is valid.
   */
  public boolean haStateValid() {
    return this.haStateValid;
  }

  /**
   * Get the state of the Namenode being monitored.
   *
   * @return State of the Namenode.
   */
  public FederationNamenodeServiceState getState() {
    if (!registrationValid) {
      return FederationNamenodeServiceState.UNAVAILABLE;
    } else if (haStateValid) {
      return FederationNamenodeServiceState.getState(status);
    } else {
      return FederationNamenodeServiceState.ACTIVE;
    }
  }

  /**
   * Get the name service identifier.
   *
   * @return The name service identifier.
   */
  public String getNameserviceId() {
    return this.nameserviceId;
  }

  /**
   * Get the namenode identifier.
   *
   * @return The namenode identifier.
   */
  public String getNamenodeId() {
    return this.namenodeId;
  }

  /**
   * Get the cluster identifier.
   *
   * @return The cluster identifier.
   */
  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * Get the block pool identifier.
   *
   * @return The block pool identifier.
   */
  public String getBlockPoolId() {
    return this.blockPoolId;
  }

  /**
   * Get the RPC address.
   *
   * @return The RPC address.
   */
  public String getServiceAddress() {
    return this.serviceAddress;
  }

  /**
   * Get the web address.
   *
   * @return The web address.
   */
  public String getWebAddress() {
    return this.webAddress;
  }

  /**
   * Get the HA service state.
   *
   * @return The HA service state.
   */
  public void setHAServiceState(HAServiceState state) {
    this.status = state;
    this.haStateValid = true;
  }

  /**
   * Set the namespace information.
   *
   * @param info Namespace information.
   */
  public void setNamespaceInfo(NamespaceInfo info) {
    this.clusterId = info.getClusterID();
    this.blockPoolId = info.getBlockPoolID();
    this.registrationValid = true;
  }

  public void setSafeMode(boolean safemode) {
    this.safeMode = safemode;
  }

  public boolean getSafemode() {
    return this.safeMode;
  }

  /**
   * Set the datanode information.
   *
   * @param numLive Number of live nodes.
   * @param numDead Number of dead nodes.
   * @param numDecom Number of decommissioned nodes.
   */
  public void setDatanodeInfo(int numLive, int numDead, int numDecom) {
    this.liveDatanodes = numLive;
    this.deadDatanodes = numDead;
    this.decomDatanodes = numDecom;
    this.statsValid = true;
  }

  /**
   * Get the number of live blocks.
   *
   * @return The number of dead nodes.
   */
  public int getNumLiveDatanodes() {
    return this.liveDatanodes;
  }

  /**
   * Get the number of dead blocks.
   *
   * @return The number of dead nodes.
   */
  public int getNumDeadDatanodes() {
    return this.deadDatanodes;
  }

  /**
   * Get the number of decommissioned nodes.
   *
   * @return The number of decommissioned nodes.
   */
  public int getNumDecomDatanodes() {
    return this.decomDatanodes;
  }

  /**
   * Set the filesystem information.
   *
   * @param available
   * @param total
   * @param numFiles
   * @param numBlocks
   * @param numBlocksMissing
   * @param numOfBlocksPendingReplication
   * @param numOfBlocksUnderReplicated
   * @param numOfBlocksPendingDeletion
   */
  public void setNamesystemInfo(long available, long total,
      long numFiles, long numBlocks, long numBlocksMissing,
      long numOfBlocksPendingReplication, long numOfBlocksUnderReplicated,
      long numOfBlocksPendingDeletion) {
    this.totalSpace = total;
    this.availableSpace = available;
    this.numOfBlocks = numBlocks;
    this.numOfBlocksMissing = numBlocksMissing;
    this.numOfBlocksPendingReplication = numOfBlocksPendingReplication;
    this.numOfBlocksUnderReplicated = numOfBlocksUnderReplicated;
    this.numOfBlocksPendingDeletion = numOfBlocksPendingDeletion;
    this.numOfFiles = numFiles;
    this.statsValid = true;
  }

  /**
   * Get the number of blocks.
   *
   * @return The number of blocks.
   */
  public long getNumBlocks() {
    return this.numOfBlocks;
  }

  /**
   * Get the number of files.
   *
   * @return The number of files.
   */
  public long getNumFiles() {
    return this.numOfFiles;
  }

  /**
   * Get the total space.
   *
   * @return The total space.
   */
  public long getTotalSpace() {
    return this.totalSpace;
  }

  /**
   * Get the available space.
   *
   * @return The available space.
   */
  public long getAvailableSpace() {
    return this.availableSpace;
  }

  /**
   * Get the number of missing blocks.
   *
   * @return Number of missing blocks.
   */
  public long getNumBlocksMissing() {
    return this.numOfBlocksMissing;
  }

  /**
   * Get the number of pending replication blocks.
   *
   * @return Number of pending replication blocks.
   */
  public long getNumOfBlocksPendingReplication() {
    return this.numOfBlocksPendingReplication;
  }

  /**
   * Get the number of under replicated blocks.
   *
   * @return Number of under replicated blocks.
   */
  public long getNumOfBlocksUnderReplicated() {
    return this.numOfBlocksUnderReplicated;
  }

  /**
   * Get the number of pending deletion blocks.
   *
   * @return Number of pending deletion blocks.
   */
  public long getNumOfBlocksPendingDeletion() {
    return this.numOfBlocksPendingDeletion;
  }

  @Override
  public String toString() {
    return String.format("%s-%s:%s",
        nameserviceId, namenodeId, serviceAddress);
  }
}