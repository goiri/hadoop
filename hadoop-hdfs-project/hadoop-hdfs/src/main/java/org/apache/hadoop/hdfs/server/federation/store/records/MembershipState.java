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

import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.ACTIVE;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.EXPIRED;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.UNAVAILABLE;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;

/**
 * Data schema for storing NN registration information in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService
 * FederationStateStoreService}.
 */
public abstract class MembershipState extends BaseRecord
    implements FederationNamenodeContext {

  /** Expiration time in ms for this entry. */
  private static long expirationMs;

  /**
   * Constructors.
   */
  public MembershipState() {
    super();
  }

  /**
   * Create a new membership instance.
   *
   * @param router Identifier of the router.
   * @param nameservice Identifier of the nameservice.
   * @param namenode Identifier of the namenode.
   * @param clusterId Identifier of the cluster.
   * @param blockPoolId Identifier of the blockpool.
   * @param rpcAddress RPC address.
   * @param webAddress HTTP address.
   * @param state State of the federation.
   * @param safemode If the safe mode is enabled.
   * @return Membership instance.
   * @throws IOException If we cannot create the instance.
   */
  public static MembershipState newInstance(String router, String nameservice,
      String namenode, String clusterId, String blockPoolId, String rpcAddress,
      String webAddress, FederationNamenodeServiceState state,
      boolean safemode) throws IOException {

    MembershipState record =
        FederationProtocolFactory.newInstance(MembershipState.class);
    record.setRouterId(router);
    record.setNameserviceId(nameservice);
    if (namenode == null) {
      record.setNamenodeId("");
    } else {
      record.setNamenodeId(namenode);
    }
    record.setRpcAddress(rpcAddress);
    record.setWebAddress(webAddress);
    record.setIsSafeMode(safemode);
    record.setState(state);
    record.setClusterId(clusterId);
    record.setBlockPoolId(blockPoolId);
    record.validate();
    return record;
  }

  public abstract void setRouterId(String routerId);

  public abstract String getRouterId();

  public abstract void setNameserviceId(String nameserviceId);

  public abstract void setNamenodeId(String namenodeId);

  public abstract void setWebAddress(String webAddress);

  public abstract void setRpcAddress(String rpcAddress);

  public abstract void setIsSafeMode(boolean isSafeMode);

  public abstract void setClusterId(String clusterId);

  public abstract void setBlockPoolId(String blockPoolId);

  public abstract void setState(FederationNamenodeServiceState state);

  public abstract String getNameserviceId();

  public abstract String getNamenodeId();

  public abstract String getClusterId();

  public abstract String getBlockPoolId();

  public abstract String getRpcAddress();

  public abstract String getWebAddress();

  public abstract boolean getIsSafeMode();

  public abstract FederationNamenodeServiceState getState();

  public abstract void setTotalSpace(long space);

  public abstract long getTotalSpace();

  public abstract void setAvailableSpace(long space);

  public abstract long getAvailableSpace();

  public abstract void setNumOfFiles(long files);

  public abstract long getNumOfFiles();

  public abstract void setNumOfBlocks(long blocks);

  public abstract long getNumOfBlocks();

  public abstract void setNumOfBlocksMissing(long blocks);

  public abstract long getNumOfBlocksMissing();

  public abstract void setNumOfBlocksPendingReplication(long blocks);

  public abstract long getNumOfBlocksPendingReplication();

  public abstract void setNumOfBlocksUnderReplicated(long blocks);

  public abstract long getNumOfBlocksUnderReplicated();

  public abstract void setNumOfBlocksPendingDeletion(long blocks);

  public abstract long getNumOfBlocksPendingDeletion();

  public abstract void setNumberOfActiveDatanodes(int nodes);

  public abstract int getNumberOfActiveDatanodes();

  public abstract void setNumberOfDeadDatanodes(int nodes);

  public abstract int getNumberOfDeadDatanodes();

  public abstract void setNumberOfDecomDatanodes(int nodes);

  public abstract int getNumberOfDecomDatanodes();

  public abstract void setLastContact(long contact);

  public abstract long getLastContact();


  @Override
  public String toString() {
    return getRouterId() + "->" + getNameserviceId() + ":" + getNamenodeId()
        + ":" + getRpcAddress() + "-" + getState();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("routerId", getRouterId());
    map.put("nameserviceId", getNameserviceId());
    map.put("namenodeId", getNamenodeId());
    return map;
  }

  /**
   * Check if the namenode is available.
   *
   * @return If the namenode is available.
   */
  public boolean isAvailable() {
    return getState() == ACTIVE;
  }

  /**
   * Validates the entry. Throws an IllegalArgementException if the data record
   * is missing required information.
   */
  public void validate() {
    super.validate();
    if (getNameserviceId() == null || getNameserviceId().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid registration, no nameservice specified " + this);
    }
    if (getWebAddress() == null || getWebAddress().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid registration, no web address specified " + this);
    }
    if (getRpcAddress() == null || getRpcAddress().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid registration, no rpc address specified " + this);
    }
    if (getState() != UNAVAILABLE && getState() != EXPIRED
        && (getBlockPoolId().isEmpty() || getBlockPoolId().length() == 0)) {
      throw new IllegalArgumentException(
          "Invalid registration, no block pool specified " + this);
    }
  }


  /**
   * Overrides the cached getBlockPoolId() with an update. The state will be
   * reset when the cache is flushed
   *
   * @param newState Service state of the namenode.
   */
  public void overrideState(FederationNamenodeServiceState newState) {
    this.setState(newState);
  }

  /**
   * Sort by nameservice, namenode, and router.
   *
   * @param other Another membership to compare to.
   * @return If this object goes before the parameter.
   */
  public int compareNameTo(MembershipState other) {
    int ret = this.getNameserviceId().compareTo(other.getNameserviceId());
    if (ret == 0) {
      ret = this.getNamenodeId().compareTo(other.getNamenodeId());
    }
    if (ret == 0) {
      ret = this.getRouterId().compareTo(other.getRouterId());
    }
    return ret;
  }

  /**
   * Get the identifier of this namenode registration.
   * @return Identifier of the namenode.
   */
  public String getNamenodeKey() {
    return FederationUtil.generateNamenodeId(
        this.getNameserviceId(), this.getNamenodeId());
  }

  @Override
  public boolean shouldUpdateField(String fieldName) {
    if (fieldName.equals("dateCreated")) {
      return false;
    } else if (fieldName.equals("lastContact")
        && (this.getState() == EXPIRED || this.getState() == UNAVAILABLE)) {
      return false;
    }
    return true;
  }

  @Override
  public boolean checkExpired(long currentTime) {
    if (super.checkExpired(currentTime)) {
      this.setState(EXPIRED);
      // Commit it
      return true;
    }
    return false;
  }

  @Override
  public long getExpirationMs() {
    return MembershipState.expirationMs;
  }

  /**
   * Set the expiration time for this class.
   *
   * @param time Expiration time in milliseconds.
   */
  public static void setExpirationMs(long time) {
    MembershipState.expirationMs = time;
  }

  /**
   * Get a comparator based on the name.
   *
   * @return Comparator based on the name.
   */
  public static Comparator<MembershipState> getNameComparator() {
    return new Comparator<MembershipState>() {
      public int compare(MembershipState m1, MembershipState m2) {
        return m1.compareNameTo(m2);
      }
    };
  }
}
