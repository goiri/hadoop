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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;

/**
 * Protobuf implementation of the MembershipState record.
 */
public class MembershipStatePBImpl extends MembershipState {

  private FederationProtocolPBTranslator<NamenodeMembershipRecordProto, NamenodeMembershipRecordProto.Builder, NamenodeMembershipRecordProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<NamenodeMembershipRecordProto, NamenodeMembershipRecordProto.Builder, NamenodeMembershipRecordProtoOrBuilder>(
          NamenodeMembershipRecordProto.class);

  public MembershipStatePBImpl() {
  }

  @Override
  public NamenodeMembershipRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void deserialize(String data) throws IOException {
    byte[] bytes = FederationPBHelper.deserialize(data);
    NamenodeMembershipRecordProto proto =
        NamenodeMembershipRecordProto.newBuilder().mergeFrom(bytes).build();
    this.translator.setProto(proto);
  }

  @Override
  public void setRouterId(String routerId) {
    this.translator.getBuilder().setRouterId(routerId);
  }

  @Override
  public String getRouterId() {
    return this.translator.getProtoOrBuilder().getRouterId();
  }

  @Override
  public void setNameserviceId(String nameserviceId) {
    this.translator.getBuilder().setNameserviceId(nameserviceId);
  }

  @Override
  public void setNamenodeId(String namenodeId) {
    this.translator.getBuilder().setNamenodeId(namenodeId);
  }

  @Override
  public void setWebAddress(String webAddress) {
    this.translator.getBuilder().setWebAddress(webAddress);
  }

  @Override
  public void setRpcAddress(String rpcAddress) {
    this.translator.getBuilder().setRpcAddress(rpcAddress);
  }

  @Override
  public void setIsSafeMode(boolean isSafeMode) {
    this.translator.getBuilder().setIsSafeMode(isSafeMode);
  }

  @Override
  public void setClusterId(String clusterId) {
    this.translator.getBuilder().setClusterId(clusterId);
  }

  @Override
  public void setBlockPoolId(String blockPoolId) {
    this.translator.getBuilder().setBlockPoolId(blockPoolId);
  }

  @Override
  public void setState(FederationNamenodeServiceState state) {
    this.translator.getBuilder().setState(state.toString());
  }

  @Override
  public String getNameserviceId() {
    return this.translator.getProtoOrBuilder().getNameserviceId();
  }

  @Override
  public String getNamenodeId() {
    return this.translator.getProtoOrBuilder().getNamenodeId();
  }

  @Override
  public String getClusterId() {
    return this.translator.getProtoOrBuilder().getClusterId();
  }

  @Override
  public String getBlockPoolId() {
    return this.translator.getProtoOrBuilder().getBlockPoolId();
  }

  @Override
  public String getRpcAddress() {
    return this.translator.getProtoOrBuilder().getRpcAddress();
  }

  @Override
  public String getWebAddress() {
    return this.translator.getProtoOrBuilder().getWebAddress();
  }

  @Override
  public boolean getIsSafeMode() {
    return this.translator.getProtoOrBuilder().getIsSafeMode();
  }

  @Override
  public FederationNamenodeServiceState getState() {
    FederationNamenodeServiceState ret =
        FederationNamenodeServiceState.UNAVAILABLE;
    try {
      ret = FederationNamenodeServiceState.valueOf(
          this.translator.getProtoOrBuilder().getState());
    } catch (IllegalArgumentException e) {
      // Ignore this error
    }
    return ret;
  }

  @Override
  public void setTotalSpace(long space) {
    this.translator.getBuilder().setTotalSpace(space);
  }

  @Override
  public long getTotalSpace() {
    return this.translator.getProtoOrBuilder().getTotalSpace();
  }

  @Override
  public void setAvailableSpace(long space) {
    this.translator.getBuilder().setAvailableSpace(space);
  }

  @Override
  public long getAvailableSpace() {
    return this.translator.getProtoOrBuilder().getAvailableSpace();
  }

  @Override
  public void setNumOfFiles(long files) {
    this.translator.getBuilder().setNumOfFiles(files);
  }

  @Override
  public long getNumOfFiles() {
    return this.translator.getProtoOrBuilder().getNumOfFiles();
  }

  @Override
  public void setNumOfBlocks(long blocks) {
    this.translator.getBuilder().setNumOfBlocks(blocks);
  }

  @Override
  public long getNumOfBlocks() {
    return this.translator.getProtoOrBuilder().getNumOfBlocks();
  }

  @Override
  public void setNumOfBlocksMissing(long blocks) {
    this.translator.getBuilder().setNumOfBlocksMissing(blocks);
  }

  @Override
  public long getNumOfBlocksMissing() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksMissing();
  }

  @Override
  public void setNumOfBlocksPendingReplication(long blocks) {
    this.translator.getBuilder().setNumOfBlocksPendingReplication(blocks);
  }

  @Override
  public long getNumOfBlocksPendingReplication() {
    return this.translator.getProtoOrBuilder()
        .getNumOfBlocksPendingReplication();
  }

  @Override
  public void setNumOfBlocksUnderReplicated(long blocks) {
    this.translator.getBuilder().setNumOfBlocksUnderReplicated(blocks);
  }

  @Override
  public long getNumOfBlocksUnderReplicated() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksUnderReplicated();
  }

  @Override
  public void setNumOfBlocksPendingDeletion(long blocks) {
    this.translator.getBuilder().setNumOfBlocksPendingDeletion(blocks);
  }

  @Override
  public long getNumOfBlocksPendingDeletion() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksPendingDeletion();
  }

  @Override
  public void setNumberOfActiveDatanodes(int nodes) {
    this.translator.getBuilder().setNumberOfActiveDatanodes(nodes);
  }

  @Override
  public int getNumberOfActiveDatanodes() {
    return this.translator.getProtoOrBuilder().getNumberOfActiveDatanodes();
  }

  @Override
  public void setNumberOfDeadDatanodes(int nodes) {
    this.translator.getBuilder().setNumberOfDeadDatanodes(nodes);
  }

  @Override
  public int getNumberOfDeadDatanodes() {
    return this.translator.getProtoOrBuilder().getNumberOfDeadDatanodes();
  }

  @Override
  public void setNumberOfDecomDatanodes(int nodes) {
    this.translator.getBuilder().setNumberOfDecomDatanodes(nodes);
  }

  @Override
  public int getNumberOfDecomDatanodes() {
    return this.translator.getProtoOrBuilder().getNumberOfDecomDatanodes();
  }

  @Override
  public void setLastContact(long contact) {
    this.translator.getBuilder().setLastContact(contact);
  }

  @Override
  public long getLastContact() {
    return this.translator.getProtoOrBuilder().getLastContact();
  }

  @Override
  public void setDateModified(long time) {
    this.translator.getBuilder().setDateModified(time);
  }

  @Override
  public long getDateModified() {
    return this.translator.getProtoOrBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    this.translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return this.translator.getProtoOrBuilder().getDateCreated();
  }
}
