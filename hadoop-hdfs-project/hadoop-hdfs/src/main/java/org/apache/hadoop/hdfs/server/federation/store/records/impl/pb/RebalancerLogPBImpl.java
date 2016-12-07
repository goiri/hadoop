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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RebalancerRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RebalancerRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog;

/**
 * Protobuf implementation of the RebalancerLog record.
 */
public class RebalancerLogPBImpl extends RebalancerLog {

  private FederationProtocolPBTranslator<RebalancerRecordProto, RebalancerRecordProto.Builder, RebalancerRecordProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<RebalancerRecordProto, RebalancerRecordProto.Builder, RebalancerRecordProtoOrBuilder>(
          RebalancerRecordProto.class);

  public RebalancerLogPBImpl() {
  }

  public RebalancerLogPBImpl(RebalancerRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public RebalancerRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void deserialize(String data) throws IOException {
    byte[] bytes = FederationPBHelper.deserialize(data);
    RebalancerRecordProto proto =
        RebalancerRecordProto.newBuilder().mergeFrom(bytes).build();
    this.translator.setProto(proto);
  }

  @Override
  public String getId() {
    return this.translator.getProtoOrBuilder().getId();
  }

  @Override
  public void setId(String value) {
    this.translator.getBuilder().setId(value);
  }

  @Override
  public RebalancerServiceState getState() {
    return RebalancerServiceState
        .valueOf(this.translator.getProtoOrBuilder().getState());
  }

  @Override
  public void setState(RebalancerServiceState newState) {
    this.translator.getBuilder().setState(newState.toString());
  }

  @Override
  public RebalancerOperationStatus getOperationStatus() {
    return RebalancerOperationStatus
        .valueOf(this.translator.getProtoOrBuilder().getOperationStatus());
  }

  @Override
  public void setOperationStatus(RebalancerOperationStatus newStatus) {
    this.translator.getBuilder().setOperationStatus(newStatus.toString());
  }

  @Override
  public String getMount() {
    return this.translator.getProtoOrBuilder().getMount();
  }

  @Override
  public void setMount(String newMount) {
    this.translator.getBuilder().setMount(newMount);
  }

  @Override
  public String getNameserviceId() {
    return this.translator.getProtoOrBuilder().getNameserviceId();
  }

  @Override
  public void setNameserviceId(String id) {
    this.translator.getBuilder().setNameserviceId(id);
  }

  @Override
  public String getDstPath() {
    return this.translator.getProtoOrBuilder().getDstPath();
  }

  @Override
  public void setDstPath(String path) {
    this.translator.getBuilder().setDstPath(path);
  }

  @Override
  public String getClientId() {
    return this.translator.getProtoOrBuilder().getClientId();
  }

  @Override
  public void setClientId(String client) {
    this.translator.getBuilder().setClientId(client);
  }

  @Override
  public long getDateReserved() {
    return this.translator.getProtoOrBuilder().getDateReserved();
  }

  @Override
  public void setDateReserved(long date) {
    this.translator.getBuilder().setDateReserved(date);
  }

  @Override
  public String getOperationResult() {
    return this.translator.getProtoOrBuilder().getOperationResult();
  }

  @Override
  public void setOperationResult(String result) {
    this.translator.getBuilder().setOperationResult(result);
  }

  @Override
  public String getJobId() {
    return this.translator.getProtoOrBuilder().getJobId();
  }

  @Override
  public void setJobId(String job) {
    this.translator.getBuilder().setJobId(job);
  }

  @Override
  public String getTrackingUrl() {
    return this.translator.getProtoOrBuilder().getTrackingUrl();
  }

  @Override
  public void setTrackingUrl(String url) {
    this.translator.getBuilder().setTrackingUrl(url);
  }

  @Override
  public float getProgress() {
    return this.translator.getProtoOrBuilder().getProgress();
  }

  @Override
  public void setProgress(float p) {
    this.translator.getBuilder().setProgress(p);
  }

  @Override
  public long getDateStateUpdated() {
    return this.translator.getProtoOrBuilder().getDateStateUpdated();
  }

  @Override
  public void setDateStateUpdated(long date) {
    this.translator.getBuilder().setDateStateUpdated(date);
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
