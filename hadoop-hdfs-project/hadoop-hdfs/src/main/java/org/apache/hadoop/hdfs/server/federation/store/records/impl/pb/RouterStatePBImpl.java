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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;

/**
 * Protobuf implementation of the RouterState record.
 */
public class RouterStatePBImpl extends RouterState {

  private FederationProtocolPBTranslator<RouterRecordProto, RouterRecordProto.Builder, RouterRecordProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<RouterRecordProto, RouterRecordProto.Builder, RouterRecordProtoOrBuilder>(
          RouterRecordProto.class);

  public RouterStatePBImpl() {
  }

  public RouterStatePBImpl(RouterRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public RouterRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void deserialize(String data) throws IOException {
    byte[] bytes = FederationPBHelper.deserialize(data);
    RouterRecordProto proto =
        RouterRecordProto.newBuilder().mergeFrom(bytes).build();
    this.translator.setProto(proto);
  }

  @Override
  public long getRegistrationTableVersion() {
    // deprecated
    return 0;
  }

  @Override
  public void setRegistrationTableVersion(long registrationTableVersion) {
    // deprecated
  }

  @Override
  public void setAddress(String address) {
    this.translator.getBuilder().setAddress(address);
  }

  @Override
  public void setDateStarted(long dateStarted) {
    this.translator.getBuilder().setDateStarted(dateStarted);
  }

  @Override
  public String getAddress() {
    return this.translator.getProtoOrBuilder().getAddress();
  }

  @Override
  public long getPathLockVersion() {
    return this.translator.getProtoOrBuilder().getPathLockVersion();
  }

  @Override
  public void setPathLockVersion(long version) {
    this.translator.getBuilder().setPathLockVersion(version);
  }

  @Override
  public long getFileResolverVersion() {
    return this.translator.getProtoOrBuilder().getFileResolverVersion();
  }

  @Override
  public void setFileResolverVersion(long version) {
    this.translator.getBuilder().setFileResolverVersion(version);
  }

  @Override
  public RouterServiceState getStatus() {
    return RouterServiceState
        .valueOf(this.translator.getProtoOrBuilder().getStatus());
  }

  @Override
  public void setStatus(RouterServiceState newStatus) {
    this.translator.getBuilder().setStatus(newStatus.toString());
  }

  @Override
  public String getBuildVersion() {
    return this.translator.getProtoOrBuilder().getBuildVersion();
  }

  @Override
  public void setBuildVersion(String version) {
    this.translator.getBuilder().setBuildVersion(version);
  }

  @Override
  public String getCompileInfo() {
    return this.translator.getProtoOrBuilder().getCompileInfo();
  }

  @Override
  public void setCompileInfo(String info) {
    this.translator.getBuilder().setCompileInfo(info);
  }

  @Override
  public long getDateStarted() {
    return this.translator.getProtoOrBuilder().getDateStarted();
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
