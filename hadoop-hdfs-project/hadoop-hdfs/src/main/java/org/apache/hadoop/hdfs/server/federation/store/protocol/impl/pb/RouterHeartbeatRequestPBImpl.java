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
package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterHeartbeatRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;

/**
 * Protobuf implementation of the state store API object
 * RouterHeartbeatRequest.
 */
public class RouterHeartbeatRequestPBImpl extends RouterHeartbeatRequest {

  private FederationProtocolPBTranslator<RouterHeartbeatRequestProto,
  RouterHeartbeatRequestProto.Builder,
  RouterHeartbeatRequestProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<RouterHeartbeatRequestProto,
      RouterHeartbeatRequestProto.Builder,
      RouterHeartbeatRequestProtoOrBuilder>(
          RouterHeartbeatRequestProto.class);

  public RouterHeartbeatRequestPBImpl() {
  }

  @Override
  public RouterHeartbeatRequestProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object protocol) {
    this.translator.setProto(protocol);
  }

  @Override
  public String getRouterId() {
    return this.translator.getProtoOrBuilder().getRouterId();
  }

  @Override
  public void setRouterId(String id) {
    this.translator.getBuilder().setRouterId(id);
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
  public long getDateStarted() {
    return this.translator.getProtoOrBuilder().getDateStarted();
  }

  @Override
  public void setDateStarted(long time) {
    this.translator.getBuilder().setDateStarted(time);
  }
}
