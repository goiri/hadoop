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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationsResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationsResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;

/**
 * Protobuf implementation of the state store API object
 * GetRouterRegistrationsResponse.
 */
public class GetRouterRegistrationsResponsePBImpl
    extends GetRouterRegistrationsResponse {

  private FederationProtocolPBTranslator<GetRouterRegistrationsResponseProto,
  GetRouterRegistrationsResponseProto.Builder,
  GetRouterRegistrationsResponseProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<GetRouterRegistrationsResponseProto,
      GetRouterRegistrationsResponseProto.Builder,
      GetRouterRegistrationsResponseProtoOrBuilder>(
          GetRouterRegistrationsResponseProto.class);

  public GetRouterRegistrationsResponsePBImpl() {

  }

  @Override
  public GetRouterRegistrationsResponseProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object protocol) {
    this.translator.setProto(protocol);
  }

  @Override
  public List<RouterState> getRouters() throws IOException {

    List<RouterState> ret = new ArrayList<RouterState>();
    List<RouterRecordProto> memberships =
        this.translator.getProtoOrBuilder().getRoutersList();
    for (RouterRecordProto memberProto : memberships) {
      RouterState membership =
          FederationProtocolFactory.newInstance(RouterState.class, memberProto);
      ret.add(membership);
    }
    return ret;
  }

  @Override
  public void setRouters(List<RouterState> records) throws IOException {

    this.translator.getBuilder().clearRouters();
    for (RouterState member : records) {
      this.translator.getBuilder()
          .addRouters((RouterRecordProto) member.getProto());
    }
  }

  @Override
  public long getTimestamp() {
    return this.translator.getProtoOrBuilder().getTimestamp();
  }

  @Override
  public void setTimestamp(long time) {
    this.translator.getBuilder().setTimestamp(time);
  }
}