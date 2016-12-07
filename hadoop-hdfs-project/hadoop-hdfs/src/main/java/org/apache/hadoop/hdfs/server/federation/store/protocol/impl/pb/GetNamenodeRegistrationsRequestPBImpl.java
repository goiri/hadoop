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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetNamenodeRegistrationsRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetNamenodeRegistrationsRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.QueryPairProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;

/**
 * Protobuf implementation of the state store API object
 * GetNamenodeRegistrationsRequest.
 */
public class GetNamenodeRegistrationsRequestPBImpl
    extends GetNamenodeRegistrationsRequest {

  private FederationProtocolPBTranslator<GetNamenodeRegistrationsRequestProto,
  GetNamenodeRegistrationsRequestProto.Builder,
  GetNamenodeRegistrationsRequestProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<GetNamenodeRegistrationsRequestProto,
      GetNamenodeRegistrationsRequestProto.Builder,
      GetNamenodeRegistrationsRequestProtoOrBuilder>(
          GetNamenodeRegistrationsRequestProto.class);

  public GetNamenodeRegistrationsRequestPBImpl() {
  }

  public GetNamenodeRegistrationsRequestPBImpl(
      GetNamenodeRegistrationsRequestProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public Map<String, String> getQuery() {

    List<QueryPairProto> list =
        this.translator.getProtoOrBuilder().getQueryList();
    Map<String, String> ret = new HashMap<String, String>();
    for (QueryPairProto item : list) {
      ret.put(item.getKey(), item.getValue());
    }
    return ret;
  }

  @Override
  public void setQuery(Map<String, String> query) {
    if (query != null) {
      for (Entry<String, String> entry : query.entrySet()) {
        QueryPairProto.Builder pairBuilder = QueryPairProto.newBuilder();
        pairBuilder.setKey(entry.getKey());
        pairBuilder.setValue(entry.getValue());
        this.translator.getBuilder().addQuery(pairBuilder.build());
      }
    }
  }

  @Override
  public GetNamenodeRegistrationsRequestProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object protocol) {
    this.translator.setProto(protocol);
  }
}
