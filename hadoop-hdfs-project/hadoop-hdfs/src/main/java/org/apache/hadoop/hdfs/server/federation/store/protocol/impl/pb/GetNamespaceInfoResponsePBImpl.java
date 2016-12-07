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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.FederationNamespaceInfoProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetNamespaceInfoResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetNamespaceInfoResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;

/**
 * Protobuf implementation of the state store API object
 * GetNamespaceInfoResponse.
 */
public class GetNamespaceInfoResponsePBImpl
    extends GetNamespaceInfoResponse {

  private FederationProtocolPBTranslator<GetNamespaceInfoResponseProto,
  GetNamespaceInfoResponseProto.Builder,
  GetNamespaceInfoResponseProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<GetNamespaceInfoResponseProto,
      GetNamespaceInfoResponseProto.Builder,
      GetNamespaceInfoResponseProtoOrBuilder>(
          GetNamespaceInfoResponseProto.class);

  public GetNamespaceInfoResponsePBImpl() {
  }

  @Override
  public GetNamespaceInfoResponseProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object protocol) {
    this.translator.setProto(protocol);
  }

  @Override
  public Set<FederationNamespaceInfo> getNamespaceInfo() {

    Set<FederationNamespaceInfo> ret = new HashSet<FederationNamespaceInfo>();
    List<FederationNamespaceInfoProto> namespaceList =
        this.translator.getProtoOrBuilder().getNamespaceInfosList();
    for(FederationNamespaceInfoProto item: namespaceList) {
      FederationNamespaceInfo info = new FederationNamespaceInfo(
          item.getBlockPoolId(), item.getClusterId(), item.getNameserviceId());
      ret.add(info);
    }
    return ret;
  }

  @Override
  public void setNamespaceInfo(Set<FederationNamespaceInfo> namespaceInfo) {
    int index = 0;
    for (FederationNamespaceInfo item : namespaceInfo) {
      FederationNamespaceInfoProto.Builder itemBuilder =
          FederationNamespaceInfoProto.newBuilder();
      itemBuilder.setClusterId(item.getClusterId());
      itemBuilder.setBlockPoolId(item.getBlockPoolId());
      itemBuilder.setNameserviceId(item.getNameserviceId());
      this.translator.getBuilder().addNamespaceInfos(index,
          itemBuilder.build());
      index++;
    }
  }
}