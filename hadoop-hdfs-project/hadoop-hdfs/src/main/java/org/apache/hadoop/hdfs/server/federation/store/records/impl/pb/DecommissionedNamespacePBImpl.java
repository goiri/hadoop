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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.DecommissionedNamespaceRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.DecommissionedNamespaceRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.DecommissionedNamespace;

/**
 * Protobuf implementation of the DecommissionedNamespace record.
 */
public class DecommissionedNamespacePBImpl extends DecommissionedNamespace {

  private FederationProtocolPBTranslator<DecommissionedNamespaceRecordProto, DecommissionedNamespaceRecordProto.Builder, DecommissionedNamespaceRecordProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<DecommissionedNamespaceRecordProto, DecommissionedNamespaceRecordProto.Builder, DecommissionedNamespaceRecordProtoOrBuilder>(
          DecommissionedNamespaceRecordProto.class);

  public DecommissionedNamespacePBImpl() {
  }

  public DecommissionedNamespacePBImpl(
      DecommissionedNamespaceRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public DecommissionedNamespaceRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object proto) {
    this.translator.setProto(proto);
  }

  @Override
  public String getNameserviceId() {
    return this.translator.getProtoOrBuilder().getNameServiceId();
  }

  @Override
  public void setNameserviceId(String nameServiceId) {
    this.translator.getBuilder().setNameServiceId(nameServiceId);
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

  @Override
  public void deserialize(String data) throws IOException {
    byte[] bytes = FederationPBHelper.deserialize(data);
    DecommissionedNamespaceRecordProto proto =
        DecommissionedNamespaceRecordProto.newBuilder().
        mergeFrom(bytes).build();
    this.translator.setProto(proto);
  }
}
