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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;

/**
 * Protobuf implementation of the state store API object
 * UpdateMountTableEntryRequest.
 */
public class UpdateMountTableEntryRequestPBImpl
    extends UpdateMountTableEntryRequest {

  private FederationProtocolPBTranslator<UpdateMountTableEntryRequestProto,
  UpdateMountTableEntryRequestProto.Builder,
  UpdateMountTableEntryRequestProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<UpdateMountTableEntryRequestProto,
      UpdateMountTableEntryRequestProto.Builder,
      UpdateMountTableEntryRequestProtoOrBuilder>(
          UpdateMountTableEntryRequestProto.class);

  public UpdateMountTableEntryRequestPBImpl() {
  }

  @Override
  public UpdateMountTableEntryRequestProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object protocol) {
    this.translator.setProto(protocol);
  }

  @Override
  public MountTable getEntry() throws IOException {
    return FederationProtocolFactory.newInstance(MountTable.class,
        this.translator.getProtoOrBuilder().getEntry());
  }

  @Override
  public void setEntry(MountTable mount) throws IOException {
    this.translator.getBuilder()
        .setEntry((MountTableRecordProto) mount.getProto());
  }
}