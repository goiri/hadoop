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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.PathLockRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.PathLockRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.PathLock;

/**
 * Protobuf implementation of the PathLock record.
 */
public class PathLockPBImpl extends PathLock {

  private FederationProtocolPBTranslator<PathLockRecordProto, PathLockRecordProto.Builder, PathLockRecordProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<PathLockRecordProto, PathLockRecordProto.Builder, PathLockRecordProtoOrBuilder>(
          PathLockRecordProto.class);

  public PathLockPBImpl() {
  }

  public PathLockPBImpl(PathLockRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public PathLockRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void deserialize(String data) throws IOException {
    byte[] bytes = FederationPBHelper.deserialize(data);
    PathLockRecordProto proto =
        PathLockRecordProto.newBuilder().mergeFrom(bytes).build();
    this.translator.setProto(proto);
  }

  @Override
  public void setSourcePath(String sourcePath) {
    this.translator.getBuilder().setSourcePath(sourcePath);
  }

  @Override
  public void setLockClient(String lockClient) {
    this.translator.getBuilder().setLockClient(lockClient);
  }

  @Override
  public String getSourcePath() {
    return this.translator.getProtoOrBuilder().getSourcePath();
  }

  @Override
  public String getLockClient() {
    return this.translator.getProtoOrBuilder().getLockClient();
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
