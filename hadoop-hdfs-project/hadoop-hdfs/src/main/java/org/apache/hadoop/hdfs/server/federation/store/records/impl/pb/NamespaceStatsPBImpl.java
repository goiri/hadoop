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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamespaceStatsRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamespaceStatsRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.NamespaceStats;

/**
 * Protobuf implementation of the NamespaceStats record.
 */
public class NamespaceStatsPBImpl extends NamespaceStats {

  private FederationProtocolPBTranslator<NamespaceStatsRecordProto, NamespaceStatsRecordProto.Builder, NamespaceStatsRecordProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<NamespaceStatsRecordProto, NamespaceStatsRecordProto.Builder, NamespaceStatsRecordProtoOrBuilder>(
          NamespaceStatsRecordProto.class);

  public NamespaceStatsPBImpl() {
  }

  public NamespaceStatsPBImpl(NamespaceStatsRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public NamespaceStatsRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Object proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void deserialize(String data) throws IOException {
    byte[] bytes = FederationPBHelper.deserialize(data);
    NamespaceStatsRecordProto proto =
        NamespaceStatsRecordProto.newBuilder().mergeFrom(bytes).build();
    this.translator.setProto(proto);
  }

  @Override
  public String getRouterId() {
    return this.translator.getProtoOrBuilder().getRouterId();
  }

  @Override
  public void setRouterId(String routerId) {
    this.translator.getBuilder().setRouterId(routerId);
  }

  @Override
  public long getTimeWindowStart() {
    return this.translator.getProtoOrBuilder().getTimeWindowStart();
  }

  @Override
  public void setTimeWindowStart(long timeWindowStart) {
    this.translator.getBuilder().setTimeWindowStart(timeWindowStart);
  }

  @Override
  public String getStats() {
    return this.translator.getProtoOrBuilder().getStats();
  }

  @Override
  public void setStats(String stats) {
    this.translator.getBuilder().setStats(stats);
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
