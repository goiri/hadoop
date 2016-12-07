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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetMountTableEntriesRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetMountTableEntriesResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.GetPathStatsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.GetPathStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.GetPathStatsResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.ResetPerfCountersRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.ResetPerfCountersResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.RestartRpcServerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.RestartRpcServerResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.SetConfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.SetConfigurationResponseProto;
import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is used on the server side. Calls come across the wire for the for
 * protocol {@link RouterAdminProtocolPB}. This class translates the PB data
 * types to the native data types used inside the HDFS Router as specified in
 * the generic RouterAdminProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RouterAdminProtocolServerSideTranslatorPB implements
    RouterAdminProtocolPB {
  final private RouterAdminServer server;

  /**
   * Constructor.
   * @param server The NN server.
   * @throws IOException
   */
  public RouterAdminProtocolServerSideTranslatorPB(RouterAdminServer server)
      throws IOException {
    this.server = server;
  }

  @Override
  public AddMountTableEntryResponseProto addMountTableEntry(
      RpcController controller, AddMountTableEntryRequestProto request)
      throws ServiceException {

    try {
      AddMountTableEntryRequest req = FederationProtocolFactory
          .newInstance(AddMountTableEntryRequest.class, request);
      AddMountTableEntryResponse response = server.addMountTableEntry(req);
      return FederationPBHelper.convert(response);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Remove an entry from the mount table.
   */
  @Override
  public RemoveMountTableEntryResponseProto removeMountTableEntry(
      RpcController controller, RemoveMountTableEntryRequestProto request)
      throws ServiceException {
    try {
      RemoveMountTableEntryRequest req = FederationProtocolFactory
          .newInstance(RemoveMountTableEntryRequest.class, request);
      RemoveMountTableEntryResponse response =
          server.removeMountTableEntry(req);
      return FederationPBHelper.convert(response);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get matching mount table entries.
   */
  @Override
  public GetMountTableEntriesResponseProto getMountTableEntries(
      RpcController controller, GetMountTableEntriesRequestProto request)
          throws ServiceException {
    try {
      GetMountTableEntriesRequest req = FederationProtocolFactory
          .newInstance(GetMountTableEntriesRequest.class, request);
      GetMountTableEntriesResponse response = server.getMountTableEntries(req);
      return FederationPBHelper.convert(response);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Update a single mount table entry.
   */
  @Override
  public UpdateMountTableEntryResponseProto updateMountTableEntry(
      RpcController controller, UpdateMountTableEntryRequestProto request)
          throws ServiceException {
    try {
      UpdateMountTableEntryRequest req = FederationProtocolFactory
          .newInstance(UpdateMountTableEntryRequest.class, request);
      UpdateMountTableEntryResponse response =
          server.updateMountTableEntry(req);
      return FederationPBHelper.convert(response);
    } catch (IOException e) {
      throw new ServiceException(e);
    }

  }

  @Override
  public GetPathStatsResponseProto getPathStats(RpcController controller,
      GetPathStatsRequestProto request) throws ServiceException {
    try {
      long timeWindow = request.getTimeWindow();
      boolean max = request.getMax();
      PathTree<Long> pathStats = server.getPathStats(timeWindow, max);
      Builder builder = GetPathStatsResponseProto.newBuilder();
      builder.setPathStats(pathStats.toJSON());
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ResetPerfCountersResponseProto resetPerfCounters(
      RpcController controller, ResetPerfCountersRequestProto request)
          throws ServiceException {
    try {
      long flags = request.getFlags();
      server.resetPerfCounters(flags);
      return ResetPerfCountersResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RestartRpcServerResponseProto restartRpcServer(
      RpcController controller, RestartRpcServerRequestProto request)
          throws ServiceException {
    try {
      server.restartRpcServer();
      return RestartRpcServerResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetConfigurationResponseProto setConfiguration(
      RpcController controller, SetConfigurationRequestProto request)
          throws ServiceException {
    try {
      String key = request.getKey();
      String value = request.getValue();
      server.setConfiguration(key, value);
      return SetConfigurationResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
