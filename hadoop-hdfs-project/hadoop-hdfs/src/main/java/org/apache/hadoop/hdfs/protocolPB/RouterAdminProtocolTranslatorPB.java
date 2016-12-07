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

import java.io.Closeable;
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
import org.apache.hadoop.hdfs.protocol.RouterAdminProtocol;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.GetPathStatsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.ResetPerfCountersRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.RestartRpcServerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.SetConfigurationRequestProto;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
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
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import com.google.protobuf.ServiceException;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to the
 * new PB types.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RouterAdminProtocolTranslatorPB
    implements ProtocolMetaInterface, RouterAdminProtocol, Closeable,
    ProtocolTranslator, FederationMountTableStore {
  final private RouterAdminProtocolPB rpcProxy;

  public RouterAdminProtocolTranslatorPB(RouterAdminProtocolPB proxy) {
    rpcProxy = proxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        RouterAdminProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(RouterAdminProtocolPB.class), methodName);
  }

  @Override
  public PathTree<Long> getPathStats(long timeWindow, boolean max) throws IOException {
    GetPathStatsRequestProto.Builder req =
        GetPathStatsRequestProto.newBuilder();
    req.setTimeWindow(timeWindow);
    req.setMax(max);
    try {
      String jsonPathStats =
          rpcProxy.getPathStats(null, req.build()).getPathStats();
      PathTree<Long> tree = new PathTree<Long>(jsonPathStats);
      return tree;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void resetPerfCounters(long flags) throws IOException {
    ResetPerfCountersRequestProto.Builder req =
        ResetPerfCountersRequestProto.newBuilder();
    req.setFlags(flags);
    try {
      rpcProxy.resetPerfCounters(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setConfiguration(String key, String value) throws IOException {
    SetConfigurationRequestProto.Builder req =
        SetConfigurationRequestProto.newBuilder();
    req.setKey(key);
    req.setValue(value);
    try {
      rpcProxy.setConfiguration(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void restartRpcServer() throws IOException {
    RestartRpcServerRequestProto.Builder req =
        RestartRpcServerRequestProto.newBuilder();
    try {
      rpcProxy.restartRpcServer(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request)
          throws StateStoreUnavailableException, IOException {
    AddMountTableEntryRequestProto proto = FederationPBHelper.convert(request);
    try {
      AddMountTableEntryResponseProto response =
          rpcProxy.addMountTableEntry(null, proto);
      return FederationProtocolFactory
          .newInstance(AddMountTableEntryResponse.class, response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request)
          throws StateStoreUnavailableException, IOException {
    UpdateMountTableEntryRequestProto proto =
        FederationPBHelper.convert(request);
    try {
      UpdateMountTableEntryResponseProto response =
          rpcProxy.updateMountTableEntry(null, proto);
      return FederationProtocolFactory
          .newInstance(UpdateMountTableEntryResponse.class, response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request)
          throws StateStoreUnavailableException, IOException {
    RemoveMountTableEntryRequestProto proto =
        FederationPBHelper.convert(request);
    try {
      RemoveMountTableEntryResponseProto response =
          rpcProxy.removeMountTableEntry(null, proto);
      return FederationProtocolFactory
          .newInstance(RemoveMountTableEntryResponse.class, response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request)
          throws StateStoreUnavailableException, IOException {

    GetMountTableEntriesRequestProto proto =
        FederationPBHelper.convert(request);
    try {
      GetMountTableEntriesResponseProto response =
          rpcProxy.getMountTableEntries(null, proto);
      return FederationProtocolFactory
          .newInstance(GetMountTableEntriesResponse.class,
          response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }
}
