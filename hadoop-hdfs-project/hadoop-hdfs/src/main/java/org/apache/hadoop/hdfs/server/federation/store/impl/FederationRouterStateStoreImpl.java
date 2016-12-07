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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.FederationRouterStateStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;

/**
 * Implementation of the {@link FederationRouterStateStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationRouterStateStoreImpl
    extends FederationStateStoreInterface
    implements FederationRouterStateStore {

  @Override
  public void init() {
  }

  /////////////////////////////////////////////////////////
  // Router interface
  /////////////////////////////////////////////////////////

  private Map<String, String> queryForRouter(String routerId) {
    Map<String, String> query = new HashMap<String, String>();
    query.put("address", routerId);
    return query;
  }

  @Override
  public GetRouterRegistrationResponse getRouterRegistration(
      GetRouterRegistrationRequest request) throws IOException {

    String routerId = request.getRouterId();
    Map<String, String> query = queryForRouter(routerId);
    RouterState record = getDriver().get(RouterState.class, query);
    if (record != null) {
      FederationStateStoreUtils.overrideExpiredRecord(
          getDriver(), record, RouterState.class);
    }
    GetRouterRegistrationResponse response =
        FederationProtocolFactory.newInstance(
            GetRouterRegistrationResponse.class);
    response.setRouter(record);
    return response;
  }

  @Override
  public GetRouterRegistrationsResponse getRouterRegistrations(
      GetRouterRegistrationsRequest request)
          throws IOException {

    QueryResult<RouterState> result = getDriver().get(RouterState.class);
    FederationStateStoreUtils.overrideExpiredRecords(
        getDriver(), result,  RouterState.class);
    GetRouterRegistrationsResponse response =
        FederationProtocolFactory.newInstance(
            GetRouterRegistrationsResponse.class);
    response.setRouters(result.getRecords());
    response.setTimestamp(result.getTimestamp());
    return response;
  }

  @Override
  public RouterHeartbeatResponse routerHeartbeat(RouterHeartbeatRequest request)
      throws IOException {

    RouterState record = RouterState.newInstance(
        request.getRouterId(),
        request.getDateStarted(),
        request.getStatus());
    boolean status = getDriver().updateOrCreate(record, true, false);
    RouterHeartbeatResponse response =
        FederationProtocolFactory.newInstance(
            RouterHeartbeatResponse.class);
    response.setStatus(status);
    return response;
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return RouterState.class;
  }
}
