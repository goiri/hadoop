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
package org.apache.hadoop.hdfs.server.federation.store;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;

/**
 * Utility wrapper around HDFS Federation state store API requests.
 */
public final class FederationRouterStateStoreUtils {

  private FederationRouterStateStoreUtils() {
    // Utility class
  }

  public static RouterState getRouterRegistration(
      FederationRouterStateStore store, String routerId) throws IOException {

    GetRouterRegistrationRequest request =
        FederationProtocolFactory.newInstance(
            GetRouterRegistrationRequest.class);
    request.setRouterId(routerId);
    GetRouterRegistrationResponse response =
        store.getRouterRegistration(request);
    return response.getRouter();
  }

  public static List<RouterState> getAllActiveRouters(
      FederationRouterStateStore store) throws IOException {

    GetRouterRegistrationsRequest request =
        FederationProtocolFactory.newInstance(
            GetRouterRegistrationsRequest.class);
    GetRouterRegistrationsResponse response =
        store.getRouterRegistrations(request);
    return response.getRouters();
  }


  public static boolean sendRouterHeartbeat(FederationRouterStateStore store,
      String routerId, RouterServiceState state, long dateStarted)
          throws IOException {

    RouterHeartbeatRequest request = FederationProtocolFactory.newInstance(
        RouterHeartbeatRequest.class);
    request.setDateStarted(dateStarted);
    request.setRouterId(routerId);
    request.setStatus(state);
    RouterHeartbeatResponse response;
    response = store.routerHeartbeat(request);
    return response.getStatus();
  }
}
