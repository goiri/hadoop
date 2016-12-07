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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetMountTableEntriesRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetMountTableEntriesResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolBase;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;

import com.google.protobuf.GeneratedMessage;

/**
 * Helper class for creating, inflating and deflating record records and API
 * elements using protobuf.
 */
public final class FederationPBHelper {

  private FederationPBHelper() {
    // Utility class
  }

  /**
   * Serializes a protobuf message into a base64 string.
   *
   * @param message Populated protobuf message.
   * @return Base64 encoding of a protobuf message.
   */
  public static String serialize(FederationProtocolBase obj) {
    GeneratedMessage message = (GeneratedMessage) obj.getProto();
    byte[] byteArray = message.toByteArray();
    // Encode without CRLF
    byte[] byteArray64 = Base64.encodeBase64(byteArray, false);
    String base64Encoded = StringUtils.newStringUtf8(byteArray64);
    return base64Encoded;
  }

  /**
   * Transforms a base64 serialized string into a byte stream that can be used
   * to create a protobuf-backed record.
   *
   * @param base64String Base64 representation of a serialized protobuf message.
   * @return byte stream compatible with protobuf.
   * @throws IOException
   */
  public static byte[] deserialize(String base64String) throws IOException {
    return Base64.decodeBase64(base64String);
  }

  /**
   * Get the name of the protobuf implementation.
   *
   * @param clazz Original class backed by a PB implementation.
   * @return Name of the class.
   */
  public static String getClassName(Class<?> clazz) {
    String className = clazz.getCanonicalName();
    String simpleName = clazz.getSimpleName();
    className = className.replaceAll(simpleName,
        "impl.pb." + clazz.getSimpleName() + "PBImpl");
    return className;
  }

  //
  // MountTable conversions used by router admin translator.
  //
  public static GetMountTableEntriesResponseProto convert(
      GetMountTableEntriesResponse response) {

    if (response instanceof GetMountTableEntriesResponsePBImpl) {
      return (GetMountTableEntriesResponseProto) response.getProto();
    }
    return null;
  }

  public static GetMountTableEntriesRequestProto convert(
      GetMountTableEntriesRequest request) {

    if (request instanceof GetMountTableEntriesRequestPBImpl) {
      return (GetMountTableEntriesRequestProto) request.getProto();
    }
    return null;
  }

  public static AddMountTableEntryResponseProto convert(
      AddMountTableEntryResponse response) {

    if (response instanceof AddMountTableEntryResponsePBImpl) {
      return (AddMountTableEntryResponseProto) response.getProto();
    }
    return null;
  }

  public static AddMountTableEntryRequestProto convert(
      AddMountTableEntryRequest request) {

    if (request instanceof AddMountTableEntryRequestPBImpl) {
      return (AddMountTableEntryRequestProto) request.getProto();
    }
    return null;
  }

  public static UpdateMountTableEntryResponseProto convert(
      UpdateMountTableEntryResponse response) {
    if (response instanceof UpdateMountTableEntryResponsePBImpl) {
      return (UpdateMountTableEntryResponseProto) response.getProto();
    }
    return null;
  }

  public static RemoveMountTableEntryRequestProto convert(
      RemoveMountTableEntryRequest request) {

    if (request instanceof RemoveMountTableEntryRequestPBImpl) {
      return (RemoveMountTableEntryRequestProto) request.getProto();
    }
    return null;
  }

  public static RemoveMountTableEntryResponseProto convert(
      RemoveMountTableEntryResponse response) {

    if (response instanceof RemoveMountTableEntryResponsePBImpl) {
      return (RemoveMountTableEntryResponseProto) response.getProto();
    }
    return null;
  }

  public static UpdateMountTableEntryRequestProto convert(
      UpdateMountTableEntryRequest request) {

    if (request instanceof UpdateMountTableEntryRequestPBImpl) {
      return (UpdateMountTableEntryRequestProto) request.getProto();
    }
    return null;
  }
}
