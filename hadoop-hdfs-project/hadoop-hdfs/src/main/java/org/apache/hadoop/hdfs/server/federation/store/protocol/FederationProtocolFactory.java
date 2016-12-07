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
package org.apache.hadoop.hdfs.server.federation.store.protocol;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationPBHelper;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

/**
 * Helper class that constructs the serialization protocol (i.e. protobuf) in
 * use for data storage and data transmission of all objects.
 */
public final class FederationProtocolFactory {

  private FederationProtocolFactory() {
    // Utility class
  }

  /**
   * Serializes a record into a base64 string.
   *
   * @param obj FederationProtocolBase object that can be serialized.
   * @return A base64 string representation of the object.
   */
  public static String serialize(FederationProtocolBase obj) {
    return FederationPBHelper.serialize(obj);
  }

  /**
   * Creates a new record and populates it with data from a protocol object.
   *
   * @param clazz The class of the record to create.
   * @param protocol Serialized data to use to populate the record.
   * @return A new record of type clazz populated with the deserialized data.
   * @throws IOException If the record cannot be created.
   */
  public static <T extends FederationProtocolBase> T newInstance(
      Class<T> clazz, Object protocol) throws IOException {
    T record = newInstance(clazz);
    record.setProto(protocol);
    return record;
  }

  /**
   * Create a new empty protocol/API object.
   *
   * @param clazz The protocol/API object type to create.
   * @return A empty protocol/API object of type clazz.
   * @throws IOException If the record cannot be created.
   */
  public static <T extends FederationProtocolBase> T newInstance(Class<T> clazz)
      throws IOException {

    String className = FederationPBHelper.getClassName(clazz);
    try {
      @SuppressWarnings("unchecked")
      Class<T> c = (Class<T>) Class.forName(className);
      T newObject = c.newInstance();
      newObject.initialize();
      return newObject;
    } catch (Exception e) {
      throw new IOException("Failed to create PBimpl " + clazz.getName()
          + " exception " + e.getMessage());
    }
  }

  /**
   * Get the abstract superclass for a specific data record implementation.
   *
   * @param record An instance of a data record backed by a serialization
   *          implementation.
   * @return The abstract superclass for the record.
   */
  @SuppressWarnings("unchecked")
  public static <T extends BaseRecord> Class<T> getReferenceClass(T record) {
    return (Class<T>) record.getClass().getGenericSuperclass();
  }
}
