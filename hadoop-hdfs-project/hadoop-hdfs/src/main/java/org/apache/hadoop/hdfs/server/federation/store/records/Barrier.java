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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;

/**
 * Represents an generic acknowledgement of a specific record. A client will add
 * this record to the data store when acknowledging the processing of a specific
 * data record from the state store.
 */
public abstract class Barrier extends BaseRecord {

  /** Expiration time in ms for this entry. */
  private static long expirationMs;
  /** Identifier for the actual barier node. */
  public static final String BARRIER_IDENTIFIER = "BarrierReady";

  public Barrier() {
    super();
  }

  public static Barrier newInstance(String record, String client)
      throws IOException {
    if (record == null || client == null) {
      throw new IOException(
          "Cannot create new instance of barrier record,"
          + " null parameters are not allowed.");
    }
    Barrier obj = FederationProtocolFactory.newInstance(Barrier.class);
    obj.setRecordIdentifier(record);
    obj.setClientIdentifier(client);
    return obj;
  }

  public abstract void setRecordIdentifier(String recordIdentifier);

  public abstract void setClientIdentifier(String clientIdentifier);

  public abstract String getRecordIdentifier();

  public abstract String getClientIdentifier();

  public static String constructPrimaryKey(String recordId, String client) {
    SortedMap<String, String> keyMap = new TreeMap<String, String>();
    keyMap.put("recordIdentifier", recordId);
    keyMap.put("clientIdentifier", client);
    return generateMashupKey(keyMap);
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> keyMap = new TreeMap<String, String>();
    keyMap.put("recordIdentifier", this.getRecordIdentifier());
    keyMap.put("clientIdentifier", this.getClientIdentifier());
    return keyMap;
  }

  @Override
  public long getExpirationMs() {
    return Barrier.expirationMs;
  }

  public static void setExpirationMs(long time) {
    Barrier.expirationMs = time;
  }

}
