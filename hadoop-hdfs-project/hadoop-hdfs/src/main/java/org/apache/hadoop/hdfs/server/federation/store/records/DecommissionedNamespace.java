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
 * Data record indicating a specific nameservice ID has been decommissioned and
 * is no longer valid. Allows for quick decommissioning and disabling of
 * nameservices.
 */
public abstract class DecommissionedNamespace extends BaseRecord {

  public DecommissionedNamespace() {
    super();
  }

  /**
   * Get the identifier of the nameservice to decommission.
   *
   * @return Identifier of the nameservice to decommission.
   */
  public abstract String getNameserviceId();

  /**
   * Set the identifier of the nameservice to decommission.
   *
   * @param nameServiceId Identifier of the nameservice to decommission.
   */
  public abstract void setNameserviceId(String nameServiceId);

  /**
   * Create a new instance of this record.
   * @param nsId Nameservice identifier.
   * @return
   * @throws IOException
   */
  public static DecommissionedNamespace newInstance(String nsId)
      throws IOException {
    DecommissionedNamespace record =
        FederationProtocolFactory.newInstance(DecommissionedNamespace.class);
    record.setNameserviceId(nsId);
    return record;
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> keyMap = new TreeMap<String, String>();
    keyMap.put("nameServiceId", this.getNameserviceId());
    return keyMap;
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }
}
