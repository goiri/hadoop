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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper for a key value cache.
 * @param <T> Type of the value stored.
 */
public class KeyValueCache<T> {

  private ConcurrentHashMap<String, T> recordMap;
  private boolean isInitialized = false;

  public KeyValueCache() {
    this.recordMap = new ConcurrentHashMap<String, T>();
  }

  public boolean isInitialized() {
    return this.isInitialized;
  }

  public void setInitialized() {
    this.isInitialized = true;
  }

  public Collection<T> getValues() {
    return recordMap.values();
  }

  public Set<String> getKeys() {
    return recordMap.keySet();
  }

  public void add(String key, T record) {
    recordMap.put(key, record);
  }

  public T get(String key) {
    return recordMap.get(key);
  }

  public void remove(String key) {
    recordMap.remove(key);
  }

  public void clear() {
    recordMap.clear();
  }
}
