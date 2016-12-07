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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;

/**
 * Set of utility functions used to query, create, update and delete data
 * records in the state store.
 */
public final class FederationStateStoreUtils {

  private static final Log LOG =
      LogFactory.getLog(FederationStateStoreUtils.class);

  private FederationStateStoreUtils() {
    // Utility class
  }

  /**
   * Filters a list of records to find all records matching the query.
   *
   * @param query Map of field names and objects to use to filter results.
   * @param records List of data records to filter.
   * @return List of all records matching the query (or empty list if none
   *         match), null if the data set could not be filtered.
   */
  public static <T extends BaseRecord> List<T> filterMultiple(
      Map<String, String> query, Iterable<T> records) {

    List<T> matchingList = new ArrayList<T>();
    try {
      for (T record : records) {
        boolean matches = true;
        if (query != null) {
          for (Entry<String, String> keyValue : query.entrySet()) {
            String key = keyValue.getKey();
            String value = keyValue.getValue();
            Object fieldData = record.getField(key);
            if(fieldData == null) {
              throw new IllegalArgumentException(
                  "Field was not found on object " + key);
            }
            // Compare string values
            String data = serialize(fieldData, String.class);
            if (!data.equals(value)) {
              matches = false;
              break;
            }
          }
        }
        if (matches) {
          matchingList.add(record);
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to parse list for query: " + query, e);
      return null;
    }
    return matchingList;
  }

  /**
   * Filters a list of records to find a single record matching the query.
   *
   * @param query Map of field names and objects to filter results.
   * @param records List of records to filter.
   * @return A single matching record in the input list. Null if there are no
   *         matching records or more than one machine record in the input
   *         list.
   */
  public static BaseRecord filterSingle(Map<String, String> query,
      Iterable<? extends BaseRecord> records) {

    List<? extends BaseRecord> results = filterMultiple(query, records);
    if (results != null && results.size() == 1) {
      return results.get(0);
    } else {
      return null;
    }
  }

  /**
   * Updates the state store with any record overrides we detected, such as an
   * expired state.
   *
   * @param query RecordQueryResult containing the data to be inspected.
   * @param clazz Type of objects contained in the query.
   * @throws IOException
   */
  public static <T extends BaseRecord> void overrideExpiredRecords(
      StateStoreDriver driver, QueryResult<T> query, Class<T> clazz)
          throws IOException {

    List<T> commitRecords = new ArrayList<T>();
    List<T> records = query.getRecords();
    long currentDriverTime = query.getTimestamp();
    if (records == null || currentDriverTime <= 0) {
      LOG.error("Unable to check overrides for record " + clazz);
      return;
    }
    for (T record : records) {
      if (record.checkExpired(currentDriverTime)) {
        LOG.info("Committing override to state store for record " + record);
        commitRecords.add(record);
      }
    }
    if (commitRecords.size() > 0) {
      driver.updateOrCreate(commitRecords, clazz, true, false);
    }
  }

  /**
   * Updates the state store with any record overrides we detected, such as an
   * expired state.
   *
   * @param driver State store driver for the data store.
   * @param record Record record to be updated.
   * @param clazz Type of data record.
   * @throws IOException
   */
  public static <T extends BaseRecord> void overrideExpiredRecord(
      StateStoreDriver driver, T record, Class<T> clazz)
          throws IOException {

    List<T> records = new ArrayList<T>();
    records.add(record);
    long time = driver.getTime();
    QueryResult<T> query = new QueryResult<T>(records, time);
    overrideExpiredRecords(driver, query, clazz);
  }

  /**
   * Serializes a data object. Default serialization is to a String. Override if
   * additional serialization is required.
   *
   * @param data Object to serialize.
   * @param clazz Class to serialize to. Only string is supported.
   * @return Serialized object.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Object> T serialize(Object data, Class<T> clazz) {

    if (clazz != String.class) {
      throw new UnsupportedOperationException(
          "Can serialize only to a string.");
    }
    if (data == null) {
      return (T) "null";
    }

    Class<? extends Object> className = data.getClass();
    if (className == String.class) {
      return (T) data;
    } else if (className == Long.class || className == long.class) {
      return (T) data.toString();
    } else if (className == Integer.class || className == int.class) {
      return (T) data.toString();
    } else if (className == Double.class || className == double.class) {
      return (T) data.toString();
    } else if (className == Float.class || className == float.class) {
      return (T) data.toString();
    } else if (className == Boolean.class || className == boolean.class) {
      return (T) data.toString();
    } else if (className.isEnum()) {
      return (T) data.toString();
    } else {
      // We could also return an empty string
      return (T) data.toString();
    }
  }

  /**
   * Expands a data object from the store into an record object. Default store
   * data type is a String. Override if additional serialization is required.
   *
   * @param data Object containing the serialized data. Only string is
   *          supported.
   * @param clazz Target object class to hold the deserialized data.
   * @return An instance of the target data object initialized with the
   *         deserialized data.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T extends Object> T deserialize(Object data, Class<T> clazz) {

    if (data.getClass() != String.class) {
      throw new UnsupportedOperationException(
          "Only string types can be deserialized");
    }
    String dataString = (String) data;
    if (dataString.equals("null")) {
      return null;
    } else if (clazz == String.class) {
      return (T) dataString;
    } else if (clazz == Long.class || clazz == long.class) {
      return (T) Long.valueOf(dataString);
    } else if (clazz == Integer.class || clazz == int.class) {
      return (T) Integer.valueOf(dataString);
    } else if (clazz == Double.class || clazz == double.class) {
      return (T) Double.valueOf(dataString);
    } else if (clazz == Float.class || clazz == float.class) {
      return (T) Float.valueOf(dataString);
    } else if (clazz == Boolean.class || clazz == boolean.class) {
      return (T) Boolean.valueOf(dataString);
    } else if (clazz.isEnum()) {
      return (T) Enum.valueOf((Class<Enum>) clazz, dataString);
    }
    return null;
  }
}
