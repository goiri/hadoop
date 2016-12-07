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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolBase;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.util.Time;
import org.codehaus.jettison.json.JSONObject;

/**
 * Abstract base of a data record in the StateStore. All StateStore records are
 * derived from this class.  Data records are persisted in the data store and
 * are identified by their primary key.  Each data record
 * contains:
 * <ul>
 * <li>A primary key consisting of a combination of record data fields.
 * <li>A modification date which acts as a version number.
 * <li>A creation date.
 * </ul>
 */
public abstract class BaseRecord extends FederationProtocolBase
    implements Comparable<BaseRecord> {

  private static final Log LOG = LogFactory.getLog(BaseRecord.class);

  /**
   * Set the modification time for the record.
   *
   * @param time Modification time of the record.
   */
  public abstract void setDateModified(long time);

  /**
   * Get the modification time for the record.
   *
   * @return Modification time of the record.
   */
  public abstract long getDateModified();

  /**
   * Set the creation time for the record.
   *
   * @param time Creation time of the record.
   */
  public abstract void setDateCreated(long time);

  /**
   * Get the creation time for the record.
   *
   * @return Creation time of the record
   */
  public abstract long getDateCreated();

  /**
   * Get the expiration time for the record.
   *
   * @return Expiration time for the record.
   */
  public abstract long getExpirationMs();

  /**
   * Map of primary key names->values for the record. The primary key can be a
   * combination of 1-n different State Store serialized values.
   *
   * @return Map of key/value pairs that constitute this object's primary key.
   */
  public abstract SortedMap<String, String> getPrimaryKeys();

  /**
   * Populate the record from a base64 serialized data string.
   *
   * @param serializedString The base64 serialization of the record.
   * @throws IOException if the record cannot be populated from the string.
   */
  public abstract void deserialize(String data) throws IOException;

  @Override
  public void initialize() {
    // Call this after the object has been constructed
    initializeDefaultTimes();
  }

  /**
   * Serialize the object string representation for storage.
   *
   * @return A base64 string representation of the object.
   */
  public String serialize() {
    return FederationProtocolFactory.serialize(this);
  }

  /**
   * Join the primary keys into one single primary key.
   *
   * @return A string that is guaranteed to be unique amongst all records of
   *         this type.
   */
  public String getPrimaryKey() {
    return generateMashupKey(getPrimaryKeys());
  }

  /**
   * Generates a cache key from a map of values.
   *
   * @param keys Map of values.
   * @return String mashup of key values.
   */
  protected static String generateMashupKey(SortedMap<String, String> keys) {
    StringBuilder builder = new StringBuilder();
    for (Object value : keys.values()) {
      if (builder.length() > 0) {
        builder.append("-");
      }
      builder.append(value);
    }
    return builder.toString();
  }

  /**
   * Initialize default times. The driver may update these timestamps on insert
   * and/or update. This should only be called when initializing an object that
   * is not backed by a data store.
   */
  public void initializeDefaultTimes() {
    this.setDateCreated(Time.now());
    this.setDateModified(Time.now());
  }

  /**
   * Override equals check to use primary key(s) for comparison.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BaseRecord)) {
      return false;
    }

    BaseRecord baseObject = (BaseRecord) obj;
    Map<String, String> keyset1 = this.getPrimaryKeys();
    Map<String, String> keyset2 = baseObject.getPrimaryKeys();
    return keyset1.equals(keyset2);
  }

  /**
   * Override hash code to use primary key(s) for comparison.
   */
  @Override
  public int hashCode() {
    Map<String, String> keyset = this.getPrimaryKeys();
    return keyset.hashCode();
  }

  @Override
  public int compareTo(BaseRecord record) {
    if (record == null) {
      return -1;
    }
    // Descending date order
    return (int) (record.getDateModified() - this.getDateModified());
  }

  /**
   * Called when the modification time and current time is available, checks for
   * expirations.
   *
   * @param currentTime The current timestamp in ms from the data store, to be
   *          compared against the modification and creation dates of the
   *          object.
   * @return boolean True if the record has been updated and should be
   *         committed to the data store. Override for customized behavior.
   */
  public boolean checkExpired(long currentTime) {
    long expiration = this.getExpirationMs();
    return (getDateModified() > 0 && expiration > 0
        && (getDateModified() + expiration) < currentTime);
  }

  /**
   * Called before serializing a record to the State Store. Opportunity to
   * update state store serialized fields before the data is committed to the
   * store.
   */
  public void deflate() {
  }

  /**
   * Validates the record. Called when the record is created, populated from the
   * state store, and before committing to the state store.
   *
   * @throws IllegalArgumentException
   */
  public void validate() {
    if (getDateCreated() == 0 || getDateModified() == 0) {
      throw new IllegalArgumentException(
          "Invalid record, no date specified " + this.toString());
    }
  }

  /**
   * Fetch a unique identifier consisting of the class name, primary key, last
   * mod timestamp and version of the record. This ensures that each identifier
   * represents only a single record in a single state (no repeats).
   *
   * @return Unique string identifier for this record.
   */
  public String getUniqueIdentifier() {
    String primaryKey = getPrimaryKey();
    return this.getClass().getCanonicalName() + "-" + primaryKey + "-"
        + getDateModified();
  }

  /**
   * If we should update a field.
   *
   * @param fieldName Name of the field.
   * @return If we should update a field.
   */
  public boolean shouldUpdateField(String fieldName) {
    if (fieldName.equals("dateCreated")) {
      return false;
    }
    return true;
  }

  /**
   * If we should insert a field.
   *
   * @param fieldName Name of the field.
   * @return If we should insert a field.
   */
  public boolean shouldInsertField(String fieldName) {
    return true;
  }

  /**
   * Get JSON for a data record.
   *
   * @param data Data to get JSON for.
   * @return JSON represenation.
   */
  public static Object getJson(Object data) {
    if (data == null) {
      return JSONObject.NULL;
    }
    return data;
  }

  /**
   * Get JSON for this record.
   *
   * @return Map representing the data for the JSON representation.
   */
  public Map<String, Object> getJson() {
    Map<String, Object> json = new HashMap<String, Object>();
    Map<String, Class<?>> fields = this.getFields();

    for (String fieldName : fields.keySet()) {
      if (!fieldName.equalsIgnoreCase("proto")) {
        try {
          Object value = this.getField(fieldName);
          json.put(fieldName, BaseRecord.getJson(value));
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Unable to serialize field " + fieldName + " into JSON.");
        }
      }
    }
    return json;
  }

  @Override
  public String toString() {
    return this.getPrimaryKey();
  }

  /**
   * Get the version number of this record. Default is the dateModified in ms
   * since reference date.
   *
   * @return Long with the version of the record.
   */
  public long getVersion() {
    return getDateModified();
  }

  /**
   * Finds the appropriate getter for a field name.
   *
   * @param fieldName The legacy name of the field.
   * @return The matching getter or null if not found.
   */
  public Method locateGetter(String fieldName) {
    for (Method m : this.getClass().getMethods()) {
      if (m.getName().equalsIgnoreCase("get" + fieldName)) {
        return m;
      }
    }
    return null;
  }

  /**
   * Finds the appropriate setter for a field name.
   *
   * @param fieldName The legacy name of the field.
   * @return The matching setter or null if not found.
   */
  public Method locateSetter(String fieldName) {
    for (Method m : this.getClass().getMethods()) {
      if (m.getName().equalsIgnoreCase("set" + fieldName)) {
        return m;
      }
    }
    return null;
  }

  /**
   * Fetches the value for a field name.
   *
   * @param fieldName the legacy name of the field.
   * @return The field data or null if not found.
   */
  public Object getField(String fieldName) {
    Object result = null;
    Method m = this.locateGetter(fieldName);
    if (m != null) {
      try {
        result = m.invoke(this);
      } catch (Exception e) {
        LOG.error("Unable to get field " + fieldName + " on object " + this);
      }
    }
    return result;
  }

  /**
   * Get the type of a field.
   *
   * @param fieldName
   * @return Field type
   */
  public Class<?> getFieldType(String fieldName) {
    Method m = this.locateGetter(fieldName);
    return m.getReturnType();
  }

  /**
   * Sets the value of a field on the object.
   *
   * @param fieldName The string name of the field.
   * @param data The data to pass to the field's setter.
   *
   * @return True if successful, fails if failed.
   */
  public boolean setField(String fieldName, Object data) {
    Method m = this.locateSetter(fieldName);
    if (m != null) {
      try {
        m.invoke(this, data);
      } catch (Exception e) {
        LOG.error("Unable to set field " + fieldName + " on object "
            + this.getClass().getName() + " to data " + data + " of type "
            + data.getClass(), e);
        return false;
      }
    }
    return true;
  }

  /**
   * Returns all serializable fields in the object.
   *
   * @return Map with the fields.
   */
  public Map<String, Class<?>> getFields() {
    Map<String, Class<?>> getters = new HashMap<String, Class<?>>();
    for (Method m : this.getClass().getDeclaredMethods()) {
      if (m.getName().startsWith("get")) {
        try {
          Class<?> type = m.getReturnType();
          char[] c = m.getName().substring(3).toCharArray();
          c[0] = Character.toLowerCase(c[0]);
          String key = new String(c);
          getters.put(key, type);
        } catch (Exception e) {
          LOG.error("Unable to execute getter " + m.getName()
              + " on object " + this.toString());
        }
      }
    }
    return getters;
  }
}
