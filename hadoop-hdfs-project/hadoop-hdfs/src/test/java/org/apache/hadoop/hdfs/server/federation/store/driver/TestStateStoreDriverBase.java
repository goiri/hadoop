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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.records.Barrier;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.DecommissionedNamespace;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.NamespaceStats;
import org.apache.hadoop.hdfs.server.federation.store.records.PathLock;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.RebalancerLog;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.junit.AfterClass;

/**
 * Base tests for the driver. The particular implementations will use this to
 * test their functionality.
 */
public class TestStateStoreDriverBase {

  protected static FederationStateStoreService stateStore;
  protected static Configuration conf;

  @AfterClass
  public static void tearDownCluster() {
    if (stateStore != null) {
      stateStore.stop();
    }
  }

  public static void generateStateStore(Configuration configuration)
      throws Exception {
    conf = configuration;
    stateStore = FederationStateStoreTestUtils.generateStateStore(configuration);
  }

  private String generateRandomString() {
    Random rand = new Random();
    String randomString = "/randomString-" + rand.nextInt();
    return randomString;
  }

  private long generateRandomLong() {
    Random rand = new Random();
    return rand.nextLong();
  }

  @SuppressWarnings("rawtypes")
  private <T extends Enum> T generateRandomEnum(Class<T> enumClass) {
    Random rand = new Random();
    int x = rand.nextInt(enumClass.getEnumConstants().length);
    T data = enumClass.getEnumConstants()[x];
    return data;
  }

  @SuppressWarnings("unchecked")
  private <T extends BaseRecord> T generateFakeRecord(Class<T> recordClass)
      throws IllegalArgumentException, IllegalAccessException, IOException {

    if (recordClass == MountTable.class) {
      String src = "/" + generateRandomString();
      Map<String, String> destMap = new HashMap<String, String>();
      destMap.put(generateRandomString(), "/" + generateRandomString());
      return (T) MountTable.newInstance(src, destMap);
    } else if (recordClass == MembershipState.class) {
      return (T) MembershipState.newInstance(generateRandomString(), generateRandomString(),
          generateRandomString(), generateRandomString(), generateRandomString(),
          generateRandomString(), generateRandomString(),
          generateRandomEnum(FederationNamenodeServiceState.class), false);
    } else if (recordClass == RouterState.class) {
      return (T) RouterState.newInstance(generateRandomString(),
          generateRandomLong(), generateRandomEnum(RouterServiceState.class));
    } else if (recordClass == RebalancerLog.class) {
      return (T) RebalancerLog.newInstance(generateRandomString(),
          generateRandomString(), generateRandomString(),
          "/" + generateRandomString());
    } else if (recordClass == PathLock.class) {
      return (T) PathLock.newInstance(generateRandomString(),
          generateRandomString());
    } else if (recordClass == Barrier.class) {
      return (T) Barrier.newInstance(generateRandomString(),
          generateRandomString());
    } else if (recordClass == NamespaceStats.class) {
      return (T) NamespaceStats.newInstance(generateRandomString(),
          generateRandomLong(), generateRandomString());
    } else if (recordClass == DecommissionedNamespace.class) {
      return (T) DecommissionedNamespace.newInstance(generateRandomString());
    }

    return null;
  }

  private void validateRecord(BaseRecord original, BaseRecord committed)
      throws IllegalArgumentException, IllegalAccessException {

    Map<String, Class<?>> fields = original.getFields();

    for (String key : fields.keySet()) {
      if (key.equals("dateModified")
          || key.equals("dateCreated")
          || key.equals("proto")) {
        // Fields are updated/set on commit and fetch and may not match
        // the fields that are initialized in a non-committed object.
        continue;
      }
      Object data1 = original.getField(key);
      Object data2 = committed.getField(key);
      if (!data1.equals(data2)) {
          throw new IllegalArgumentException(
              "Field " + key + " does not match");
      }
    }

    long now = stateStore.getDriver().getTime();
    assertTrue(
        committed.getDateCreated() <= now && committed.getDateCreated() > 0);
    assertTrue(committed.getDateModified() >= committed.getDateCreated());
  }

  public static void deleteAll(StateStoreDriver driver) throws IOException {
    driver.delete(MountTable.class);
    driver.delete(RebalancerLog.class);
    driver.delete(RouterState.class);
    driver.delete(MembershipState.class);
  }

  public <T extends BaseRecord> void testInsert(StateStoreDriver driver,
      Class<T> recordClass)
          throws IllegalArgumentException, IllegalAccessException, IOException {

    assertTrue(driver.delete(recordClass));
    QueryResult<T> records = driver.get(recordClass);
    assertTrue(records.getRecords().isEmpty());

    // Insert single
    BaseRecord record = generateFakeRecord(recordClass);
    driver.updateOrCreate(record, true, false);

    // Verify
    records = driver.get(recordClass);
    assertEquals(1, records.getRecords().size());
    validateRecord(record, records.getRecords().get(0));

    // Insert multiple
    List<T> insertList = new ArrayList<T>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(recordClass);
      insertList.add(newRecord);
    }
    driver.updateOrCreate(insertList, recordClass, true, false);

    // Verify
    records = driver.get(recordClass);
    assertEquals(11, records.getRecords().size());
  }

  public <T extends BaseRecord> void testFetchErrors(StateStoreDriver driver,
      Class<T> recordClass)
          throws IllegalArgumentException, IllegalAccessException, IOException {

    // Fetch empty list
    driver.delete(recordClass);
    QueryResult<T> records = driver.get(recordClass);
    assertNotNull(records);
    assertEquals(records.getRecords().size(), 0);

    // Insert single
    BaseRecord record = generateFakeRecord(recordClass);
    assertTrue(driver.updateOrCreate(record, true, false));

    // Verify
    records = driver.get(recordClass);
    assertEquals(1, records.getRecords().size());
    validateRecord(record, records.getRecords().get(0));

    // Test fetch single object key name is not a valid column, throws
    // IOException
    Map<String, String> query = new HashMap<String, String>();
    boolean exceptionThrown = false;
    query.put("UnknownKey", "UnknownValue");
    try {
      driver.get(recordClass, query);
    } catch (IOException ex) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    // Test fetch multiple objects key name is not a valid column, throws
    // IOException
    exceptionThrown = false;
    query.put("UnknownKey", "UnknownValue");
    try {
      driver.getMultiple(recordClass, query);
    } catch (IOException ex) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    // Test fetch single object does not exist returns null
    query = new HashMap<String, String>();
    query.put(record.getPrimaryKeys().keySet().iterator().next(),
        "NonMatchingKey");
    assertNull(driver.get(recordClass, query));

    // Test fetch multiple objects does not exist returns empty list
    assertEquals(driver.getMultiple(recordClass, query).size(), 0);
  }

  public <T extends BaseRecord> void testUpdate(
      StateStoreDriver driver, Class<T> clazz)
          throws IllegalArgumentException, IllegalAccessException, IOException,
          NoSuchFieldException, SecurityException {

    driver.delete(clazz);
    QueryResult<T> records = driver.get(clazz);
    assertTrue(records.getRecords().isEmpty());

    // Insert multiple
    List<T> insertList = new ArrayList<T>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }

    // Verify
    assertEquals(true,
        driver.updateOrCreate(insertList, clazz, false, true));
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 10);

    // Generate a new record with the same PK fields as an existing record
    BaseRecord updatedRecord = generateFakeRecord(clazz);
    BaseRecord existingRecord = records.getRecords().get(0);
    for (Entry<String, String> key : existingRecord.getPrimaryKeys()
        .entrySet()) {
      Class<?> fieldType = existingRecord.getFieldType(key.getKey());
      assertEquals(true,
          updatedRecord.setField(key.getKey(), FederationStateStoreUtils
              .deserialize(key.getValue(), fieldType)));
    }

    // Attempt an update of an existing entry, but it is not allowed.
    assertEquals(false, driver.updateOrCreate(updatedRecord, false, true));

    // Verify no update occurred, all original records are unchanged
    QueryResult<T> newRecords = driver.get(clazz);
    assertTrue(newRecords.getRecords().size() == 10);
    assertEquals("A single entry was improperly updated in the store.",
        countMatchingEntries(records.getRecords(), newRecords.getRecords()), 10);

    // Update the entry (allowing updates)
    assertEquals(true, driver.updateOrCreate(updatedRecord, true, false));

    // Verify that one entry no longer matches the original set
    newRecords = driver.get(clazz);
    assertEquals(10, newRecords.getRecords().size());
    assertEquals(
        "Record of type " + clazz + " was not updated in the store.", 9,
        countMatchingEntries(records.getRecords(), newRecords.getRecords()));

  }

  private int countMatchingEntries(
      Collection<? extends BaseRecord> committedList,
      Collection<? extends BaseRecord> matchList) {

    int matchingCount = 0;
    for (BaseRecord committed : committedList) {
      for (BaseRecord match : matchList) {
        try {
          validateRecord(match, committed);
          matchingCount++;
        } catch (Exception ex) {
        }
      }
    }
    return matchingCount;
  }

  public <T extends BaseRecord> void testDelete(
      StateStoreDriver driver, Class<T> clazz)
          throws IllegalArgumentException, IllegalAccessException, IOException {

    // Delete all
    assertTrue(driver.delete(clazz));
    QueryResult<T> records = driver.get(clazz);
    assertTrue(records.getRecords().isEmpty());

    // Insert multiple
    List<T> insertList = new ArrayList<T>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }

    // Verify
    assertEquals(true,
        driver.updateOrCreate(insertList, clazz, false, true));
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 10);

    // Delete Single
    assertTrue(driver.delete(records.getRecords().get(0)));

    // Verify
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 9);

    // Delete with filter
    assertTrue(driver.delete(
        FederationProtocolFactory.getReferenceClass(records.getRecords().get(0)),
        records.getRecords().get(0).getPrimaryKeys()) > 0);
    assertTrue(driver.delete(
        FederationProtocolFactory.getReferenceClass(records.getRecords().get(0)),
        records.getRecords().get(1).getPrimaryKeys()) > 0);

    // Verify
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 7);

    // Delete all
    assertTrue(driver.delete(clazz));

    // Verify
    records = driver.get(clazz);
    assertTrue(records.getRecords().size() == 0);
  }

  public void testInsert(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(driver, MountTable.class);
    testInsert(driver, RebalancerLog.class);
    testInsert(driver, RouterState.class);
    testInsert(driver, MembershipState.class);
    testInsert(driver, PathLock.class);
    testInsert(driver, DecommissionedNamespace.class);
    testInsert(driver, NamespaceStats.class);
  }

  public void testUpdate(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException,
      NoSuchFieldException, SecurityException {
    testUpdate(driver, MountTable.class);
    testUpdate(driver, RebalancerLog.class);
    testUpdate(driver, RouterState.class);
    testUpdate(driver, MembershipState.class);
    testUpdate(driver, PathLock.class);
    testUpdate(driver, NamespaceStats.class);
  }

  public void testDelete(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testDelete(driver, MountTable.class);
    testDelete(driver, RebalancerLog.class);
    testDelete(driver, RouterState.class);
    testDelete(driver, MembershipState.class);
    testDelete(driver, PathLock.class);
    testDelete(driver, NamespaceStats.class);
  }

  public void testFetchErrors(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testFetchErrors(driver, MountTable.class);
    testFetchErrors(driver, RebalancerLog.class);
    testFetchErrors(driver, RouterState.class);
    testFetchErrors(driver, MembershipState.class);
    testFetchErrors(driver, PathLock.class);
  }

  public void testMetrics(StateStoreDriver driver)
      throws IOException, IllegalArgumentException, IllegalAccessException {

    MountTable insertRecord =
        this.generateFakeRecord(MountTable.class);
    Map<String, String> query = new HashMap<String, String>();
    query.put("sourcePath", insertRecord.getSourcePath());

    // UpdateOrCreateSingle
    stateStore.resetMetrics();
    driver.updateOrCreate(insertRecord, true, false);

    // UpdateOrCreateMultiple
    stateStore.resetMetrics();
    driver.updateOrCreate(insertRecord, true, false);

    // Get Single
    stateStore.resetMetrics();
    query.put("sourcePath", insertRecord.getSourcePath());
    driver.get(MountTable.class, query);

    // GetAll
    stateStore.resetMetrics();
    driver.get(MountTable.class);

    // GetMultiple
    stateStore.resetMetrics();
    driver.getMultiple(MountTable.class, query);

    // Insert fails
    stateStore.resetMetrics();
    driver.updateOrCreate(insertRecord, false, true);

    // Delete single
    stateStore.resetMetrics();
    driver.delete(insertRecord);

    // Delete multiple
    stateStore.resetMetrics();
    driver.updateOrCreate(insertRecord, true, false);
    driver.delete(MountTable.class, query);

    // Delete all
    stateStore.resetMetrics();
    driver.updateOrCreate(insertRecord, true, false);
    driver.delete(MountTable.class);
  }
}
