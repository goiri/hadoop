package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.junit.Test;

public class TestRouterState {

  private static final String address = "address";
  private static final String buildVersion = "buildVersion";
  private static final String compileInfo = "compileInfo";
  private static final long startTime = 100;
  private static final long dateModified = 200;
  private static final long dateCreated = 300;
  private static final long pathLockVersion = 400;
  private static final long fileResolverVersion = 500;
  private static final RouterServiceState state = RouterServiceState.RUNNING;
  
  
  private RouterState generateRecord() throws IOException {
    RouterState record =
        RouterState.newInstance(address, startTime, state);
    record.setBuildVersion(buildVersion);
    record.setCompileInfo(compileInfo);
    record.setDateCreated(dateCreated);
    record.setDateModified(dateModified);
    record.setPathLockVersion(pathLockVersion);
    record.setFileResolverVersion(fileResolverVersion);
    return record;
  }
  
  private void validateRecord(RouterState record) {
    assertEquals(address, record.getAddress());
    assertEquals(startTime, record.getDateStarted());
    assertEquals(state, record.getStatus());
    assertEquals(compileInfo, record.getCompileInfo());
    assertEquals(buildVersion, record.getBuildVersion());
    assertEquals(pathLockVersion, record.getPathLockVersion());
    assertEquals(fileResolverVersion, record.getFileResolverVersion());
  }
  
  @Test
  public void testGetterSetter() throws IOException {
    RouterState record = generateRecord();
    validateRecord(record);
  }

  @Test
  public void testSerialization() throws IOException {

    RouterState record = generateRecord();

    String serializedString = record.serialize();
    RouterState newRecord = FederationProtocolFactory
        .newInstance(RouterState.class);
    newRecord.deserialize(serializedString);

    validateRecord(newRecord);
  }
}
