package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMountTable {

  private static final String src = "/test";
  private static final String dstNs1 = "ns0";
  private static final String dstNs2 = "ns1";
  private static final String dstPath1 = "/path1";
  private static final String dstPath2 = "/path/path2";
  private static final long dateCreated = 100;
  private static final long dateModified = 200;
  private static final String dstString = "ns0:::/path1,ns1:::/path/path2";
  
  private static Map<String, String> dest;
  
  @BeforeClass
  public static void setup() {
    dest = new HashMap<String, String>();
    dest.put(dstNs1,dstPath1);
    dest.put(dstNs2,dstPath2);
  }

  private void validateDestinations(MountTable record) {
    
    assertEquals(src, record.getSourcePath());
    assertEquals(2, record.getDestinations().size());
    
    RemoteLocation location1 = record.getDestinations().get(0);
    assertEquals(dstNs1, location1.getNameserviceId());
    assertEquals(dstPath1, location1.getDest());
    
    RemoteLocation location2 = record.getDestinations().get(1);
    assertEquals(dstNs2, location2.getNameserviceId());
    assertEquals(dstPath2, location2.getDest());
  }
  
  @Test
  public void testGetterSetter() throws IOException {

    MountTable record =
        MountTable.newInstance(src, dest);
    
    validateDestinations(record);
    assertEquals(src, record.getSourcePath());
    assertEquals(dstString, record.getDestPaths());
    assertTrue(dateCreated > 0);
    assertTrue(dateModified > 0);
    
    record = MountTable.newInstance(src, dstString, dateCreated, dateModified);
    
    validateDestinations(record);
    assertEquals(src, record.getSourcePath());
    assertEquals(dstString, record.getDestPaths());
    assertEquals(dateCreated, record.getDateCreated());
    assertEquals(dateModified, record.getDateModified());
  }

  @Test
  public void testSerialization() throws IOException {

    MountTable record = MountTable.newInstance(src, dstString,
        dateCreated, dateModified);

    String serializedString = record.serialize();
    MountTable newRecord = FederationProtocolFactory
        .newInstance(MountTable.class);
    newRecord.deserialize(serializedString);
    
    validateDestinations(record);
    assertEquals(src, record.getSourcePath());
    assertEquals(dstString, record.getDestPaths());
    assertEquals(dateCreated, record.getDateCreated());
    assertEquals(dateModified, record.getDateModified());
  }
}
