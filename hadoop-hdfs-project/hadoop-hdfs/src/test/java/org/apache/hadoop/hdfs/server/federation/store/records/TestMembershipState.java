package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.junit.Test;

public class TestMembershipState {

  private static final String ROUTER = "router";
  private static final String nameservice = "nameservice";
  private static final String namenode = "namenode";
  private static final String clusterId = "cluster";
  private static final String blockPoolId = "blockpool";
  private static final String rpcAddress = "rpcaddress";
  private static final String webAddress = "webaddress";
  private static final boolean safemode = false;
  private static final long dateCreated = 100;
  private static final long dateModified = 200;
  private static final long numBlocks = 300;
  private static final long numFiles = 400;
  private static final int numDead = 500;
  private static final int numActive = 600;
  private static final int numDecom = 700;
  private static final long numBlocksMissing = 800;
  private static final long totalSpace = 900;
  private static final long availableSpace = 1000;
  private static FederationNamenodeServiceState state =
      FederationNamenodeServiceState.ACTIVE;

  private MembershipState createRecord() throws IOException {
    
    MembershipState record =
        MembershipState.newInstance(ROUTER, nameservice, namenode, clusterId,
            blockPoolId, rpcAddress, webAddress, state, safemode);
    record.setDateCreated(dateCreated);
    record.setDateModified(dateModified);
    record.setNumOfBlocks(numBlocks);
    record.setNumOfFiles(numFiles);
    record.setNumberOfActiveDatanodes(numActive);
    record.setNumberOfDeadDatanodes(numDead);
    record.setNumberOfDecomDatanodes(numDecom);
    record.setNumOfBlocksMissing(numBlocksMissing);
    record.setTotalSpace(totalSpace);
    record.setAvailableSpace(availableSpace);
    return record;
  }
  
  private void validateRecord(MembershipState record) {
    
    assertEquals(ROUTER, record.getRouterId());
    assertEquals(nameservice, record.getNameserviceId());
    assertEquals(clusterId, record.getClusterId());
    assertEquals(blockPoolId, record.getBlockPoolId());
    assertEquals(rpcAddress, record.getRpcAddress());
    assertEquals(webAddress, record.getWebAddress());
    assertEquals(state, record.getState());
    assertEquals(safemode, record.getIsSafeMode());
    assertEquals(dateCreated, record.getDateCreated());
    assertEquals(dateModified, record.getDateModified());
    assertEquals(numBlocks, record.getNumOfBlocks());
    assertEquals(numFiles, record.getNumOfFiles());
    assertEquals(numActive, record.getNumberOfActiveDatanodes());
    assertEquals(numDead, record.getNumberOfDeadDatanodes());
    assertEquals(numDecom, record.getNumberOfDecomDatanodes());
    assertEquals(totalSpace, record.getTotalSpace());
    assertEquals(availableSpace, record.getAvailableSpace());
  }
  
  @Test
  public void testGetterSetter() throws IOException {
    MembershipState record = createRecord();
    validateRecord(record);
  }

  @Test
  public void testSerialization() throws IOException {

    MembershipState record = createRecord();

    String serializedString = record.serialize();
    MembershipState newRecord = FederationProtocolFactory
        .newInstance(MembershipState.class);
    newRecord.deserialize(serializedString);

    validateRecord(newRecord);
  }
}
