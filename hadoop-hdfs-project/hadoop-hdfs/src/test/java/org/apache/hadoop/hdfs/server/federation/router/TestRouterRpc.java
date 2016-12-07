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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.TEST_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The the RPC interface of the {@link Router} implemented by
 * {@link RouterRpcServer}.
 */
public class TestRouterRpc {

  private static RouterDFSCluster cluster;
  private RouterContext router;
  private String ns;
  private NamenodeContext namenode;
  private ClientProtocol routerProtocol;
  private ClientProtocol namenodeProtocol;
  private FileSystem routerFileSystem;
  private FileSystem namenodeFileSystem;
  private String routerFile;
  private String namenodeFile;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new RouterDFSCluster(false, 2);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    cluster.addRouterOverrides((new RouterConfigBuilder()).rpc().build());
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
  }

  @Before
  public void testSetup() throws Exception {

    // Create mock locations
    cluster.installMockLocations();

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    // Pick a NS, namenode and router for this test
    this.router = cluster.getRandomRouter();
    this.ns = cluster.getRandomNameservice();
    this.namenode = cluster.getNamenode(ns, null);

    // Handles to the ClientProtocol interface
    this.routerProtocol = router.getClient().getNamenode();
    this.namenodeProtocol = namenode.getClient().getNamenode();

    // Handles to the filesystem client
    this.namenodeFileSystem = namenode.getFileSystem();
    this.routerFileSystem = router.getFileSystem();

    // Create a test file on the NN
    Random r = new Random();
    String randomString = "testfile-" + r.nextInt();
    this.namenodeFile =
        cluster.getNamenodeTestDirectoryForNameservice(ns) + "/" + randomString;
    this.routerFile = cluster.getFederatedTestDirectoryForNameservice(ns) + "/"
        + randomString;
    FederationTestUtils.createFile(namenodeFileSystem, namenodeFile, 32);
    FederationTestUtils.verifyFileExists(namenodeFileSystem, namenodeFile);
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testRpcService() throws IOException {
    Router testRouter = new Router();
    Configuration routerConfig = cluster
        .generateRouterConfiguration(
        cluster.getNameservices().get(0), null);
    RouterRpcServer server =
        new RouterRpcServer(routerConfig, testRouter.getNamenodeResolver(),
            testRouter.getSubclusterResolver(), testRouter.getPathLockManager());
    server.init(routerConfig);
    assertEquals(STATE.INITED, server.getServiceState());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    server.close();
    testRouter.close();
  }

  protected RouterDFSCluster getCluster() {
    return TestRouterRpc.cluster;
  }

  protected RouterContext getRouter() {
    return this.router;
  }

  protected void setRouter(RouterContext r)
      throws IOException, URISyntaxException {
    this.router = r;
    this.routerProtocol = r.getClient().getNamenode();
    this.routerFileSystem = r.getFileSystem();
  }

  protected FileSystem getRouterFileSystem() {
    return this.routerFileSystem;
  }

  protected FileSystem getNamenodeFileSystem() {
    return this.namenodeFileSystem;
  }

  protected ClientProtocol getRouterProtocol() {
    return this.routerProtocol;
  }

  protected ClientProtocol getNamenodeProtocol() {
    return this.namenodeProtocol;
  }

  protected NamenodeContext getNamenode() {
    return this.namenode;
  }

  protected void setNamenodeFile(String filename) {
    this.namenodeFile = filename;
  }

  protected String getNamenodeFile() {
    return this.namenodeFile;
  }

  protected void setRouterFile(String filename) {
    this.routerFile = filename;
  }

  protected String getRouterFile() {
    return this.routerFile;
  }

  protected void setNamenode(NamenodeContext nn)
      throws IOException, URISyntaxException {
    this.namenode = nn;
    this.namenodeProtocol = nn.getClient().getNamenode();
    this.namenodeFileSystem = nn.getFileSystem();
  }

  protected String getNs() {
    return this.ns;
  }

  protected void setNs(String nameservice) {
    this.ns = nameservice;
  }


  protected void compareResponses(ClientProtocol protocol1,
      ClientProtocol protocol2, Method m, Object[] paramList) {

    Object return1 = null;
    Object return2 = null;
    Exception exception1 = null;
    Exception exception2 = null;

    try {
      return1 = m.invoke(protocol1, paramList);
    } catch (Exception ex) {
      ex.printStackTrace();
      exception1 = ex;
    }

    try {
      return2 = m.invoke(protocol2, paramList);
    } catch (Exception ex) {
      exception2 = ex;
    }
    assertEquals(return1, return2);
    if (exception1 == null && exception2 == null) {
      return;
    }
    assertEquals(exception1.getCause().getClass(),
        exception2.getCause().getClass());
  }

  @Test
  public void testProxyListFiles()
 throws IOException, InterruptedException,
      URISyntaxException, NoSuchMethodException, SecurityException {

    // Verify that the root listing is a union of the mount table destinations
    // and the files stored at all nameservices mounted at the root (ns0 + ns1)
    //
    // / -->
    // /ns0 (from mount table)
    // /ns1 (from mount table)
    // all items in / of ns0 (default NS)
    //

    // Collect the mount table entries from the root mount point
    SortedSet<String> requiredPaths = new TreeSet<String>();
    for (String mount : router.getRouter().getSubclusterResolver()
        .getMountPoints("/")) {
      requiredPaths.add(mount);
    }

    // Collect all files/dirs on the root path of the default NS
    String defaultNs = cluster.getNameservices().get(0);
    NamenodeContext nn = cluster.getNamenode(defaultNs, null);
    FileStatus[] iterator = nn.getFileSystem().listStatus(new Path("/"));
    for (FileStatus file : iterator) {
      requiredPaths.add(file.getPath().getName());
    }

    // Fetch listing
    DirectoryListing listing =
        routerProtocol.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
    Iterator<String> requiredPathsIterator = requiredPaths.iterator();
    // Match each path returned and verify order returned
    for(HdfsFileStatus f : listing.getPartialListing()) {
      String fileName = requiredPathsIterator.next();
      String currentFile = f.getFullPath(new Path("/")).getName();
      assertEquals(currentFile, fileName);
    }

    // Verify the total number of results found/matched
    assertEquals(requiredPaths.size(), listing.getPartialListing().length);

    // List a path that doesn't exist and validate error response with NN
    // behavior.
    Method m = ClientProtocol.class.getMethod("getListing", String.class,
        byte[].class, boolean.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, HdfsFileStatus.EMPTY_NAME, false});
  }

  @Test
  public void testProxyListFilesWithConflict()
      throws IOException, InterruptedException {

    int initialCount =
        FederationTestUtils.countContents(router.getFileSystem(), "/");

    // Add a directory to the namespace that conflicts with one of
    // the mount points
    NamenodeContext nn = cluster.getNamenode(ns, null);
    FederationTestUtils.addDirectory(nn.getFileSystem(),
        cluster.getFederatedTestDirectoryForNameservice(ns));

    // root file system now for NS X:
    // / ->
    // /ns0 (mount table)
    // /ns1 (mount table)
    // /target-ns0 (the target folder for the NS0 mapped to /
    // /nsX (local directory that duplicates mount table)
    //
    int newCount =
        FederationTestUtils.countContents(router.getFileSystem(), "/");
    assertEquals(initialCount, newCount);

    // Verify that each root path is readable and contains a single test
    // directory
    assertEquals(1, FederationTestUtils.countContents(router.getFileSystem(),
        cluster.getFederatedPathForNameservice(ns)));

    // Verify that real folder for the ns contains a single
    // test directory
    assertEquals(1, FederationTestUtils.countContents(router.getFileSystem(),
        cluster.getFederatedPathForNameservice(ns)));

  }

  protected void testRename(RouterContext testRouter, String filename,
      String renamedFile, boolean exceptionExpected) throws IOException {
    FederationTestUtils.createFile(testRouter.getFileSystem(), filename, 32);
    // verify
    FederationTestUtils.verifyFileExists(testRouter.getFileSystem(), filename);
    // rename
    boolean exceptionThrown = false;
    try {
      testRouter.getClient().getNamenode().rename(filename, renamedFile);
    } catch (Exception ex) {
      exceptionThrown = true;
    }
    if (exceptionExpected) {
      // Error was expected
      assertEquals(true, exceptionThrown);
      assertEquals(true,
          testRouter.getFileContext().delete(new Path(filename), true));
    } else {
      // No error was expected
      assertEquals(false, exceptionThrown);
      // verify
      assertEquals(true, FederationTestUtils
          .verifyFileExists(testRouter.getFileSystem(), renamedFile));
      // delete
      assertEquals(true,
          testRouter.getFileContext().delete(new Path(renamedFile), true));
    }
  }

  protected void testRename2(RouterContext testRouter, String filename,
      String renamedFile, boolean exceptionExpected) throws IOException {
    FederationTestUtils.createFile(testRouter.getFileSystem(), filename, 32);
    // verify
    FederationTestUtils.verifyFileExists(testRouter.getFileSystem(), filename);
    // rename
    boolean exceptionThrown = false;
    try {
      testRouter.getClient().getNamenode().rename2(filename, renamedFile,
          new Options.Rename[] {});
    } catch (Exception ex) {
      exceptionThrown = true;
    }
    assertEquals(exceptionExpected, exceptionThrown);
    if (exceptionExpected) {
      // Error was expected
      assertEquals(true,
          testRouter.getFileContext().delete(new Path(filename), true));
    } else {
      // verify
      assertEquals(true, FederationTestUtils
          .verifyFileExists(testRouter.getFileSystem(), renamedFile));
      // delete
      assertEquals(true,
          testRouter.getFileContext().delete(new Path(renamedFile), true));
    }
  }

  @Test
  public void testProxyRenameFiles() throws IOException, InterruptedException {

    Thread.sleep(5000);
    String ns0 = cluster.getNameservices().get(0);
    String ns1 = cluster.getNameservices().get(1);

    // Rename within the same namespace
    // /ns0/testdir/testrename -> /ns0/testdir/testrename-append
    String filename =
        cluster.getFederatedTestDirectoryForNameservice(ns0) + "/testrename";
    String renamedFile = filename + "-append";
    testRename(router, filename, renamedFile, false);
    testRename2(router, filename, renamedFile, false);

    // Rename a file to a destination that is in a different namespace (fails)
    filename =
        cluster.getFederatedTestDirectoryForNameservice(ns0) + "/testrename";
    renamedFile =
        cluster.getFederatedTestDirectoryForNameservice(ns1) + "/testrename";
    testRename(router, filename, renamedFile, true);
    testRename2(router, filename, renamedFile, true);
  }

  @Test
  public void testProxyChownFiles() throws Exception {

    String newUsername = "TestUser";
    String newGroup = "TestGroup";

    // change owner
    routerProtocol.setOwner(routerFile, newUsername, newGroup);

    // Verify with NN
    FileStatus file = FederationTestUtils
        .getFileStatus(namenode.getFileSystem(), namenodeFile);
    assertTrue(file.getOwner().equals(newUsername));
    assertTrue(file.getGroup().equals(newGroup));

    // Bad request and validate router response matches NN response.
    Method m = ClientProtocol.class.getMethod("setOwner", String.class,
        String.class, String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, newUsername, newGroup});
  }

  @Test
  public void testProxyGetStats() throws Exception {

    long[] combinedData = routerProtocol.getStats();

    long[] individualData = new long[10];
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      long[] data = n.getClient().getNamenode().getStats();
      for (int i = 0; i < data.length; i++) {
        individualData[i] += data[i];
      }
      assert(data.length == combinedData.length);
    }

    for (int i = 0; i < combinedData.length && i < individualData.length; i++) {
      if (i == 2) {
        // Skip available storage as this fluctuates in mini cluster
        continue;
      }
      assertEquals(combinedData[i], individualData[i]);
    }
  }

  @Test
  public void testProxyGetDatanodeReport() throws Exception {

    DatanodeInfo[] combinedData =
        routerProtocol.getDatanodeReport(DatanodeReportType.ALL);

    Set<Integer> individualData = new HashSet<Integer>();
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DatanodeInfo[] data =
          n.getClient().getNamenode().getDatanodeReport(DatanodeReportType.ALL);
      for (int i = 0; i < data.length; i++) {
        // Collect unique DNs based on their xfer port
        DatanodeInfo info = data[i];
        individualData.add(info.getXferPort());
      }
    }
    assertEquals(combinedData.length, individualData.size());
  }

  @Test
  public void testProxyGetDatanodeStorageReport()
      throws IOException, InterruptedException, URISyntaxException {

    DatanodeStorageReport[] combinedData =
        routerProtocol
        .getDatanodeStorageReport(DatanodeReportType.ALL);

    Set<String> individualData = new HashSet<String>();
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DatanodeStorageReport[] data = n.getClient().getNamenode()
          .getDatanodeStorageReport(DatanodeReportType.ALL);
      for (int i = 0; i < data.length; i++) {
        // Determine unique DN instances
        individualData.add(data[i].getDatanodeInfo().getNetworkLocation());
      }
    }
    assertEquals(combinedData.length, individualData.size());
  }

  @Test
  public void testProxyMkdir() throws Exception {

    int intialListing =
        FederationTestUtils.countContents(routerFileSystem, "/");
    String directoryPath = "/testdir";
    FsPermission permission = new FsPermission("705");

    // Create a directory via the router at the root level
    routerProtocol.mkdirs(directoryPath, permission, false);

    // Verify the root listing has the item via the router
    assertEquals(intialListing + 1,
        FederationTestUtils.countContents(routerFileSystem, "/"));
    assertEquals(true,
        FederationTestUtils.verifyFileExists(routerFileSystem, directoryPath));

    // Verify the directory is only present on 1 namenode
    int foundCount = 0;
    for(NamenodeContext n : cluster.getNamenodes()) {
      if(FederationTestUtils.verifyFileExists(n.getFileSystem(),
          directoryPath)) {
        foundCount++;
      }
    }
    assertEquals(1, foundCount);
    assertEquals(true,
        FederationTestUtils.deleteFile(routerFileSystem, directoryPath));

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("mkdirs", String.class,
        FsPermission.class, boolean.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, permission, false});
  }

  @Test
  public void testProxyChmodFiles() throws Exception {

    FsPermission permission = new FsPermission("444");

    // change permissions
    routerProtocol.setPermission(routerFile, permission);

    // Validate permissions NN
    FileStatus file = FederationTestUtils
        .getFileStatus(namenode.getFileSystem(), namenodeFile);
    assertEquals(permission, file.getPermission());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("setPermission", String.class,
        FsPermission.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, permission});
  }

  @Test
  public void testProxySetReplication() throws Exception {

    // Check current replication via NN
    FileStatus file =
        FederationTestUtils.getFileStatus(namenodeFileSystem, namenodeFile);
    assertEquals(1, file.getReplication());

    // increment replication via router
    routerProtocol.setReplication(routerFile, (short) 2);

    // Verify via NN
    file = FederationTestUtils.getFileStatus(namenodeFileSystem, namenodeFile);
    assertEquals(2, file.getReplication());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("setReplication", String.class,
        short.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, (short) 2});
  }

  @Test
  public void testProxyGetBlockLocations() throws Exception {

    // Fetch block locations via router
    LocatedBlocks locations =
        routerProtocol.getBlockLocations(routerFile, 0, 1024);
    assertEquals(1, locations.getLocatedBlocks().size());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("getBlockLocations", String.class,
        long.class, long.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, (long) 0, (long) 0});
  }

  @Test
  public void testProxyStoragePolicy() throws Exception {

    // Query initial policy via NN
    HdfsFileStatus status = namenode.getClient().getFileInfo(namenodeFile);

    // Set a random policy via router
    BlockStoragePolicy[] policies = namenode.getClient().getStoragePolicies();
    BlockStoragePolicy policy = policies[0];

    while (policy.isCopyOnCreateFile()) {
      // Pick a non copy on create policy
      Random rand = new Random();
      policy = policies[rand.nextInt(policies.length)];
    }
    routerProtocol.setStoragePolicy(routerFile, policy.getName());

    // Verify policy via NN
    HdfsFileStatus newStatus = namenode.getClient().getFileInfo(namenodeFile);
    assertTrue(newStatus.getStoragePolicy() == policy.getId());
    assertTrue(newStatus.getStoragePolicy() != status.getStoragePolicy());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("setStoragePolicy", String.class,
        String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, "badpolicy"});
  }

  @Test
  public void testProxyGetPreferedBlockSize() throws Exception {

    // Query via NN
    long namenodeSize = namenodeProtocol.getPreferredBlockSize(namenodeFile);

    // Query via router
    long routerSize = routerProtocol.getPreferredBlockSize(routerFile);
    assertEquals(routerSize, namenodeSize);

    // Validate router failure response matches NN failure response.
    Method m =
        ClientProtocol.class.getMethod("getPreferredBlockSize", String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath});
  }

  private void testConcat(String source, String target,
      boolean failureExpected) {
    boolean failure = false;
    try {
      // Concat test file with fill block length file via router
      routerProtocol.concat(target, new String[] {source});
    } catch (IOException ex) {
      failure = true;
    }
    assertEquals(failureExpected, failure);
  }

  @Test
  public void testProxyConcatFile() throws Exception {

    // Create a stub file in the primary ns
    String sameNameservice = ns;
    String existingFile =
        cluster.getFederatedTestDirectoryForNameservice(sameNameservice)
            + "_concatfile";
    int existingFileSize = 32;
    FederationTestUtils.createFile(routerFileSystem, existingFile,
        existingFileSize);

    // Identify an alternate nameservice that doesn't match the existing file
    String alternateNameservice = null;
    for (String n : cluster.getNameservices()) {
      if (!n.equals(sameNameservice)) {
        alternateNameservice = n;
        break;
      }
    }

    // Create new files, must be a full block to use concat. One file is in the
    // same namespace as the target file, the other is in a different namespace.
    String altRouterFile =
        cluster.getFederatedTestDirectoryForNameservice(alternateNameservice)
            + "_newfile";
    String sameRouterFile =
        cluster.getFederatedTestDirectoryForNameservice(sameNameservice)
            + "_newfile";
    long blockSize = FederationTestUtils.BLOCK_SIZE_BYTES;
    FederationTestUtils.createFile(routerFileSystem, altRouterFile, blockSize);
    FederationTestUtils.createFile(routerFileSystem, sameRouterFile, blockSize);

    // Concat in different namespaces, fails
    testConcat(existingFile, altRouterFile, true);

    // Concat in same namespaces, succeeds
    testConcat(existingFile, sameRouterFile, false);

    // Check target file length
    FileStatus status =
        FederationTestUtils.getFileStatus(routerFileSystem, sameRouterFile);
    assertEquals(existingFileSize + blockSize,
        status.getLen());

    // Validate router failure response matches NN failure response.
    Method m =
        ClientProtocol.class.getMethod("concat", String.class, String[].class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, namenodeProtocol, m,
        new Object[] {badPath, new String[] {routerFile}});
  }

  @Test
  public void testProxyGetAdditionalDatanode()
      throws IOException, InterruptedException, URISyntaxException {

    // Use primitive apis to open a file, add a block, then get a datanode
    // location
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
    String newRouterFile = routerFile + "_additionalDatanode";
    HdfsFileStatus status = routerProtocol.create(newRouterFile,
        new FsPermission("777"), router.getClient().getClientName(),
          new EnumSetWritable<CreateFlag>(createFlag), true, (short) 1,
          (long) 1024, CryptoProtocolVersion.supported());

    // Add a block via router (requires client to have same lease as for
    // next command
    LocatedBlock block = routerProtocol.addBlock(newRouterFile,
            router.getClient().getClientName(), null, null, status.getFileId(),
            null);

    DatanodeInfo[] exclusions = new DatanodeInfo[0];


    LocatedBlock newBlock =
        routerProtocol.getAdditionalDatanode(newRouterFile, status.getFileId(),
        block.getBlock(), block.getLocations(), block.getStorageIDs(),
        exclusions, 1, router.getClient().getClientName());
    assertNotNull(newBlock);

    // TODO - check new block location via NN.
  }

  @Test
  public void testProxyCreateFileAlternateUser()
      throws IOException, URISyntaxException, InterruptedException {

    // Create via Router
    String routerDir = cluster.getFederatedTestDirectoryForNameservice(ns);
    String namenodeDir = cluster.getNamenodeTestDirectoryForNameservice(ns);
    String newRouterFile = routerDir + "/unknownuser";
    String newNamenodeFile = namenodeDir + "/unknownuser";
    String username = "unknownuser";

    // Allow all user access to dir
    namenode.getFileContext().setPermission(new Path(namenodeDir),
        new FsPermission("777"));

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
    DFSClient client = router.getClient(ugi);
    client.create(newRouterFile, true);

    // Fetch via NN and check user
    FileStatus status =
        FederationTestUtils.getFileStatus(namenodeFileSystem, newNamenodeFile);
    assertEquals(status.getOwner(), username);
  }

  @Test
  public void testProxyGetFileInfoAcessException() throws IOException {

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("unknownuser");

    // List files from the NN and trap the exception
    Exception nnFailure = null;
    try {
      namenode.getClient(ugi)
          .getLocatedBlocks(cluster.getNamenodeTestFileForNameservice(ns), 0);
    } catch (Exception e) {
      nnFailure = e;
    }

    // List files from the router and trap the exception
    Exception routerFailure = null;
    try {
      router.getClient(ugi)
          .getLocatedBlocks(cluster.getFederatedTestFileForNameservice(ns), 0);
    } catch (Exception e) {
      routerFailure = e;
    }

    assertTrue(routerFailure != null);
    assertTrue(nnFailure != null);

    // TODO: This will fail when running full unit test as either the
    // router's or namenode's DFS client may have been opened with a
    // different ugi. Need to clear the DFSClient lease/socket cache or run
    // this test alone.
    assertTrue(routerFailure.getClass().equals(nnFailure.getClass()));
  }
}
