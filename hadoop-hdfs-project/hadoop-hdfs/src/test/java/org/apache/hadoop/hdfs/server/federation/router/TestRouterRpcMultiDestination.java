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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;

/**
 * The the RPC interface of the {@link getRouter()} implemented by
 * {@link RouterRpcServer}.
 */
public class TestRouterRpcMultiDestination extends TestRouterRpc {



  @Override
  public void testSetup() throws Exception {

    // Create mock locations
    getCluster().installMockLocations();

    // Add extra location for the root mount /
    // such that the root mount points to
    // /
    // ns0->/
    // ns1->/
    for (RouterContext router : getCluster().getRouters()) {
      MockResolver resolver =
          (MockResolver) router.getRouter().getSubclusterResolver();
      resolver.addLocation("/", getCluster().getNameservices().get(1), "/");
    }

    // Create a mount that points to 2 dirs in the same ns
    // /same
    // ns0->/target-ns0
    // ns0->/
    for (RouterContext router : getCluster().getRouters()) {
      MockResolver resolver =
          (MockResolver) router.getRouter().getSubclusterResolver();
      String ns = getCluster().getNameservices().get(0);
      resolver.addLocation("/same", ns, "/");
      resolver.addLocation("/same", ns,
          getCluster().getNamenodePathForNameservice(ns));
    }

    // Delete all files via the NNs and verify
    getCluster().deleteAllFiles();

    // Create test fixtures on NN
    getCluster().createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    // Pick a NS, namenode and getRouter() for this test
    RouterContext router = getCluster().getRandomRouter();
    this.setRouter(router);

    String ns = getCluster().getRandomNameservice();
    this.setNs(ns);
    this.setNamenode(getCluster().getNamenode(ns, null));

    // Create a test file on a single NN that is accessed via a getRouter() path
    // with 2 destinations. All tests should failover to the alternate
    // destination if the wrong NN is attempted first.
    Random r = new Random();
    String randomString = "testfile-" + r.nextInt();
    setNamenodeFile("/" + randomString);
    setRouterFile("/" + randomString);
    FederationTestUtils.createFile(getNamenodeFileSystem(), getNamenodeFile(),
        32);
    FederationTestUtils.verifyFileExists(getNamenodeFileSystem(),
        getNamenodeFile());
    FederationTestUtils.verifyFileExists(getRouterFileSystem(),
        getRouterFile());
  }

  private void testListing(String path) throws IOException {
    // Collect the mount table entries for this path
    SortedSet<String> requiredPaths = new TreeSet<String>();
    for (String mount : getRouter().getRouter().getSubclusterResolver()
        .getMountPoints(path)) {
      requiredPaths.add(mount);
    }

    // Collect all files/dirs fr
    MockResolver resolver =
        (MockResolver) getRouter().getRouter().getSubclusterResolver();
    PathLocation location = resolver.getDestinationForPath(path);
    for (RemoteLocation loc : location.getDestinations()) {
      NamenodeContext nn =
          getCluster().getNamenode(loc.getNameserviceId(), null);
      FileStatus[] iterator =
          nn.getFileSystem().listStatus(new Path(loc.getDest()));
      for (FileStatus file : iterator) {
        requiredPaths.add(file.getPath().getName());
      }
    }

    // Fetch listing
    DirectoryListing listing =
        getRouterProtocol().getListing(path, HdfsFileStatus.EMPTY_NAME, false);
    Iterator<String> requiredPathsIterator = requiredPaths.iterator();
    // Match each path returned and verify order returned
    for (HdfsFileStatus f : listing.getPartialListing()) {
      String fileName = requiredPathsIterator.next();
      String currentFile = f.getFullPath(new Path(path)).getName();
      assertEquals(currentFile, fileName);
    }

    // Verify the total number of results found/matched
    assertEquals(requiredPaths.size(), listing.getPartialListing().length);
  }

  @Override
  public void testProxyListFiles() throws IOException, InterruptedException,
      URISyntaxException, NoSuchMethodException, SecurityException {

    // Verify that the root listing is a union of the mount table destinations
    // and the files stored at all nameservices mounted at the root (ns0 + ns1)
    //
    // / -->
    // /ns0 (from mount table)
    // /ns1 (from mount table)
    // /same (from the mount table)
    // all items in / of ns0 from mapping of / -> ns0:::/)
    // all items in / of ns1 from mapping of / -> ns1:::/)
    //
    testListing("/");

    // Verify that the /same mount point lists the contents of both dirs in the
    // same ns.
    //
    // /same -->
    // /target-ns0 (from root of ns0)
    // /testdir (from contents of /target-ns0)
    testListing("/same");

    // List a path that doesn't exist and validate error response with NN
    // behavior.
    ClientProtocol namenodeProtocol =
        getCluster().getRandomNamenode().getClient().getNamenode();
    Method m = ClientProtocol.class.getMethod("getListing", String.class,
        byte[].class, boolean.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(getRouterProtocol(), namenodeProtocol, m,
        new Object[] {badPath, HdfsFileStatus.EMPTY_NAME, false});
  }

  @Override
  public void testProxyRenameFiles() throws IOException, InterruptedException {

    super.testProxyRenameFiles();

    String ns0 = getCluster().getNameservices().get(0);
    String ns1 = getCluster().getNameservices().get(1);

    // Rename a file from ns0 into the root (mapped to both ns0 and ns1)
    String filename = getCluster().getFederatedTestDirectoryForNameservice(ns0)
        + "/testrename";
    String renamedFile = "/testrename";
    testRename(getRouter(), filename, renamedFile, false);
    testRename2(getRouter(), filename, renamedFile, false);

    // Rename a file from ns1 into the root (mapped to both ns0 and ns1)
    filename = getCluster().getFederatedTestDirectoryForNameservice(ns1)
        + "/testrename";
    testRename(getRouter(), filename, renamedFile, false);
    testRename2(getRouter(), filename, renamedFile, false);
  }
}

