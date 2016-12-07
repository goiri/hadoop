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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.FederationPathLockStore;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

/**
 * This class is responsible for handling all of the RPC calls to the It is
 * created, started, and stopped by {@link Router}. It implements the
 * {@link ClientProtocol} to mimic a
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode} and proxies
 * the requests to the active
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode}.
 */
public class RouterRpcServer extends AbstractService implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(RouterRpcServer.class);

  private Configuration conf;

  /** The RPC server that listens to requests from clients. */
  private final Server rpcServer;
  private final InetSocketAddress rpcAddress;

  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  /** Monitor metrics for the RPC calls. */
  private RouterRpcMonitor rpcMonitor;

  /** Number of RPC handlers. */
  private int handlerCount;
  /** Number of readers. */
  private int readerCount;
  /** Size of the handler queue. */
  private int handlerQueueSize;
  /** Size of the reader queue. */
  private int readerQueueSize;

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private ActiveNamenodeResolver namenodeResolver;

  /** Interface to map global name space to HDFS subcluster name spaces. */
  private FileSubclusterResolver subclusterResolver;

  /** Interface to the path lock cache/store. */
  private FederationPathLockStore pathLockStore;

  /** If we are in safe mode, fail requests as if a standby NN. */
  private volatile boolean safeMode;

  /**
   * Construct a router RPC server.
   *
   * @param configuration HDFS Configuration.
   * @param nnResolver The NN resolver instance to determine active NNs in HA.
   * @param fileResolver File resolver to resolve file paths to subclusters.
   * @param pathLock Path lock state store API handle to detect locked paths.
   * @throws IOException If the RPC server could not be created.
   */
  public RouterRpcServer(Configuration configuration,
      ActiveNamenodeResolver nnResolver, FileSubclusterResolver fileResolver,
      FederationPathLockStore pathLock) throws IOException {
    super(RouterRpcServer.class.getName());

    this.conf = configuration;
    this.namenodeResolver = nnResolver;
    this.subclusterResolver = fileResolver;
    this.pathLockStore = pathLock;

    this.handlerCount = this.conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY,
        DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    this.readerCount = this.conf.getInt(DFS_ROUTER_READER_COUNT_KEY,
        DFS_ROUTER_READER_COUNT_DEFAULT);

    this.handlerQueueSize = this.conf.getInt(DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY,
        DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT);

    // Override hadoop common - IPC setting
    this.readerQueueSize = this.conf.getInt(DFS_ROUTER_READER_QUEUE_SIZE_KEY,
        DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
        readerQueueSize);

    RPC.setProtocolEngine(this.conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine.class);

    ClientNamenodeProtocolServerSideTranslatorPB
    clientProtocolServerTranslator =
        new ClientNamenodeProtocolServerSideTranslatorPB(this);
    BlockingService clientNNPbService = ClientNamenodeProtocol
        .newReflectiveBlockingService(clientProtocolServerTranslator);

    WritableRpcEngine.ensureInitialized();

    InetSocketAddress confRpcAddress =
        FederationUtil.getRouterRpcServerAddress(conf);
    String bindHost = FederationUtil.getRouterRpcServerBindHost(conf);
    if (bindHost == null) {
      bindHost = confRpcAddress.getHostName();
    }
    LOG.info("RPC server binding to " + bindHost + ":"
        + confRpcAddress.getPort() + " with " + handlerCount + " handlers.");

    this.rpcServer = new RPC.Builder(this.conf)
        .setProtocol(ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(bindHost)
        .setPort(confRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setnumReaders(readerCount)
        .setQueueSizePerHandler(handlerQueueSize * handlerCount)
        .setVerbose(false)
        .build();

    // The RPC-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.rpcServer.getListenerAddress();
    this.rpcAddress = new InetSocketAddress(confRpcAddress.getHostName(),
        listenAddress.getPort());

    // Create metrics monitor
    this.rpcMonitor = (RouterRpcMonitor) FederationUtil.createInstance(
        this.conf, DFSConfigKeys.DFS_ROUTER_METRICS_CLASS,
        DFSConfigKeys.DFS_ROUTER_METRICS_CLASS_DEFAULT, RouterRpcMonitor.class);
    if (this.rpcMonitor == null) {
      LOG.error("Unable to instantiate router RPC metrics class.");
    } else {
      this.rpcMonitor.initialize(conf, this);
    }

    // Create the client
    this.rpcClient = new RouterRpcClient(conf, namenodeResolver, rpcMonitor);
  }

  /**
   * Get the RPC client to the Namenode.
   *
   * @return RPC clients to the Namenodes.
   */
  public RouterRpcClient getRPCClient() {
    return rpcClient;
  }

  /**
   * Get the RPC monitor and metrics.
   *
   * @return RPC monitor and metrics.
   */
  public RouterRpcMonitor getRPCMonitor() {
    return rpcMonitor;
  }

  /**
   * Get the number of RPC server handlers.
   *
   * @return Number of RPC server handlers.
   */
  public int getHandlerCount() {
    return this.handlerCount;
  }

  /**
   * Get the number of RPC server readers.
   *
   * @return Number of RPC server readers.
   */
  public int getReaderCount() {
    return this.readerCount;
  }

  /**
   * Get the size of the handler queue.
   *
   * @return Size of the handler queue.
   */
  public int getHandlerQueueSize() {
    return this.handlerQueueSize;
  }

  /**
   * Get the size of the reader queue.
   *
   * @return Size of the reader queue.
   */
  public int getReaderQueueSize() {
    return this.readerQueueSize;
  }

  /**
   * Allow access to the client RPC server for testing.
   *
   * @return The RPC server.
   */
  @VisibleForTesting
  public Server getServer() {
    return this.rpcServer;
  }

  /**
   * Get the RPC address of the service.
   *
   * @return RPC service address.
   */
  public InetSocketAddress getRpcAddress() {
    return this.rpcAddress;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;
    super.serviceInit(configuration);
  }

  /**
   * Start client and service RPC servers.
   */
  @Override
  protected void serviceStart() throws Exception {
    this.rpcServer.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rpcServer != null) {
      this.rpcServer.stop();
    }
    if (rpcMonitor != null) {
      this.rpcMonitor.close();
    }
    super.serviceStop();
  }

  /**
   * Wait until the RPC servers have shutdown.
   */
  void join() throws InterruptedException {
    if (rpcServer != null) {
      rpcServer.join();
    }
  }

  /**
   * Default handler when we haven't implemented an operation. It always throws
   * an exception reporting the operation is not supported.
   */
  private void genericHandler() {
    String methodName = FederationUtil.getMethodName();
    throw new UnsupportedOperationException(
        "Operation \"" + methodName + "\" is not supported");
  }

  /**
   * Log the function we are currently calling.
   *
   * @throws StandbyException If the router is currently in safemode.
   */
  private void logFunction() throws StandbyException {
    if (LOG.isDebugEnabled()) {
      String methodName = FederationUtil.getMethodName();
      LOG.debug("Proxying operation: " + methodName);
    }
    if (rpcMonitor != null) {
      rpcMonitor.startOp();
    }
    if (safeMode) {
      // Throw standby exception, router is not available
      if (rpcMonitor != null) {
        rpcMonitor.routerFailureSafemode();
      }
      throw new StandbyException(
          "Router is in safe mode and cannot handle requests.");
    }
  }

  /**
   * In safe mode all RPC requests will fail and return a standby exception.
   * The client will try another Router, similar to the client retry logic for
   * HA.
   *
   * @param mode True if enabled, False if disabled.
   */
  public void setSafeMode(boolean mode) {
    this.safeMode = mode;
  }

  /**
   * Check if the Router is in safe mode and cannot serve RPC calls.
   *
   * @return If the Router is in safe mode.
   */
  public boolean isSafeMode() {
    return this.safeMode;
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    logFunction();
    genericHandler();
    return 0;
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, final long offset,
      final long length) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src);
    RemoteMethod remoteMethod = new RemoteMethod("getBlockLocations",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), offset, length);
    return (LocatedBlocks) rpcClient.invokeSequential(locations, remoteMethod,
        LocatedBlocks.class, null);
  }

  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    logFunction();
    RemoteMethod method = new RemoteMethod("getServerDefaults");
    String ns = subclusterResolver.getDefaultNamespace();
    return (FsServerDefaults) rpcClient.invokeSingle(ns, method);
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, final FsPermission masked,
      final String clientName, final EnumSetWritable<CreateFlag> flag,
      final boolean createParent, final short replication, final long blockSize,
      final CryptoProtocolVersion[] supportedVersions) throws IOException {
    logFunction();

    final LinkedList<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteLocation createLocation = locations.getFirst();
    if (locations.size() > 1) {
      try {
        // Check if this file already exists
        LocatedBlocks existingLocation = getBlockLocations(src, 0, 1);
        if (existingLocation != null) {
          // Forward the request to the existing location and let the NN handle
          // the error.
          String blockPoolId = existingLocation.getLastLocatedBlock().getBlock()
              .getBlockPoolId();
          createLocation = this.getLocationForPath(src, true, blockPoolId);
        }
      } catch (IOException ex) {
        if (!(ex instanceof FileNotFoundException)) {
          // Error querying NNs, this file may exist, re-throw the exception.
          throw ex;
        }
      }
    }
    RemoteMethod method = new RemoteMethod("create",
        new Class<?>[] {String.class, FsPermission.class, String.class,
                        EnumSetWritable.class, boolean.class, short.class,
                        long.class, CryptoProtocolVersion[].class},
        createLocation.getDest(), masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions);
    return (HdfsFileStatus) rpcClient.invokeSingle(createLocation, method);
  }

  // Medium
  @Override // ClientProtocol
  public LocatedBlock append(String src, final String clientName) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("append",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), clientName);
    return (LocatedBlock) rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
  }

  // Low
  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("recoverLease",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        clientName);
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE);
    return (Boolean) result;
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setReplication",
        new Class<?>[] {String.class, short.class}, new RemoteParam(),
        replication);
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE);
    return (Boolean) result;
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setStoragePolicy",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), policyName);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    logFunction();
    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    String ns = subclusterResolver.getDefaultNamespace();
    return (BlockStoragePolicy[]) rpcClient.invokeSingle(ns, method);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setPermission",
        new Class<?>[] {String.class, FsPermission.class}, new RemoteParam(),
        permissions);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setOwner",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), username, groupname);
    rpcClient.invokeSequential(locations, method);
  }

  /**
   * Excluded and favored nodes are not verified and will be ignored by
   * placement policy if they are not in the same nameservice as the file.
   */
  @Override // ClientProtocol
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
      String[] favoredNodes) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("addBlock",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
                        DatanodeInfo[].class, long.class, String[].class},
        new RemoteParam(), clientName, previous, excludedNodes, fileId,
        favoredNodes);
    // TODO verify the excludedNodes and favoredNodes are acceptable to this NN
    return (LocatedBlock) rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
  }

  /**
   * Excluded nodes are not verified and will be ignored by placement if they
   * are not in the same nameservice as the file.
   */
  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src, final long fileId,
      final ExtendedBlock blk, final DatanodeInfo[] existings,
      final String[] existingStorageIDs, final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName)
      throws IOException {

    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getAdditionalDatanode",
        new Class<?>[] {String.class, long.class, ExtendedBlock.class,
                        DatanodeInfo[].class, String[].class,
                        DatanodeInfo[].class, int.class, String.class},
        new RemoteParam(), fileId, blk, existings, existingStorageIDs, excludes,
        numAdditionalNodes, clientName);
    return (LocatedBlock) rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
  }

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws IOException {
    logFunction();
    RemoteMethod method = new RemoteMethod("abandonBlock",
        new Class<?>[] {ExtendedBlock.class, long.class, String.class,
                        String.class},
        b, fileId, new RemoteParam(), holder);
    rpcClient.invokeSingle(b, method);
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last,
      long fileId) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("complete",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
                        long.class}, new RemoteParam(), clientName, last,
                        fileId);
    // Complete can return true/false, so don't expect a result
    return ((Boolean) rpcClient.invokeSequential(
        locations, method, Boolean.class, null)).booleanValue();
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(
      ExtendedBlock block, String clientName) throws IOException {
    logFunction();
    RemoteMethod method = new RemoteMethod("updateBlockForPipeline",
        new Class<?>[] {ExtendedBlock.class, String.class},
        block, clientName);
    return (LocatedBlock) rpcClient.invokeSingle(block, method);
  }

  /**
   * Datanode are not verified to be in the same nameservice as the old block.
   * TODO This may require validation.
   */
  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {

    logFunction();
    RemoteMethod method = new RemoteMethod("updatePipeline",
        new Class<?>[] {String.class, ExtendedBlock.class, ExtendedBlock.class,
                        DatanodeID[].class, String[].class},
        clientName, oldBlock, newBlock, newNodes, newStorageIDs);
    rpcClient.invokeSingle(oldBlock, method);
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String src) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("getPreferredBlockSize",
        new Class<?>[] {String.class}, new RemoteParam());
    return ((Long) rpcClient.invokeSequential(
        locations, method, Long.class, null)).longValue();
  }

  /**
   * Determines combinations of elibible src/dst locations for a rename. A
   * rename cannot change the namespace. Renames are only allowed if there is an
   * eligible dst location in the same namespace as the source.
   *
   * @param srcLocations List of all potential source destinations where the
   *          path may be located. On return this list is trimmed to include
   *          only the paths that have corresponding destinations in the same
   *          namespace.
   * @param dst The destination path
   * @return A map of all eligible source namespaces and their corresponding
   *         replacement value.
   * @throws IOException If the dst paths could not be determined.
   */
  private RemoteParam getRenameDestinations(List<RemoteLocation> srcLocations,
      String dst) throws IOException {
    List<RemoteLocation> dstLocations = getLocationsForPath(dst, true);
    Map<RemoteLocation, String> dstMap = new HashMap<RemoteLocation, String>();

    Iterator<RemoteLocation> iterator = srcLocations.iterator();
    while (iterator.hasNext()) {
      RemoteLocation potentialSrc = iterator.next();
      RemoteLocation eligibleDst =
          getFirstMatchingLocation(potentialSrc, dstLocations);
      if (eligibleDst != null) {
        // Use this dst with this source location
        dstMap.put(potentialSrc, eligibleDst.getDest());
      } else {
        // This src destination is not valid, remove from list
        iterator.remove();
      }
    }
    return new RemoteParam(dstMap);
  }

  /**
   * Get first matching location.
   *
   * @param location Location we are looking for.
   * @param locations List of locations.
   * @return The first matchin location in the list.
   */
  private RemoteLocation getFirstMatchingLocation(RemoteLocation location,
      List<RemoteLocation> locations) {
    for (RemoteLocation loc : locations) {
      if (loc.getNameserviceId().equals(location.getNameserviceId())) {
        // Return first matching location
        return loc;
      }
    }
    return null;
  }

  @Deprecated
  @Override // ClientProtocol
  public boolean rename(final String src, final String dst)
      throws IOException {

    logFunction();
    // srcLocations may be trimmed by mapEligibleRenameLocations
    List<RemoteLocation> srcLocations = getLocationsForPath(src, true);
    RemoteParam dstParam = getRenameDestinations(srcLocations, dst);
    if (srcLocations.isEmpty()) {
      throw new IOException(
          "Rename of " + src + " to " + dst + " is not allowed,"
              + " no eligible destination in the same namespace was found.");
    }
    RemoteMethod method = new RemoteMethod("rename",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), dstParam);
    return ((Boolean) rpcClient.invokeSequential(
        srcLocations, method, Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public void rename2(final String src, final String dst,
      final Options.Rename... options) throws IOException {

    logFunction();
    // srcLocations may be trimmed by mapEligibleRenameLocations
    final List<RemoteLocation> srcLocations = getLocationsForPath(src, true);
    RemoteParam dstParam = getRenameDestinations(srcLocations, dst);
    if (srcLocations.isEmpty()) {
      throw new IOException(
          "Rename of " + src + " to " + dst + " is not allowed,"
              + " no eligible destination in the same namespace was found.");
    }
    RemoteMethod method = new RemoteMethod("rename2",
        new Class<?>[] {String.class, String.class, options.getClass()},
        new RemoteParam(), dstParam, options);
    rpcClient.invokeSequential(srcLocations, method, null, null);
  }

  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {

    logFunction();
    // See if the src and target files are all in the same namespace
    // TODO What length to use with this API?
    LocatedBlocks targetBlocks = getBlockLocations(trg, 0, 1);
    if (targetBlocks == null) {
      throw new IOException("Cannot locate blocks for target file - " + trg);
    }
    LocatedBlock lastLocatedBlock = targetBlocks.getLastLocatedBlock();
    String targetBlockPoolId = lastLocatedBlock.getBlock().getBlockPoolId();
    for (String source : src) {
      LocatedBlocks sourceBlocks = getBlockLocations(source, 0, 1);
      if (sourceBlocks == null) {
        throw new IOException(
            "Cannot located blocks for source file " + source);
      }
      String sourceBlockPoolId =
          sourceBlocks.getLastLocatedBlock().getBlock().getBlockPoolId();
      if (!sourceBlockPoolId.equals(targetBlockPoolId)) {
        throw new IOException("Cannot concatenate source file - " + source
            + " because it is located in a different namespace"
            + " with block pool id - " + sourceBlockPoolId
            + " from the target file with block pool id - "
            + targetBlockPoolId);
      }
    }

    // Find locations in the matching namespace.
    RemoteLocation targetDestination =
        getLocationForPath(trg, true, targetBlockPoolId);
    String[] sourceDestinations = new String[src.length];
    for (int i = 0; i < src.length; i++) {
      String sourceFile = src[i];
      RemoteLocation location =
          getLocationForPath(sourceFile, true, targetBlockPoolId);
      sourceDestinations[i] = location.getDest();
    }
    // Invoke
    RemoteMethod method = new RemoteMethod("concat",
        new Class<?>[] {String.class, String[].class},
        targetDestination.getDest(), sourceDestinations);
    rpcClient.invokeSingle(targetDestination, method);
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("delete",
        new Class<?>[] {String.class, boolean.class}, new RemoteParam(),
        recursive);
    return ((Boolean) rpcClient.invokeSequential(locations, method,
        Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    logFunction();
    LinkedList<RemoteLocation> locations = getLocationsForPath(src, true);
    if (locations.size() > 1) {
      // Check if this directory already exists
      try {
        HdfsFileStatus fileStatus = getFileInfo(src);
        if (fileStatus != null) {
          throw new FileAlreadyExistsException(
              "Directory " + src + " already exists");
        }
      } catch (IOException ex) {
        // Can't query if this file exists or not.
        LOG.error("Error requesting file info for path - " + src
            + " while proxing mkdirs.", ex);
        throw ex;
      }
    }
    RemoteLocation firstLocation = locations.getFirst();
    RemoteMethod method = new RemoteMethod("mkdirs",
        new Class<?>[] {String.class, FsPermission.class, boolean.class},
        new RemoteParam(), masked, createParent);
    return ((Boolean) rpcClient.invokeSingle(firstLocation, method))
        .booleanValue();
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws IOException {
    // TODO - determine which NN to send this to
    logFunction();
    RemoteMethod method = new RemoteMethod("renewLease",
        new Class<?>[] {String.class}, clientName);
    String ns = subclusterResolver.getDefaultNamespace();
    rpcClient.invokeSingle(ns, method);
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    logFunction();

    // Fetch mount points at this path in the tree
    List<String> mountPoints = subclusterResolver.getMountPoints(src);

    // Locate the dir and fetch the listing
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("getListing",
        new Class<?>[] {String.class, startAfter.getClass(), boolean.class},
        new RemoteParam(), startAfter, needLocation);
    Object[] listings = rpcClient.invokeConcurrent(locations, method, true);

    Map<String, HdfsFileStatus> nnListing =
        new TreeMap<String, HdfsFileStatus>();
    int remainingEntries = 0;
    boolean namenodeListingExists = false;
    if (listings != null) {
      // Add existing entries
      for (Object obj : listings) {
        DirectoryListing listing = (DirectoryListing) obj;
        if (listing != null) {
          namenodeListingExists = true;
          for (HdfsFileStatus file : listing.getPartialListing()) {
            nnListing.put(file.getLocalName(), file);
          }
          remainingEntries += listing.getRemainingEntries();
        }
      }
    }

    // Add mount points at this level in the tree
    if (mountPoints != null && mountPoints.size() > 0) {
      for (String mountPoint : mountPoints) {
        // Create default folder with the mount name
        // TODO think of better default values
        long modificationTime = 0;
        long accessTime = 0;
        FsPermission permission = FsPermission.getDirDefault();
        String owner = "hadoop";
        String group = "supergroup";
        long inodeId = 0;
        int childrenNum = 0;
        HdfsFileStatus dirStatus = new HdfsFileStatus(0, true, 0, 0,
            modificationTime, accessTime, permission, owner, group, new byte[0],
            DFSUtil.string2Bytes(mountPoint), inodeId, childrenNum, null,
            (byte) 0);
        // Note, this may overwrite existing listing entries with the generic
        // dir entry.
        nnListing.put(mountPoint, dirStatus);
      }
    }

    if (!namenodeListingExists && nnListing.size() == 0) {
      // NN returns a null object if the directory cannot be found and has no
      // listing. If we didn't retrieve any NN listing data, and there are no
      // mount points here, return null.
      return null;
    }

    // Generate combined listing
    HdfsFileStatus[] combinedData = new HdfsFileStatus[nnListing.size()];
    combinedData = nnListing.values().toArray(combinedData);
    return new DirectoryListing(combinedData, remainingEntries);
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    return (HdfsFileStatus) rpcClient.invokeSequential(
        locations, method, HdfsFileStatus.class, null);
  }

  @Override // ClientProtocol
  public boolean isFileClosed(String src) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("isFileClosed",
        new Class<?>[] {String.class}, new RemoteParam());
    return ((Boolean) rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getFileLinkInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    return (HdfsFileStatus) rpcClient.invokeSequential(
        locations, method, HdfsFileStatus.class, null);
  }

  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    logFunction();
    RemoteMethod method = new RemoteMethod("getStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Object[] results = rpcClient.invokeConcurrent(nss, method, true);
    int dataLength = ((long[]) results[0]).length;
    long[] combinedData = new long[dataLength];
    for (Object o : results) {
      long[] data = (long[]) o;
      for (int i = 0; i < combinedData.length && i < data.length; i++) {
        if (data[i] >= 0) {
          combinedData[i] += data[i];
        }
      }
    }
    return combinedData;
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    logFunction();
    Map<String, DatanodeInfo> datanodesMap =
        new HashMap<String, DatanodeInfo>();
    RemoteMethod method = new RemoteMethod("getDatanodeReport",
        new Class<?>[] {DatanodeReportType.class}, type);

    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Object[] results = rpcClient.invokeConcurrent(nss, method, true);
    for (Object r : results) {
      DatanodeInfo[] result = (DatanodeInfo[]) r;
      for (DatanodeInfo node : result) {
        String nodeId = node.getXferAddr();
        if (!datanodesMap.containsKey(nodeId)) {
          datanodesMap.put(nodeId, node);
        }
        // TODO merge somehow, right now it just takes the first one
      }
    }
    Collection<DatanodeInfo> datanodes = datanodesMap.values();
    // TODO sort somehow
    DatanodeInfo[] combinedData = new DatanodeInfo[datanodes.size()];
    combinedData = datanodes.toArray(combinedData);
    return combinedData;
  }

  @Override // ClientProtocol
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    logFunction();
    Map<String, DatanodeStorageReport> datanodesMap =
        new HashMap<String, DatanodeStorageReport>();
    RemoteMethod method = new RemoteMethod("getDatanodeStorageReport",
        new Class<?>[] {DatanodeReportType.class}, type);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Object[] results = rpcClient.invokeConcurrent(nss, method, true);
    for (Object r : results) {
      DatanodeStorageReport[] result = (DatanodeStorageReport[]) r;
      for (DatanodeStorageReport node : result) {
        String nodeId = node.getDatanodeInfo().getXferAddr();
        if (!datanodesMap.containsKey(nodeId)) {
          datanodesMap.put(nodeId, node);
        }
        // TODO merge somehow, right now it just takes the first one
      }
    }

    Collection<DatanodeStorageReport> datanodes = datanodesMap.values();
    // TODO sort somehow
    DatanodeStorageReport[] combinedData =
        new DatanodeStorageReport[datanodes.size()];
    combinedData = datanodes.toArray(combinedData);
    return combinedData;
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {

    // Set safe mode in all the name spaces
    RemoteMethod method = new RemoteMethod("setSafeMode",
        new Class<?>[] {SafeModeAction.class, Boolean.class},
        action, isChecked);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Object[] results = rpcClient.invokeConcurrent(nss, method, true);

    // We only report true if all the name space are in safe mode
    int numSafemode = 0;
    for (Object result : results) {
      if (result instanceof Boolean) {
        boolean safemode = (Boolean) result;
        if (safemode) {
          numSafemode++;
        }
      }
    }
    return numSafemode == results.length;
  }

  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) throws IOException {
    logFunction();
    genericHandler();
    return false;
  }

  @Override // ClientProtocol
  public void saveNamespace() throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public long rollEdits() throws IOException {
    logFunction();
    genericHandler();
    return 0;
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(path, false);
    RemoteMethod method = new RemoteMethod("listCorruptFileBlocks",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), cookie);
    return (CorruptFileBlocks) rpcClient.invokeSequential(
        locations, method, CorruptFileBlocks.class, null);
  }

  @Override // ClientProtocol
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(path, false);
    RemoteMethod method = new RemoteMethod("getContentSummary",
        new Class<?>[] {String.class}, new RemoteParam());
    return (ContentSummary) rpcClient.invokeSequential(
        locations, method, ContentSummary.class, null);
  }

  @Override // ClientProtocol
  public void fsync(String src, long fileId, String clientName,
      long lastBlockLength) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("fsync",
        new Class<?>[] {String.class, long.class, String.class, long.class },
        new RemoteParam(), fileId, clientName, lastBlockLength);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setTimes",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), mtime, atime);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    logFunction();
    // TODO - verify that the link location is in the same NS as the targets
    List<RemoteLocation> targetLocations = getLocationsForPath(target, true);
    RemoteLocation linkLocation = getLocationsForPath(link, true).getFirst();
    RemoteMethod method = new RemoteMethod("createSymlink",
        new Class<?>[] {String.class, String.class, FsPermission.class,
                        boolean.class},
        new RemoteParam(), linkLocation.getDest(), dirPerms, createParent);
    rpcClient.invokeSequential(targetLocations, method);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(path, true);
    RemoteMethod method = new RemoteMethod("getLinkTarget",
        new Class<?>[] {String.class}, new RemoteParam());
    return (String) rpcClient.invokeSequential(
        locations, method, String.class, null);
  }

  @Override
  // Client Protocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override
  // Client Protocol
  public void disallowSnapshot(String snapshot) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override
  // ClientProtocol
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {

    logFunction();
    genericHandler();
  }

  @Override // Client Protocol
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public long addCacheDirective(CacheDirectiveInfo path,
      EnumSet<CacheFlag> flags) throws IOException {
    logFunction();
    genericHandler();
    return 0;
  }

  @Override // ClientProtocol
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public void removeCacheDirective(long id) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public void addCachePool(CachePoolInfo info) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public void removeCachePool(String cachePoolName) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("modifyAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override // ClienProtocol
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override // ClientProtocol
  public void removeDefaultAcl(String src) throws IOException {

    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeDefaultAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void removeAcl(String src) throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod(
        "setAcl", new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public AclStatus getAclStatus(String src) throws IOException {
    logFunction();
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getAclStatus",
        new Class<?>[] {String.class}, new RemoteParam());
    return (AclStatus) rpcClient.invokeSequential(
        locations, method, AclStatus.class, null);
  }

  @Override // ClientProtocol
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("createEncryptionZone",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), keyName);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public EncryptionZone getEZForPath(String src) throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src);
    RemoteMethod method = new RemoteMethod("getEZForPath",
        new Class<?>[] {String.class}, new RemoteParam());
    return (EncryptionZone) rpcClient.invokeSequential(
        locations, method, EncryptionZone.class, null);
  }

  @Override // ClientProtocol
  public BatchedEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override // ClientProtocol
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setXAttr",
        new Class<?>[] {String.class, XAttr.class, EnumSet.class},
        new RemoteParam(), xAttr, flag);
    rpcClient.invokeSequential(locations, method);
  }

  @SuppressWarnings("unchecked")
  @Override // ClientProtocol
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getXAttrs",
        new Class<?>[] {String.class, List.class}, new RemoteParam(), xAttrs);
    return (List<XAttr>) rpcClient.invokeSequential(
        locations, method, List.class, null);
  }

  @SuppressWarnings("unchecked")
  @Override // ClientProtocol
  public List<XAttr> listXAttrs(String src) throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("listXAttrs",
        new Class<?>[] {String.class}, new RemoteParam());
    return (List<XAttr>) rpcClient.invokeSequential(
        locations, method, List.class, null);
  }

  @Override // ClientProtocol
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeXAttr",
        new Class<?>[] {String.class, XAttr.class}, new RemoteParam(), xAttr);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void checkAccess(String path, FsAction mode) throws IOException {
    logFunction();
    // TODO handle virtual directories
    List<RemoteLocation> locations = getLocationsForPath(path, true);
    RemoteMethod method = new RemoteMethod("checkAccess",
        new Class<?>[] {String.class, FsAction.class},
        new RemoteParam(), mode);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public long getCurrentEditLogTxid() throws IOException {
    logFunction();
    genericHandler();
    return 0;
  }

  @Override // ClientProtocol
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    logFunction();
    genericHandler();
    return null;
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    logFunction();
    genericHandler();
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota) throws IOException {
    logFunction();
    genericHandler();
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {

    // Block pool id -> blocks
    Map<String, List<LocatedBlock>> blockLocations =
        new HashMap<String, List<LocatedBlock>>();
    for (LocatedBlock block : blocks) {
      String bpId = block.getBlock().getBlockPoolId();
      List<LocatedBlock> bpBlocks = blockLocations.get(bpId);
      if (bpBlocks == null) {
        bpBlocks = new LinkedList<LocatedBlock>();
        blockLocations.put(bpId, bpBlocks);
      }
      bpBlocks.add(block);
    }

    // Invoke each block pool
    // TODO use invokeConcurrent/invokeSequential to manage exceptions
    for (Entry<String, List<LocatedBlock>> entry : blockLocations.entrySet()) {
      String bpId = entry.getKey();
      List<LocatedBlock> bpBlocks = entry.getValue();

      LocatedBlock[] bpBlocksArray =
          bpBlocks.toArray(new LocatedBlock[bpBlocks.size()]);
      RemoteMethod method = new RemoteMethod("reportBadBlocks",
          new Class<?>[] {LocatedBlock[].class},
          new Object[] { bpBlocksArray });
      rpcClient.invokeSingleBlockPool(bpId, method);
    }
  }

  /**
   * Get the location of a path in the federated cluster.
   *
   * @param src Path to check.
   * @return Location in a subcluster.
   * @throws IOException if the location for this path cannot be determined.
   */
  private LinkedList<RemoteLocation> getLocationsForPath(String src)
      throws IOException {
    return this.getLocationsForPath(src, false);
  }

  /**
   * Locate the location with the matching block pool id.
   *
   * @param src Path to check.
   * @param failIfLocked Fail the request if locked (top mount point).
   * @param blockPoolId The blook pool ID of the namespace to search for.
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException if the location for this path cannot be determined.
   */
  private RemoteLocation getLocationForPath(String src, boolean failIfLocked,
      String blockPoolId) throws IOException {
    List<RemoteLocation> locations = getLocationsForPath(src, failIfLocked);
    String nameserviceId = null;
    // TODO - This can be a bit more efficient with better caching, however this
    // API is currently only used for concat which is not a low-latency API.
    Set<FederationNamespaceInfo> namespaces =
        this.namenodeResolver.getNamespaces();
    for (FederationNamespaceInfo namespace : namespaces) {
      if (namespace.getBlockPoolId().equals(blockPoolId)) {
        nameserviceId = namespace.getNameserviceId();
        break;
      }
    }
    if (nameserviceId == null) {
      throw new IOException(
          "Unable to located eligible nameservice for block pool id - "
              + blockPoolId);
    }
    for (RemoteLocation location : locations) {
      if (location.getNameserviceId().equals(nameserviceId)) {
        return location;
      }
    }
    throw new IOException(
        "Unable to locate an eligble remote location for block pool id - "
            + blockPoolId);
  }

  /**
   * Get the possible locations of a path in the federated cluster.
   *
   * @param src Path to check.
   * @param failIfLocked Fail the request if locked (top mount point).
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException If the location for this path cannot be determined.
   */
  private LinkedList<RemoteLocation> getLocationsForPath(
      String src, boolean failIfLocked) throws IOException {
    PathLocation location = null;
    try {
      location = subclusterResolver.getDestinationForPath(src);
    } catch (IOException ex) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.routerFailureStateStore();
      }
      throw ex;
    }
    if (location == null) {
      throw new UnsupportedOperationException(
          "Unsupported path " + src + " please check mount table");
    }
    if (failIfLocked && pathLockStore != null && pathLockStore.isLocked(src)) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.routerFailureLocked();
      }
      throw new IOException(
          "Unable to complete operation. This path is locked");
    }
    // Log the access to a path
    if (this.rpcMonitor != null) {
      this.rpcMonitor.logPathStat(src);
    }
    return location.getDestinations();
  }
}
