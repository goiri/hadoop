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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A client proxy for Router -> NN communication using the NN ClientProtocol.
 * <p>
 * Provides routers to invoke remote ClientProtocol methods and handle
 * retries/failover.
 * <ul>
 * <li>invokeSingle Make a single request to a single namespace
 * <li>invokeSequential Make a sequential series of requests to multiple
 * ordered namespaces until a condition is met.
 * <li>invokeConcurrent Make concurrent requests to multiple namespaces and
 * return all of the results.
 * </ul>
 * Also maintains a cached pool of connections to NNs. Connections are managed
 * by the ConnectionManager and are unique to each user + NN. The size of the
 * connection pool can be configured. Larger pools allow for more simultaneous
 * requests to a single NN from a single user.
 */
public class RouterRpcClient {

  private static final Log LOG = LogFactory.getLog(RouterRpcClient.class);

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private ActiveNamenodeResolver namenodeResolver;
  /** Connection pool to the Namenodes per user for performance. */
  private ConnectionManager connectionManager;
  /** Minimum size of the connection pool. */
  private int minConnectionPoolSize;
  /** Maximum size of the connection pool. */
  private int maxConnectionPoolSize;
  /** Retry policy for router -> NN communication. */
  private RetryPolicy retryPolicy;
  /** Optional perf monitor. */
  private RouterRpcMonitor rpcMonitor;

  /**
   * Create a router RPC client to manage remote procedure calls to NNs.
   *
   * @param conf Hdfs Configuation.
   * @param resolver A NN resolver to determine the currently active NN in HA.
   * @param monitor Optional performance monitor.
   */
  public RouterRpcClient(Configuration conf, ActiveNamenodeResolver resolver,
      RouterRpcMonitor monitor) {

    this.rpcMonitor = monitor;
    this.namenodeResolver = resolver;

    this.minConnectionPoolSize = 1;
    this.maxConnectionPoolSize = conf.getInt(
        DFSConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_SIZE,
        DFSConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_SIZE_DEFAULT);
    this.connectionManager = new ConnectionManager(
        conf, minConnectionPoolSize, maxConnectionPoolSize);

    // TODO Verify this will work for non-HA too
    // TODO Customize parameters
    int maxFailoverAttempts = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
    int maxRetryAttempts = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT);
    int failoverSleepBaseMillis = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
    int failoverSleepMaxMillis = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);
    this.retryPolicy = RetryPolicies.failoverOnNetworkException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, maxFailoverAttempts, maxRetryAttempts,
        failoverSleepBaseMillis, failoverSleepMaxMillis);
  }

  /**
   * Shutdown the client.
   */
  public void shutdown() {
    if (this.connectionManager != null) {
      this.connectionManager.close();
    }
  }

  /**
   * Total number of available sockets between the router and NNs.
   *
   * @return Number of namenode clients.
   */
  public int getNumNamenodeClients() {
    return this.connectionManager.getNumOpenConnections();
  }

  /**
   * Total number of open connection pools to a NN. Each connection pool.
   * represents one user + one NN.
   *
   * @return Number of namenode pools.
   */
  public int getNumNamenodePools() {
    return this.connectionManager.getNumConnectionPools();
  }

  /**
   * Minimum size of a router -> NN connection pool for a user.
   *
   * @return Minimum size of the connection pool.
   */
  public int getNamenodePoolMinSize() {
    return this.minConnectionPoolSize;
  }

  /**
   * Maximum size of a router -> NN connection pool for a user.
   *
   * @return Maximum size of the connection pool.
   */
  public int getNamenodePoolMaxSize() {
    return this.maxConnectionPoolSize;
  }

  /**
   * Get ClientProtocol proxy client for a NameNode. Each combination of user +
   * NN must use a unique proxy client. Previously created clients are cached
   * and stored in a connection pool by the ConnectionManager.
   *
   * @param rpcAddress ClientProtocol RPC server address of the NN.
   * @return ConnectionContext containing a ClientProtocol proxy client for the
   *         NN + current user.
   */
  private ConnectionContext getConnection(String rpcAddress)
      throws IOException {
    ConnectionContext connection = null;
    try {
      // Each proxy holds the UGI info for the current user when it is created.
      // This cache does not scale very well, one entry per user per namenode,
      // and may need to be adjusted and/or selectively pruned. The cache is
      // important due to the excessive overhead of creating a new proxy wrapper
      // for each individual request.

      // Fetch user from server thread
      Call curCall = Server.getCurCall().get();
      UserGroupInformation ugi;
      if (curCall != null) {
        ugi = curCall.getUserGroupInformation();
      } else {
        ugi = UserGroupInformation.getCurrentUser();
      }
      connection = this.connectionManager.getConnection(ugi, rpcAddress);
      if (LOG.isDebugEnabled()) {
        LOG.info("User " + ugi.getUserName() + " NN " + rpcAddress
            + " is using connection " + connection.hashCode());
      }
    } catch (Exception ex) {
      LOG.error("Unable to open NN client to address: " + rpcAddress, ex);
    }

    if (connection == null) {
      throw new IOException("Unable to open a connection to NN " + rpcAddress);
    }
    return connection;
  }

  /**
   * Convert an exception to an IOException.
   *
   * For a non-IOException, wrap it with IOException. For a RemoteException,
   * unwrap it. For an IOException which is not a RemoteException, return it.
   *
   * @param e Exception to convert into an exception.
   * @return Created IO exception.
   */
  private static IOException toIOException(Exception e) {
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    }
    if (e instanceof IOException) {
      return (IOException)e;
    }
    return new IOException(e);
  }

  /**
   * If we should retry the RPC call.
   *
   * @param ex Exception reported.
   * @param retryCount Number of retries.
   * @return Retry decision.
   * @throws IOException Original exception if the retry policy generates one.
   */
  private RetryDecision shouldRetry(final IOException ioe, final int retryCount)
      throws IOException {
    try {
      final RetryPolicy.RetryAction a =
          this.retryPolicy.shouldRetry(ioe, retryCount, 0, true);
      return a.action;
    } catch (Exception ex) {
      LOG.info("Re-throwing API exception, no more retries.", ex);
      throw toIOException(ex);
    }
  }

  /**
   * Invokes a method against the ClientProtocol proxy server. If a standby
   * exception is generated by the call to the client, retries using the
   * alternate server.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param client The primary ClientProtocol proxy.
   * @param namenodes A prioritized list of namenodes within the same
   *                  nameservice.
   * @param method Remote ClientProtcol method to invoke.
   * @param params Variable list of parameters matching the method.
   * @return The result of the invoking the method.
   * @throws IOException
   */
  private Object invokeMethod(
      List<? extends FederationNamenodeContext> namenodes,
      Method method, Object... params) throws IOException {

    if (namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName());
    }

    Object ret = null;
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    boolean failover = false;
    for (FederationNamenodeContext namenode : namenodes) {
      ConnectionContext connection = null;
      try {
        String rpcAddress = namenode.getRpcAddress();
        connection = this.getConnection(rpcAddress);
        ProxyAndInfo<ClientProtocol> client = connection.getClient();
        ClientProtocol proxy = client.getProxy();
        ret = invoke(0, method, proxy, params);
        if (failover) {
          // Success on alternate server, update
          String nsId = namenode.getNameserviceId();
          InetSocketAddress address = client.getAddress();
          namenodeResolver.updateActiveNamenode(nsId, address);
        }
        if (this.rpcMonitor != null) {
          this.rpcMonitor.proxyOpComplete(true);
        }
        return ret;
      } catch (IOException ex) {
        if (ex instanceof StandbyException) {
          // Fail over indicated by retry policy and/or NN
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpFailureStandby();
          }
          failover = true;
        } else if (ex instanceof RemoteException) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpComplete(true);
          }
          // RemoteException returned by NN
          throw (RemoteException) ex;
        } else {
          // Other communication error, this is a failure
          // Communication retries are handled by the retry policy
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpFailureCommunicate();
            this.rpcMonitor.proxyOpComplete(false);
          }
          throw ex;
        }
      } finally {
        if (connection != null) {
          connection.releaseClient();
        }
      }
    }
    if (this.rpcMonitor != null) {
      this.rpcMonitor.proxyOpComplete(false);
    }
    // If we get here, it means that no namenode was available
    throw new IOException("No namenode available (" + namenodes +
        ") to invoke " + method.getName());
  }

  /**
   * Invokes a method on the designated object. Catches exceptions specific to
   * the invocation.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param method Method to invoke
   * @param obj Target object for the method
   * @param params Variable parameters
   * @return Response from the remote server
   * @throws IOException
   * @throws InterruptedException
   */
  private Object invoke(int retryCount, final Method method, final Object obj,
      final Object... params) throws IOException {
    try {
      return method.invoke(obj, params);
    } catch (IllegalAccessException e) {
      LOG.error("Unexpected exception while proxying API", e);
      return null;
    } catch (IllegalArgumentException e) {
      LOG.error("Unexpected exception while proxying API", e);
      return null;
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        IOException ioe = (IOException) cause;
        // Check if we should retry.
        RetryDecision decision = shouldRetry(ioe, retryCount);
        if (decision == RetryDecision.RETRY) {
          // retry
          return invoke(++retryCount, method, obj, params);
        } else if (decision == RetryDecision.FAILOVER_AND_RETRY) {
          // failover, invoker looks for standby exceptions for failover.
          if (ioe instanceof StandbyException) {
            throw ioe;
          } else {
            throw new StandbyException(ioe.getMessage());
          }
        } else {
          if (ioe instanceof RemoteException) {
            RemoteException re = (RemoteException) ioe;
            ioe = re.unwrapRemoteException();
          }
          throw ioe;
        }
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Invokes a ClientProtocol method. Determines the target nameservice via a
   * provided block.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param block Block used to determine appropriate nameservice.
   * @param method The remote method and parameters to invoke.
   * @return The result of the invoking the method.
   * @throws IOException
   */
  public Object invokeSingle(final ExtendedBlock block, RemoteMethod method)
      throws IOException {
    String bpId = block.getBlockPoolId();
    return invokeSingleBlockPool(bpId, method);
  }

  /**
   * Invokes a ClientProtocol method. Determines the target nameservice using
   * the block pool id.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param bpId Block pool identifier.
   * @param method The remote method and parameters to invoke.
   * @return The result of the invoking the method.
   * @throws IOException
   */
  public Object invokeSingleBlockPool(final String bpId, RemoteMethod method)
      throws IOException {
    String nsId = getNameserviceForBlockPoolId(bpId);
    return invokeSingle(nsId, method);
  }

  /**
   * Invokes a ClientProtocol method against the specified namespace.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param nsId Target namespace for the method.
   * @param method The remote method and parameters to invoke.
   * @return The result of the invoking the method.
   * @throws IOException
   */
  public Object invokeSingle(final String nsId, RemoteMethod method)
      throws IOException {
    List<? extends FederationNamenodeContext> namenodes =
        getNamenodesForNameservice(nsId);
    RemoteLocationContext loc = new RemoteLocation(nsId, "/");
    return invokeMethod(namenodes, method.getMethod(), method.getParams(loc));
  }

  /**
   * Invokes a single proxy call for a single location.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param location RemoteLocation to invoke.
   * @param remoteMethod The remote method and parameters to invoke.
   * @return The result of the invoking the method if successful.
   * @throws IOException
   */
  public Object invokeSingle(final RemoteLocationContext location,
      RemoteMethod remoteMethod) throws IOException {
    List<RemoteLocationContext> locations =
        new ArrayList<RemoteLocationContext>();
    locations.add(location);
    return invokeSequential(locations, remoteMethod);
  }

  /**
   * Invokes sequential proxy calls to different locations. Continues to invoke
   * calls until a call returns without throwing a remote exception.
   *
   * @param locations List of locations/nameservices to call concurrently.
   * @param remoteMethod The remote method and parameters to invoke.
   * @return The result of the first successful call, or if no calls are
   *         successful, the result of the last RPC call executed.
   * @throws IOException if the success condition is not met and one of the RPC
   *           calls generated a remote exception.
   */
  public Object invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod) throws IOException {
    return invokeSequential(locations, remoteMethod, null, null);
  }

  /**
   * Invokes sequential proxy calls to different locations. Continues to invoke
   * calls until the success condition is met, or until all locations have been
   * attempted.
   *
   * The success condition may be specified by:
   * <ul>
   * <li>An expected result class
   * <li>An expected result value
   * </ul>
   *
   * If no expected result class/values are specified, the success condition is
   * a call that does not throw a remote exception.
   *
   * @param locations List of locations/nameservices to call concurrently.
   * @param remoteMethod The remote method and parameters to invoke.
   * @param expectedResultClass In order to be considered a positive result, the
   *          return type must be of this class.
   * @param expectedResultValue In order to be considered a positive result, the
   *          return value must equal the value of this object.
   * @return The result of the first successful call, or if no calls are
   *         successful, the result of the last RPC call executed.
   * @throws IOException if the success condition is not met and one of the RPC
   *           calls generated a remote exception.
   */
  public Object invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod, Class<?> expectedResultClass,
      Object expectedResultValue) throws IOException {

    final Method m = remoteMethod.getMethod();
    IOException lastThrownException = null;
    Object result = null;
    // Invoke in priority order
    for (final RemoteLocationContext loc : locations) {
      String ns = loc.getNameserviceId();
      List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(ns);
      try {
        result = invokeMethod(namenodes, m, remoteMethod.getParams(loc));
        // Check result class
        if (isExpectedClass(expectedResultClass, result)
            && isExpectedValue(expectedResultValue, result)) {
          // Valid result, stop here
          return result;
        }
      } catch (IOException ex) {
        // Record it and move on
        lastThrownException = (IOException) ex;
      } catch (Exception ex) {
        // Unusual error, ClientProtocol calls always use IOException (or
        // RemoteException). Re-wrap in IOException for compatibility with
        // ClientProtcol.
        LOG.error("Error proxying " + remoteMethod.getMethodName(), ex);
        lastThrownException = new IOException(
            "Unknown error proxying API " + ex.getMessage());
      }
    }
    if (lastThrownException != null) {
      // re-throw the last exception thrown for compatibility
      throw lastThrownException;
    }
    // Return the last result, whether it is the value we are looking for or a
    return result;
  }

  /**
   * Checks if a result matches the required result class.
   *
   * @param expectedResultClass Required result class, null to skip the check.
   * @param result The result to check.
   * @return True if the result is an instance of the required class or if the
   *         expected class is null.
   */
  private boolean isExpectedClass(Class<?> expectedResultClass, Object result) {
    if (expectedResultClass == null) {
      return true;
    } else if (result == null) {
      return false;
    } else {
      return expectedResultClass.isInstance(result);
    }
  }

  /**
   * Checks if a result matches the expected value.
   *
   * @param expectedResultValue The expected value, null to skip the check.
   * @param result The result to check.
   * @return True if the result is equals to the expected value or if the
   *         expected value is null.
   */
  private boolean isExpectedValue(Object expectedResultValue, Object result) {
    if (expectedResultValue == null) {
      return true;
    } else if (result == null) {
      return false;
    } else {
      return result.equals(expectedResultValue);
    }
  }

  /**
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param locations List of remote locations to call concurrently.
   * @param remoteMethod The remote method and parameters to invoke.
   * @param requiredResponse If true an exception will be thrown if all calls to
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @return The result of the invoking the method.
   * @throws IOException If requiredResponse=true and any of the calls throw an
   *           exception.
   */
  public Object[] invokeConcurrent(
      final Collection<? extends RemoteLocationContext> locations,
      final RemoteMethod method, boolean requireResponse) throws IOException {
    final Method m = method.getMethod();

    if (locations.size() == 1) {
      // Shortcut, just one call
      RemoteLocationContext location = locations.iterator().next();
      String ns = location.getNameserviceId();
      final List<? extends FederationNamenodeContext> lookup =
          getNamenodesForNameservice(ns);
      Object[] paramList = method.getParams(location);
      Object result = invokeMethod(lookup, m, paramList);
      return new Object[] {result};
    }

    ExecutorService executorService =
        Executors.newFixedThreadPool(locations.size());

    Set<Callable<Object>> callables = new HashSet<Callable<Object>>();
    for (final RemoteLocationContext location : locations) {
      String ns = location.getNameserviceId();
      final List<? extends FederationNamenodeContext> lookup =
          getNamenodesForNameservice(ns);
      final Object[] paramList = method.getParams(location);
      // Call the objectGetter in order of nameservics in the NS list
      callables.add(new Callable<Object>() {
        public Object call() throws Exception {
          return invokeMethod(lookup, m, paramList);
        }
      });
    }

    List<Future<Object>> futures;
    try {
      if (rpcMonitor != null) {
        rpcMonitor.proxyOp();
      }
      futures = executorService.invokeAll(callables);
      Object[] results = new Object[futures.size()];
      for (int i = 0; i < futures.size(); i++) {
        try {
          results[i] = futures.get(i).get();
        } catch (ExecutionException ex) {
          if (requireResponse) {
            if (IOException.class.isInstance(ex.getCause())) {
              // Response from all servers required, use this error.
              throw (IOException) ex.getCause();
            } else {
              throw new IOException("Unhandled exception while proxying API "
                + m.getName() + " - " + ex.getCause().getMessage());
            }
          }
        }
      }
      return results;
    } catch (Exception ex) {
      LOG.error("Unexpected error while invoking API", ex);
      throw new IOException(
          "Unexpected error while invoking API " + ex.getMessage());
    }
  }

  /**
   * Get a prioritized list of NNs that share the same nameservice ID (in the
   * same namespace). NNs that are reported as ACTIVE will be first in the list.
   *
   * @param nameserviceId The nameservice ID for the namespace.
   * @return A prioritized list of NNs to use for communication.
   * @throws IOException If a NN cannot be located for the nameservice ID.
   */
  public List<? extends FederationNamenodeContext> getNamenodesForNameservice(
      String nameserviceId) throws IOException {

    List<? extends FederationNamenodeContext> registrations =
        namenodeResolver.getPrioritizedNamenodesForNameserviceId(
            nameserviceId);

    if (registrations == null) {
      throw new IOException(
          "Unable to locate a registered namenode for nameserviceId "
              + nameserviceId);
    }
    return registrations;
  }

  /**
   * Get a prioritized list of NNs that share the same block pool ID (in the
   * same namespace). NNs that are reported as ACTIVE will be first in the list.
   *
   * @param blockPoolId The blockpool ID for the namespace.
   * @return A prioritized list of NNs to use for communication.
   * @throws IOException If a NN cannot be located for the block pool ID.
   */
  public List<? extends FederationNamenodeContext> getNamenodesForBlockPoolId(
      String blockPoolId) throws IOException {

    List<? extends FederationNamenodeContext> registrations =
        namenodeResolver.getPrioritizedNamenodesForBlockPoolId(blockPoolId);

    if (registrations == null) {
      throw new IOException(
          "Unable to locate a registered namenode for blockpoolid "
              + blockPoolId);
    }
    return registrations;
  }

  /**
   * Get the nameservice identifier for a block pool.
   * @param blockPoolId Identifier of the block pool.
   * @return Nameservice identifier.
   * @throws IOException
   */
  private String getNameserviceForBlockPoolId(String blockPoolId)
      throws IOException {
    List<? extends FederationNamenodeContext> namenodes =
        getNamenodesForBlockPoolId(blockPoolId);
    for (FederationNamenodeContext namenode : namenodes) {
      return namenode.getNameserviceId();
    }
    return null;
  }
}
