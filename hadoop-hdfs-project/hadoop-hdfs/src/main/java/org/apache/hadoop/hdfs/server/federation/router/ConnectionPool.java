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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.conf.Configuration;

/**
 * Maintains a pool of connections for each NN + User. The RPC client maintains
 * a single socket per user + address. To achieve throughput similar to a NN,
 * each user request is multiplexed across multiple sockets/connections from a
 * pool.
 */
public class ConnectionPool {

  private static final Log LOG = LogFactory.getLog(ConnectionPool.class);

  /** Configuration settings for the connection pool. */
  private Configuration conf;

  /** Pool of connections. */
  private List<ConnectionContext> connections =
      new ArrayList<ConnectionContext>();
  /** Connection index for round-robin. */
  private AtomicInteger clientIndex = new AtomicInteger(0);
  /** Lock to synchronize connection getting from the pool. */
  private Object connectionPoolLock = new Object();

  /** Namenode this pool connects to. */
  private String namenodeAddress;
  /** User for this connections. */
  private UserGroupInformation ugi;

  /** Min number of connections per user. */
  private int minSize;
  /** Max number of connections per user. */
  private int maxSize;

  /** How long we wait to close a connection. */
  private long connectionCleanupPeriodMs;

  /** The last time a connection was busy. */
  private long lastBusyTime = 0;


  protected ConnectionPool(Configuration config, String namenode,
      UserGroupInformation user, int minPoolSize, int maxPoolSize,
      long cleanupPeriod) throws IOException {

    this.conf = config;

    // Connection pool target
    this.ugi = user;
    this.namenodeAddress = namenode;

    // Set configuration parameters for the pool
    this.minSize = minPoolSize;
    this.maxSize = maxPoolSize;
    this.connectionCleanupPeriodMs = cleanupPeriod;

    // Add minimum connections to the pool
    for (int i = 0; i < this.minSize; i++) {
      ConnectionContext newConnection =
          createConnection(namenodeAddress, ugi, i);
      connections.add(newConnection);
    }
    LOG.info("Created connection pool " + toString() +
        " with " + this.minSize + " connections");
  }

  @Override
  public String toString() {
    return this.getConnectionId();
  }

  /**
   * Get the identifier for the connection.
   *
   * @return Identifier for the connection as user->NN.
   */
  protected String getConnectionId() {
    return this.ugi.getUserName() + "->" + this.namenodeAddress;
  }

  /**
   * Close the connection pool.
   */
  protected void close() {
    long timeSinceLastBusy = (Time.monotonicNow() - getLastBusyTime()) / 1000;
    LOG.info("Shutting down connection pool " + toString() + " used " +
        timeSinceLastBusy + " seconds ago");
  }

  /**
   * Number of connections in the pool.
   *
   * @return Number of connections.
   */
  protected int getNumConnections() {
    return this.connections.size();
  }

  /**
   * Get the last time the connection pool was busy.
   *
   * @return Last time the connection pool was busy.
   */
  protected long getLastBusyTime() {
    return this.lastBusyTime;
  }

  /**
   * Return the next connection round-robin.
   *
   * @return Connection context.
   */
  protected ConnectionContext getConnection() {
    // It is possible the list has shrunk in size
    // We catch the exception faster than locking as this is a very common op
    ConnectionContext conn = null;
    try {
      int index = this.clientIndex.incrementAndGet();
      index = index % this.connections.size();
      conn = this.connections.get(index);
    } catch (IndexOutOfBoundsException ex) {
      // TODO Try again/better algorithm
      conn = this.connections.get(0);
    }
    if (conn.isBusy() && this.connections.size() < this.maxSize) {
      // Add a new connection
      addNewConnection();
    }
    this.lastBusyTime = Time.monotonicNow();
    return conn;
  }

  /**
   * Add a new connection to the pool asynchronously.
   */
  private void addNewConnection() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        synchronized (connectionPoolLock) {
          if (connections.size() < maxSize) {
            try {
              ConnectionContext connection = createConnection(
                  namenodeAddress, ugi, connections.size());
              connections.add(connection);
              LOG.info("Added a new connection to " + getConnectionId() +
                  " with " + connections.size() + "/" + maxSize +
                  " connections");
            } catch (IOException e) {
              LOG.error(
                  "Cannot create a new connection for " + getConnectionId());
            }
          }
        }
      }
    }).start();
  }

  /**
   * Clean the connections for this pool.
   */
  protected void cleanup() {
    synchronized (connectionPoolLock) {
      if (this.connections.size() > this.minSize) {
        long timeSinceLastBusy = Time.monotonicNow() - this.lastBusyTime;
        if (timeSinceLastBusy > this.connectionCleanupPeriodMs) {
          // Drop by one client each idle interval
          // Removal is not synchronized, this may result in an out of bounds
          // exception which is handled in getConnection
          this.connections.remove(0);
          LOG.info("Removed connection " + getConnectionId() + " used " +
              (timeSinceLastBusy / 1000) + " seconds ago. Pool has " +
              this.connections.size() + "/" + this.maxSize + " connections");
        }
      }
    }
  }

  /**
   * Creates a proxy wrapper for a client NN connection. Each proxy contains
   * context for a single user/security context. To maximize throughput it is
   * recommended to use multiple connection per user+server, allowing multiple
   * writes and reads to be dispatched in parallel.
   *
   * @param conf Configuration for the connection.
   * @param nnAddress Address of server supporting the ClientProtocol.
   * @param ugi User context.
   * @param index Unique index for this connection.
   * @return Proxy for the target ClientProtocol that contains the user's
   *         security context.
   * @throws IOException
   */
  private ConnectionContext createConnection(String nnAddress,
      UserGroupInformation user, int index) throws IOException {

    RPC.setProtocolEngine(
        this.conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);

    final RetryPolicy defaultPolicy = RetryUtils.getDefaultRetryPolicy(
        this.conf,
        DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY,
        DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
        DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_KEY,
        DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
        SafeModeException.class);

    SocketFactory factory = getSocketFactory();
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(this.conf);
    }
    InetSocketAddress socket = NetUtils.createSocketAddr(nnAddress);
    ClientNamenodeProtocolPB proxy = null;
    final long version =
        RPC.getProtocolVersion(ClientNamenodeProtocolPB.class);
    int timeout = org.apache.hadoop.ipc.Client.getTimeout(this.conf);
    FederationConnectionId connectionId = new FederationConnectionId(
        socket, ClientNamenodeProtocolPB.class, user, timeout, defaultPolicy,
        this.conf, index);
    proxy = RPC.getProtocolProxy(ClientNamenodeProtocolPB.class, version,
        connectionId, this.conf, factory).getProxy();
    ClientProtocol client = new ClientNamenodeProtocolTranslatorPB(proxy);
    Text dtService = SecurityUtil.buildTokenService(socket);
    ProxyAndInfo<ClientProtocol> clientProxy =
        new ProxyAndInfo<ClientProtocol>(client, dtService, socket);
    ConnectionContext connection = new ConnectionContext(clientProxy);
    return connection;
  }

  /**
   * Get the socket factory.
   *
   * @return Default socket factory.
   */
  private static SocketFactory getSocketFactory() {
    return SocketFactory.getDefault();
  }
}
