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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

/**
 * Implements a pool of connections for the {@link Router} to be able to open
 * many connections to many Namenodes.
 */
public class ConnectionManager {

  /** Configuration for the connection manager, pool and sockets. */
  private Configuration conf;

  /** Min number of connections per user + nn. */
  private int minSize;
  /** Max number of connections per user + nn. */
  private int maxSize;

  /** How often we close a pool for a particular user + nn. */
  private long poolCleanupPeriodMs;
  /** How often we close a connection in a pool. */
  private long connectionCleanupPeriodMs;

  /** Map of connection pools, one pool per user + NN. */
  private ConcurrentHashMap<String, ConnectionPool> pools;
  /** Lock acquired when adding/removing connection pools. */
  private Object poolsLock = new Object();
  /** Periodic timer to remove stale connection pools. */
  private Timer connectionPoolTimer = new Timer();

  /** If the connection manager is running. */
  private boolean isRunning = false;


  /**
   * Creates a proxy client connection pool manager.
   *
   * @param config Configuration for the connections.
   * @param minPoolSize Min size of the connection pool.
   * @param maxPoolSize Max size of the connection pool.
   */
  public ConnectionManager(
      Configuration config, int minPoolSize, int maxPoolSize) {
    if (minPoolSize < 1) {
      throw new IllegalArgumentException(
          "Invalid minimum pool size " + minPoolSize);
    }

    this.conf = config;

    // Configure minimum and maximum connection pools
    this.minSize = minPoolSize;
    this.maxSize = maxPoolSize;

    // List with the connections
    this.pools = new ConcurrentHashMap<String, ConnectionPool>();

    // TODO set via configuration
    this.poolCleanupPeriodMs = 60 * 1000;
    this.connectionCleanupPeriodMs = 30 * 1000;

    // Schedule a task to remove stale connection pools and sockets
    long recyleTimeMs = Math.min(poolCleanupPeriodMs, connectionCleanupPeriodMs);
    this.connectionPoolTimer.scheduleAtFixedRate(
        new CleanupTask(), 0, recyleTimeMs);

    // Mark the manager as running
    this.isRunning = true;
  }


  /**
   * Stop the connection manager by closing all the pools.
   */
  public void close() {
    this.connectionPoolTimer.cancel();
    synchronized (this.poolsLock) {
      this.isRunning = false;
      for (ConnectionPool pool : this.pools.values()) {
        pool.close();
      }
      this.pools.clear();
    }
  }

  /**
   * Generates a unique key to identify a connection a user + destination.
   *
   * @param username Name of the user.
   * @param namenode Namenode address with port.
   * @return Connection unique key.
   */
  private String connectionKey(String username, String namenode) {
    return username + "-" + namenode;
  }

  /**
   * Fetches the next available proxy client in the pool. Each client connection
   * is reserved for a single user and cannot be reused until free.
   *
   * @param ugi User context information.
   * @param namenode Namenode for the connection.
   * @return Proxy client.
   * @throws IOException
   */
  public ConnectionContext getConnection(
      UserGroupInformation ugi, String namenode) throws IOException {

    // Check for a pool outside of the lock for performance.
    String username = ugi.getUserName();
    String connectionKey = connectionKey(username, namenode);
    ConnectionPool pool = this.pools.get(connectionKey);
    if (pool == null) {
      synchronized (this.poolsLock) {
        if (!this.isRunning) {
          // Manager is shutdown
          return null;
        }
        // Stop other requests for this user + NN until we have created our
        // pool, otherwise we may create duplicate pools with extra connections
        pool = this.pools.get(connectionKey);
        if (pool == null) {
          pool = new ConnectionPool(
              conf, namenode, ugi, minSize, maxSize, connectionCleanupPeriodMs);
          this.pools.put(connectionKey, pool);
        }
      }
    }
    return pool.getConnection();
  }

  /**
   * Mark the connection as not busy after the proxy request is completed.
   *
   * @param conn Connection to release.
   */
  public void releaseConnection(ConnectionContext conn) {
    conn.releaseClient();
  }

  /**
   * Get the number of connection pools.
   *
   * @return Number of connection pools.
   */
  public int getNumConnectionPools() {
    return this.pools.size();
  }

  /**
   * Get the minimum pool size.
   *
   * @return Minimum pool size.
   */
  public int getMinPoolSize() {
    return this.minSize;
  }

  /**
   * Get the maximum pool size.
   *
   * @return Maximum pool size.
   */
  public int getMaxPoolSize() {
    return this.maxSize;
  }

  /**
   * Get number of open connections.
   *
   * @return Number of open connections.
   */
  public int getNumOpenConnections() {
    int total = 0;
    for (ConnectionPool pool : this.pools.values()) {
      total += pool.getNumConnections();
    }
    return total;
  }

  /**
   * Removes stale connections not accessed recently from the pool.
   */
  private class CleanupTask extends TimerTask {
    @Override
    public void run() {
      synchronized (poolsLock) {
        long currentTime = Time.monotonicNow();
        Iterator<Entry<String, ConnectionPool>> iter =
            pools.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<String, ConnectionPool> entry = iter.next();
          ConnectionPool pool = entry.getValue();
          long lastBusyTime = pool.getLastBusyTime();
          boolean isStale = currentTime > (lastBusyTime + poolCleanupPeriodMs);
          if (lastBusyTime > 0 && isStale) {
            // Remove this pool
            pool.close();
            iter.remove();
          } else {
            // Keep this pool but clean connections inside
            pool.cleanup();
          }
        }
      }
    }
  }
}
