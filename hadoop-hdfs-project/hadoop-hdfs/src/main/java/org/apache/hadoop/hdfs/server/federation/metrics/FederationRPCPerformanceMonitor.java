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
package org.apache.hadoop.hdfs.server.federation.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcMonitor;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeNode;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * Customizable RPC performance monitor. Receives events from the RPC server
 * and aggregates them via JMX.
 */
public class FederationRPCPerformanceMonitor implements RouterRpcMonitor {

  private static final Log LOG =
      LogFactory.getLog(FederationRPCPerformanceMonitor.class);

  /** Time for an operation to be received in the Router. */
  private static final ThreadLocal<Long> START_TIME =
      new ThreadLocal<Long>();
  /** Time for an operation to be send to the Namenode. */
  private static final ThreadLocal<Long> PROXY_TIME =
      new ThreadLocal<Long>();

  private Configuration conf;
  private RouterRpcServer server;

  /** JMX interface to monitor the RPC metrics. */
  private FederationRPCMetrics metrics;
  private ObjectName registeredBean;

  /** Tree with operations per path. */
  private Object pathStatsLock = new Object();
  private PathTree<Long> pathStats;

  /** Periodic service that uploads stats to the State Store. */
  private NamespaceStatsUploaderService statsUploader;

  /** Thread pool for logging stats. */
  private ThreadPoolExecutor executor;

  @Override
  public void initialize(
      Configuration configuration, RouterRpcServer rpcServer) {

    this.conf = configuration;
    this.server = rpcServer;
    // Create metrics
    this.metrics = FederationRPCMetrics.create(conf, server);

    // Create thread pool
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    // Adding JMX interface
    try {
      StandardMBean bean =
          new StandardMBean(this.metrics, FederationRPCMBean.class);
      registeredBean = MBeans.register("Router", "FederationRPC", bean);
      LOG.info("Registered FederationRPCMBean: " + registeredBean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad FederationRPCMBean setup", e);
    }

    // Namespace access statistics
    this.pathStats = new PathTree<Long>();

    // Create service to upload statistics to the State Store
    this.statsUploader = new NamespaceStatsUploaderService(this);
    this.statsUploader.init(this.conf);
    this.statsUploader.start();
  }

  @Override
  public void close() {
    if (registeredBean != null) {
      MBeans.unregister(registeredBean);
      registeredBean = null;
    }
    if (this.statsUploader != null) {
      this.statsUploader.stop();
    }
    if (executor != null) {
      executor.shutdown();
    }
  }

  /**
   * Resets all RPC service performance counters to their defaults.
   */
  public void resetPerfCounters() {
    if (registeredBean != null) {
      MBeans.unregister(registeredBean);
      registeredBean = null;
    }
    if (metrics != null) {
      FederationRPCMetrics.reset();
      metrics = null;
    }
    initialize(conf, server);
  }

  @Override
  public void startOp() {
    START_TIME.set(this.getNow());
  }

  @Override
  public long proxyOp() {
    PROXY_TIME.set(this.getNow());
    long processingTime = getProcessingTime();
    if (processingTime >= 0) {
      metrics.addProcessingTime(processingTime);
    }
    return Thread.currentThread().getId();
  }

  @Override
  public void proxyOpComplete(boolean success) {
    if (success) {
      long proxyTime = getProxyTime();
      if (proxyTime >= 0) {
        metrics.addProxyTime(proxyTime);
      }
    }
  }

  @Override
  public void proxyOpFailureStandby() {
    metrics.incrProxyOpFailureStandby();
  }

  @Override
  public void proxyOpFailureCommunicate() {
    metrics.incrProxyOpFailureCommunicate();
  }

  @Override
  public void routerFailureStateStore() {
    metrics.incrRouterFailureStateStore();
  }

  @Override
  public void routerFailureSafemode() {
    metrics.incrRouterFailureSafemode();
  }

  @Override
  public void routerFailureLocked() {
    metrics.incrRouterFailureLocked();
  }

  @Override
  public void logPathStat(final String path) {
    // TODO Queue for a periodic task to process
    Callable<Void> pathTask = new Callable<Void>() {
      public Void call() {
        synchronized (pathStatsLock) {
          // Atomic get/set
          PathTreeNode<Long> node = pathStats.findNode(path);
          if (node != null && node.getValue() != null) {
            node.setValue(node.getValue() + 1);
          } else {
            pathStats.add(path, (long) 1);
          }
        }
        return null;
      }
    };
    executor.submit(pathTask);
  }

  @Override
  public PathTree<Long> getPathStats() {
    return this.pathStats;
  }

  /**
   * Get current time.
   * @return Current time in nanoseconds.
   */
  private long getNow() {
    return System.nanoTime();
  }

  /**
   * Get time between we receiving the operation and sending it to the Namenode.
   * @return Processing time in nanoseconds.
   */
  private long getProcessingTime() {
    if (START_TIME.get() != null && START_TIME.get() > 0 &&
        PROXY_TIME.get() != null && PROXY_TIME.get() > 0) {
      return PROXY_TIME.get() - START_TIME.get();
    }
    return -1;
  }

  /**
   * Get time between now and when the operation was forwarded to the Namenode.
   * @return Current proxy time in nanoseconds.
   */
  private long getProxyTime() {
    if (PROXY_TIME.get() != null && PROXY_TIME.get() > 0) {
      return getNow() - PROXY_TIME.get();
    }
    return -1;
  }
}
