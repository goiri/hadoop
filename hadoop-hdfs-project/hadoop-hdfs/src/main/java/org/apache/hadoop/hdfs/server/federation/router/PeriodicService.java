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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Time;

/**
 * Service to periodically execute a runnable.
 */
public class PeriodicService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(PeriodicService.class);

  /** Interval for running the periodic service in seconds. */
  private int intervalSec;
  /** Name of the service. */
  private String serviceName;

  /** Runnable for the periodic service. */
  private Runnable periodicRunnable;
  /** Scheduler for the periodic service. */
  private ScheduledExecutorService scheduler;

  /** If the service is running. */
  private volatile boolean isRunning = false;

  /** How many times we run. */
  private long runCount;
  /** How many errors we got. */
  private long errorCount;
  /** When was the last time we executed this service successfully. */
  private long lastUpdate;

  /**
   * Create a new periodic update service.
   *
   * @param name Name of the service.
   */
  public PeriodicService(String name) {
    super(name);
    this.serviceName = name;
    this.intervalSec = 60;
  }

  /**
   * Set the interval for the periodic service.
   *
   * @param interval Interval in seconds.
   */
  protected void setIntervalSecs(int interval) {
    this.intervalSec = interval;
  }

  /**
   * Get the interval for the periodic service.
   *
   * @return Interval in seconds.
   */
  protected long getIntervalSecs() {
    return this.intervalSec;
  }

  /**
   * Set the runnable operation that this service will run periodically.
   *
   * @param runnable Runnable operation to run periodically.
   */
  protected void setRunnable(Runnable runnable) {
    this.periodicRunnable = runnable;
  }

  /**
   * Get how many times we failed to run the periodic service.
   *
   * @return Times we failed to run the periodic service.
   */
  protected long getErrorCount() {
    return this.errorCount;
  }

  /**
   * Get how many times we run the periodic service.
   *
   * @return Times we run the periodic service.
   */
  protected long getRunCount() {
    return this.runCount;
  }

  /**
   * Get the last time the periodic service was executed.
   *
   * @return Last time the periodic service was executed.
   */
  protected long getLastUpdate() {
    return this.lastUpdate;
  }

  /**
   * Restart the periodic service.
   */
  protected void restart() {
    if (this.getServiceState() == STATE.STARTED) {
      startUpdater();
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    LOG.info("Starting periodic service " + this.getClass().getSimpleName());
    startUpdater();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopUpdater();
    LOG.info("Stopping periodic service " + this.getClass().getSimpleName());
    super.serviceStop();
  }

  /**
   * Stop the periodic task.
   */
  protected void stopUpdater() {
    if (this.isRunning) {
      LOG.info(serviceName + " is shutting down.");
      this.isRunning = false;
      this.scheduler.shutdownNow();
      this.scheduler = null;
    }
  }

  /**
   * Start the periodic execution.
   */
  protected void startUpdater() {
    stopUpdater();

    // Create the runnable service
    Runnable updateRunnable = new Runnable() {
      @Override
      public void run() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running " + serviceName + " update task.");
        }
        try {
          if (!isRunning) {
            return;
          }
          periodicRunnable.run();
          runCount++;
          lastUpdate = Time.now();
        } catch (Exception ex) {
          errorCount++;
          LOG.warn(serviceName + " service threw an exception", ex);
        }
      }
    };

    // Start the execution of the periodic service
    this.isRunning = true;
    this.scheduler = Executors.newScheduledThreadPool(1);
    this.scheduler.scheduleWithFixedDelay(
        updateRunnable, 0, intervalSec, TimeUnit.SECONDS);
  }
}
