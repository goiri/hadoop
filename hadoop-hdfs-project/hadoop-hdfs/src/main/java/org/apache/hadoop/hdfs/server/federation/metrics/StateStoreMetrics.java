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

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Implementations of the JMX interface for the State Store metrics.
 */
@Metrics(name = "StateStoreActivity", about = "Router metrics",
    context = "router")
public class StateStoreMetrics implements StateStoreMBean {

  private final MetricsRegistry registry = new MetricsRegistry("router");

  @Metric("GET transactions")
  private MutableRate reads;
  @Metric("INSERT/UPDATE transactions")
  private MutableRate writes;
  @Metric("DELETE transactions")
  private MutableRate deletes;
  @Metric("Failed transactions")
  private MutableRate failures;

  public StateStoreMetrics(Configuration conf) {
    registry.tag(SessionId, "RouterSession");
    registry.tag(ProcessName, "Router");
  }

  public static StateStoreMetrics create(Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(new StateStoreMetrics(conf));
  }

  public void addRead(long latency) {
    reads.add(latency);
  }

  public void addWrite(long latency) {
    writes.add(latency);
  }

  public void addFailure(long latency) {
    failures.add(latency);
  }

  public void addDelete(long latency) {
    deletes.add(latency);
  }
}
