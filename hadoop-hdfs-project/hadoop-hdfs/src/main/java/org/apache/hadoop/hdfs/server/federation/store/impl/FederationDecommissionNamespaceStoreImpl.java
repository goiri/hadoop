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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.FederationDecommissionNamespaceStore;
import org.apache.hadoop.hdfs.server.federation.store.FederationCachedStateStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.DecommissionedNamespace;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;

/**
 * Implementation of {@link FederationDecommisionNamespaceStore}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationDecommissionNamespaceStoreImpl
    extends FederationStateStoreInterface implements
    FederationDecommissionNamespaceStore, FederationCachedStateStore {

  private static final Log LOG = LogFactory.getLog(
      FederationDecommissionNamespaceStoreImpl.class);

  /** Decomissioned name spaces. */
  private Set<String> decommissionedNamespaces;

  /** Lock for accessing the cached list of decom namespaces. */
  private final ReadWriteLock decommissionedNamespacesLock =
      new ReentrantReadWriteLock();


  @Override
  public void init() {
  }

  /////////////////////////////////////////////////////////
  // Namespace decommissioning
  /////////////////////////////////////////////////////////

  @Override
  public boolean disableNamespace(String nsId)
      throws IOException {

    DecommissionedNamespace record = DecommissionedNamespace.newInstance(nsId);
    return getDriver().updateOrCreate(record, false, false);
  }

  @Override
  public boolean enableNamespace(String nsId)
      throws IOException {

    DecommissionedNamespace record = DecommissionedNamespace.newInstance(nsId);
    return getDriver().delete(record);
  }

  @Override
  public Set<String> getDisabledNamespaces() throws IOException {

    try {
      this.decommissionedNamespacesLock.readLock().lock();
      if (this.decommissionedNamespaces == null) {
        throw new StateStoreUnavailableException(
            "Decommissioned namespaces have not been loaded.");
      }
      return this.decommissionedNamespaces;
    } finally {
      this.decommissionedNamespacesLock.readLock().unlock();
    }
  }

  /**
   * Refreshes the cached list of decommissioned nameservices from the data
   * store.
   */
  @Override
  public boolean loadData() {
    try {
      QueryResult<DecommissionedNamespace> result =
          getDriver().get(DecommissionedNamespace.class);

      // Build a set of decommissioned namespaces
      Set<String> decomNamespaces = new TreeSet<String>();
      List<DecommissionedNamespace> records = result.getRecords();
      for (DecommissionedNamespace record : records) {
        String nsId = record.getNameserviceId();
        decomNamespaces.add(nsId);
      }

      this.decommissionedNamespacesLock.writeLock().lock();
      try {
        this.decommissionedNamespaces = decomNamespaces;
      } finally {
        this.decommissionedNamespacesLock.writeLock().unlock();
      }
    } catch (IOException e) {
      LOG.error("Failed to fetch decommissioned records from data store.", e);
      return false;
    }
    return true;
  }

  @Override
  public Class<? extends BaseRecord> requiredRecord() {
    return DecommissionedNamespace.class;
  }
}
