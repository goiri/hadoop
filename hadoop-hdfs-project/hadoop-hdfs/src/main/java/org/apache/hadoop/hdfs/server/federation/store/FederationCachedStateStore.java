package org.apache.hadoop.hdfs.server.federation.store;

import java.io.IOException;

/**
 * Interface for clients that subscribe to periodic cache updates from the state
 * store service.
 */
public interface FederationCachedStateStore {

  /**
   * Refresh all internal state and caches from the state store.
   *
   * @return True if successful.
   * @throws IOException If the data store could not be accessed.
   */
  boolean loadData() throws IOException;
}
