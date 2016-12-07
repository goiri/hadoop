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
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * A map of the properties and target destinations (nameservice + path) for
 * a path in the global/federated namespace.
 * This data is generated from the @see MountTable records.
 */
public class PathLocation {

  /** Source path in global namespace. */
  private String sourcePath;

  /** Remote paths in the target namespaces. */
  private LinkedList<RemoteLocation> destinations;

  /** List of nameservices present. */
  private Set<String> subclusters;

  /**
   * Prioritize a location/destination by its namespace/nameserviceId.
   *
   * @param namespace The namespace/nameserviceID to prioritize.
   * @throws IOException
   */
  public void prioritizeNamespace(String namespace) throws IOException {
    prioritizeNamespace(this.destinations, namespace);
  }

  public static void prioritizeNamespace(LinkedList<RemoteLocation> locations,
      String namespace) throws IOException {
    if (locations.size() <= 1) {
      return;
    }
    Iterator<RemoteLocation> iterator = locations.iterator();
    RemoteLocation foundLocation = null;
    while (iterator.hasNext()) {
      // Remove at most one instance of a location for this namespace
      RemoteLocation currentLocation = iterator.next();
      if (currentLocation.getNameserviceId().equals(namespace)) {
        iterator.remove();
        foundLocation = currentLocation;
        break;
      }
    }
    if (foundLocation == null) {
      throw new IOException(
          "Unable to locate an eligible location with namespace - " + namespace
              + " in " + locations);
    }
    locations.addFirst(foundLocation);
  }

  /**
   * Create a new MountTableLocation.
   *
   * @param source Source path in the global namespace.
   * @param dest Destinations of the mount table entry.
   * @param identifier Unique identifier representing the combination of
   *          subclusters present in the destination list.
   */
  public PathLocation(String source, LinkedList<RemoteLocation> dest,
      Set<String> namespaces) {
    this.sourcePath = source;
    this.destinations = dest;
    this.subclusters = namespaces;
  }

  /**
   * Get the source path in the global namespace for this path location.
   *
   * @return The path in the global namespace.
   */
  public String getSourcePath() {
    return this.sourcePath;
  }

  /**
   * Get the list of subclusters defined for the destinations.
   */
  public Set<String> getSubclusters() {
    return this.subclusters;
  }

  @Override
  public String toString() {
    RemoteLocation loc = getDefaultLocation();
    return loc.getNameserviceId() + "->" + loc.getDest();
  }

  /**
   * Check if this location supports multiple clusters/paths.
   *
   * @return If it has multiple destinations.
   */
  public boolean hasMultipleDestinations() {
    return this.destinations.size() > 1;
  }

  /**
   * Get the list of locations found in the mount table.
   * The first result is the highest priority path.
   *
   * @return List of remote locations.
   */
  public LinkedList<RemoteLocation> getDestinations() {
    return this.destinations;
  }

  /**
   * Get the default or highest priority location.
   *
   * @return The default location.
   */
  public RemoteLocation getDefaultLocation() {
    if (destinations.isEmpty() || destinations.getFirst().getDest() == null) {
      throw new UnsupportedOperationException(
          "Unsupported path " + sourcePath + " please check mount table");
    }
    return destinations.getFirst();
  }
}