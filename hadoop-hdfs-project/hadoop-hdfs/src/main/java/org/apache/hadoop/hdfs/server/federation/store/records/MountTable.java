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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;

/**
 * Data schema for
 * {@link org.apache.hadoop.hdfs.server.federation.store.
 * FederationMountTableStore FederationMountTableStore} data stored in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.
 * FederationStateStoreService FederationStateStoreService}. Supports string
 * serialization.
 */
public abstract class MountTable extends BaseRecord {

  /** Non-serialized helper variables. */
  private Object destinationsLock = new Object();
  /** Pre-built list of remote locations from the location string. */
  private LinkedList<RemoteLocation> destinations;
  /** Cached set of namespace identifiers found for this entry. */
  private Set<String> namespaces;
  /** Separator used to serialize remote locations into a string. */
  private static final String LIST_SEPARATOR = ":::";

  /**
   * Get source path in the federated namespace.
   *
   * @return Source path in the federated namespace.
   */
  public abstract String getSourcePath();

  /**
   * Set source path in the federated namespace.
   *
   * @param path Source path in the federated namespace.
   */
  public abstract void setSourcePath(String path);

  /**
   * Get the destination paths.
   *
   * @return Destination paths.
   */
  public abstract String getDestPaths();

  /**
   * Set the destination paths.
   *
   * @param paths Destination paths.
   */
  public abstract void setDestPaths(String paths);

  /**
   * Build the destination string.
   * @param nameservice Destination nameservice.
   * @param path Destination path in the nameservice.
   * @return Destination string.
   */
  private static String buildDestString(String nameservice, String path) {
    return nameservice + LIST_SEPARATOR + path;
  }

  /**
   * Serializes the _destinations list into a string for data storage.
   *
   * @return Serialized string
   */
  private static String buildDestString(List<RemoteLocation> destinations) {
    StringBuilder builder = new StringBuilder();
    for (RemoteLocation location : destinations) {
      if (builder.length() > 0) {
        builder.append(",");
      }
      String nsId = location.getNameserviceId();
      String dest = location.getDest();
      String destString = buildDestString(nsId, dest);
      builder.append(destString);
    }
    return builder.toString();
  }

  private static String normalizeFileSystemPath(String mount) {
    Path path = new Path(mount);
    return path.toString();
  }

  /**
   * Get the destinations from a serialized location string.
   *
   * @param locationString Locations to get the remote locations from.
   * @return List with the destinations.
   */
  private static LinkedList<RemoteLocation> getLocations(
      String locationString) {
    String[] components = locationString.split(",");
    if (components.length == 0) {
      throw new IllegalArgumentException(
          "Invalid destination paths in mount table: "
              + Arrays.toString(components));
    }

    LinkedList<RemoteLocation> paths = new LinkedList<RemoteLocation>();
    for (int i = 0; i < components.length; i++) {
      String[] subComponents = components[i].split(LIST_SEPARATOR);

      if (subComponents.length != 2) {
        throw new IllegalArgumentException(
            "Invalid destination paths in mount table: "
                + Arrays.toString(subComponents));
      }
      RemoteLocation remoteLocation =
          new RemoteLocation(subComponents[0], subComponents[1]);
      paths.add(remoteLocation);
    }
    return paths;
  }

  /**
   * Get a list of namespaces present in the destinations for this entry.
   *
   * @return Set of nameserviceIds.
   */
  public Set<String> getNamespaces() {
    if (namespaces == null) {
      LinkedList<RemoteLocation> locations = this.getDestinations();
      this.namespaces = new HashSet<String>();
      for (RemoteLocation location : locations) {
        this.namespaces.add(location.getNameserviceId());
      }
    }
    return this.namespaces;
  }

  /**
   * Get a list of destinations (namespace + path) present for this entry.
   *
   * @return List of RemoteLocation destinations.
   */
  public LinkedList<RemoteLocation> getDestinations() {
    if (destinations == null) {
      synchronized (destinationsLock) {
        this.destinations = MountTable.getLocations(this.getDestPaths());
      }
    }
    return destinations;
  }


  /**
   * Constructor for a mount table entry with multiple destinations.
   *
   * @param src Source path in the mount entry.
   * @param destinations Nameservice destinations of the mount point.
   * @throws IOException
   */
  public static MountTable newInstance(String src,
      Map<String, String> destinations) throws IOException {
    MountTable record = FederationProtocolFactory.newInstance(MountTable.class);

    // Normalize the mount path
    record.setSourcePath(MountTable.normalizeFileSystemPath(src));

    // Build a list of remote locations
    LinkedList<RemoteLocation> locations = new LinkedList<RemoteLocation>();
    for (Entry<String, String> entry : destinations.entrySet()) {
      String path = entry.getValue();
      String nameserviceId = entry.getKey();
      MountTable.normalizeFileSystemPath(path);
      RemoteLocation location = new RemoteLocation(nameserviceId, path);
      locations.add(location);
    }

    // Set the cached destinations
    record.destinations = locations;

    // Set the serialized dest string
    record.setDestPaths(MountTable.buildDestString(locations));

    // Validate
    record.validate();
    return record;
  }

  /**
   * Constructor for a mount table entry with a single destinations.
   *
   * @param src Source path in the mount entry.
   * @param dest Nameservice destination of the mount point.
   * @param dateCreated Created date.
   * @param dateModified Modified date.
   * @throws IOException
   */
  public static MountTable newInstance(String src, String dest,
      long dateCreated, long dateModified) throws IOException {
    MountTable record = FederationProtocolFactory.newInstance(MountTable.class);

    record.setDateCreated(dateCreated);
    record.setDateModified(dateModified);
    // Normalize the path
    record.setSourcePath(MountTable.normalizeFileSystemPath(src));
    record.setDestPaths(dest);
    record.destinations = MountTable.getLocations(dest);

    record.validate();
    return record;
  }

  /**
   * Add a new destination to this mount table entry.
   */
  public void addDestination(String nameserviceId, String path)
      throws IOException {
    synchronized (destinationsLock) {
      LinkedList<RemoteLocation> existingDestinations = this.getDestinations();
      RemoteLocation newLocation = new RemoteLocation(nameserviceId, path);
      if (existingDestinations.contains(newLocation)) {
        throw new IOException("An existing destination for " + nameserviceId
            + "->" + path + " already exists.");
      }
      existingDestinations.add(newLocation);
      destinations = existingDestinations;
      this.setDestPaths(MountTable.buildDestString(existingDestinations));
    }
  }

  /**
   * Default constructor for a mount table entry.
   */
  public MountTable() {
    super();
  }

  /**
   * Get the default location.
   * @return The default location.
   */
  public RemoteLocation getDefaultLocation() {
    return this.getDestinations().getLast();
  }

  @Override
  public String toString() {
    return this.getSourcePath() + "->" + this.getDestPaths();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("sourcePath", this.getSourcePath());
    return map;
  }

  /**
   * Build the primary key map for a particular source path.
   *
   * @param srcPath
   * @return Primary key map for the record.
   */
  public static Map<String, String> generatePrimaryKeys(String srcPath) {
    Map<String, String> keyMap = new HashMap<String, String>();
    keyMap.put("sourcePath", srcPath);
    return keyMap;
  }

  @Override
  public void validate() {
    super.validate();
    if (this.getSourcePath() == null || this.getSourcePath().length() == 0) {
      throw new IllegalArgumentException(
          "Invalid mount table entry, no source path specified "
              + this.toString());
    }
    if (!this.getSourcePath().startsWith("/")) {
      throw new IllegalArgumentException(
          "Invalid mount table entry, all mount points"
              + " must start with a leading / "
              + this.toString());
    }
    if (this.getDestinations() == null || this.getDestinations().size() == 0) {
      throw new IllegalArgumentException(
          "Invalid mount table entry, no destination paths specified "
              + this.toString());
    }
    for (RemoteLocation loc : destinations) {
      String nsId = loc.getNameserviceId();
      if (nsId == null || nsId.length() == 0) {
        throw new IllegalArgumentException(
            "Invalid mount table entry, invalid destination nameservice "
                + this.toString());
      }
      if (loc.getDest() == null || loc.getDest().length() == 0) {
        throw new IllegalArgumentException(
            "Invalid mount table entry, invalid destination path "
                + this.toString());
      }
      if (!loc.getDest().startsWith("/")) {
        throw new IllegalArgumentException(
            "Invalid mount table entry, all destination points"
                + " must start with a leading / " + this);
      }
    }
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }

  /**
   * Sort by length of the mount string.
   *
   * @param other Another mount table object to compare to.
   * @return If this object goes before the parameter.
   */
  public int compareMountLength(MountTable other) {
    return this.getSourcePath().length() - other.getSourcePath().length();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(this.getSourcePath())
        .append(this.getDestPaths())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj.hashCode() == this.hashCode();
  }

  /**
   * Get a comparator based on the name.
   *
   * @return Comparator based on the name.
   */
  public static Comparator<MountTable> getSourceComparator() {
    return new Comparator<MountTable>() {
      public int compare(MountTable m1, MountTable m2) {
        return m1.getSourcePath().compareTo(m2.getSourcePath());
      }
    };
  }
}
