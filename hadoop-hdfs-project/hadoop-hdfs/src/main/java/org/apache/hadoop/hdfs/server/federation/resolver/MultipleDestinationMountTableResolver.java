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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreService;
import org.apache.hadoop.hdfs.server.federation.utils.ConsistentHashRing;

/**
 * Mount table resolver that supports multiple locations for each mount entry.
 * The returned location contains prioritized remote paths from highest priority
 * to the lowest priority. Multiple locations for a mount point are optional.
 * When multiple locations are specified, both will be checked for the presence
 * of a file and the nameservice for a new file/dir is chosen based on the
 * results of a consistent hashing algorithm.
 * <p>
 * Does the Mount table entry for this path have multiple destinations?
 * <ul>
 * <li>No -> Return the location
 * <li>Yes -> Return all locations, prioritizing the best guess from the
 * consistent hashing algorithm.
 * </ul>
 * <p>
 * The consistent hashing result is dependent on the number and combination of
 * nameservices that are registered for particular mount point. The order of
 * nameservices/locations in the mount table is not prioritized. Each consistent
 * hash calculation considers only the set of unique nameservices present for
 * the mount table location.
 */
public class MultipleDestinationMountTableResolver extends MountTableResolver {

  private static final Log LOG =
      LogFactory.getLog(MultipleDestinationMountTableResolver.class);

  /** Namespace set hash -> Locator. */
  private ConcurrentHashMap<Integer, ConsistentHashRing> hashResolverMap;

  public static final String PATH_SEPARATOR = "/";

  public MultipleDestinationMountTableResolver(Configuration conf,
      FederationStateStoreService store) {
    super(conf, store);
    this.hashResolverMap = new ConcurrentHashMap<Integer, ConsistentHashRing>();
  }

  @Override
  public List<String> getMountPoints(String path) throws IOException {
    return super.getMountPoints(path);
  }

  /**
   * Get the cached (if available) or generate a new hash resolver for this
   * particular set of unique namespace identifiers.
   *
   * @param namespaces A set of unique namespace identifiers.
   * @return A hash resolver configured to consistently resolve paths to
   *         namespaces using the provided set of namespace identifiers.
   */
  private ConsistentHashRing getHashResolver(
      Set<String> namespaces) {
    ConsistentHashRing resolver =
        hashResolverMap.get(namespaces.hashCode());
    if (resolver == null) {
      resolver = new ConsistentHashRing(namespaces);
      hashResolverMap.put(namespaces.hashCode(), resolver);
    }
    return resolver;
  }

  /**
   * Trims a path to at most the immediate child of a parent path. For example
   * <ul>
   * <li>path = /a/b/c, parent = /a will be trimmed to /a/b.
   * <li>path = /a/b, parent = /a/b will be trimmed to /a/b
   * </ul>
   *
   * @param path The path to trim
   * @param parent The parent used to find the immediate child.
   * @return
   * @throws IOException
   */
  private static String trimPathToChild(String path, String parent)
      throws IOException {
    // Path is invalid or equal to the parent
    if (path.length() <= parent.length()) {
      return parent;
    }
    String remainder = path.substring(parent.length());
    String[] components =
        remainder.replaceFirst("^/", "").split(Path.SEPARATOR);
    if (components.length > 0 && components[0].length() > 0) {
      if (parent.endsWith(Path.SEPARATOR)) {
        return parent + components[0];
      } else {
        return parent + Path.SEPARATOR + components[0];
      }
    } else {
      return parent;
    }
  }

  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    PathLocation mountTableResults = super.getDestinationForPath(path);
    if (mountTableResults.hasMultipleDestinations()) {
      // Multiple destinations, use the result from the consistent hashing
      // locator to prioritize the locations for this path.
      ConsistentHashRing locator =
          this.getHashResolver(mountTableResults.getSubclusters());
      // Hash only up to the immediate child of the mount point.
      // This prevents the need to create/maintain subtrees under each
      // multi-destination mount point. Each child of a multi-destination
      // mount is mapped to only 1 hash location.
      String trimmedPath =
          trimPathToChild(path, mountTableResults.getSourcePath());
      // Copying files *._COPYING_ will be renamed, remove that chunk
      String copyingSuffix = "._COPYING_";
      if (trimmedPath.endsWith(copyingSuffix)) {
        int newLength = trimmedPath.length() - copyingSuffix.length();
        if (newLength > 0) {
          trimmedPath = trimmedPath.substring(0, newLength);
        }
      }
      String namespace = locator.getLocation(trimmedPath);
      if (namespace != null) {
        mountTableResults.prioritizeNamespace(namespace);
      } else {
        LOG.error("Unable to determine priority namespace via consistent " +
            "hashing for path - " + path);
      }
    }
    return mountTableResults;
  }
}
