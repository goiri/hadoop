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
package org.apache.hadoop.hdfs.server.federation.utils.tree;

import java.util.List;

/**
 * Utilities for managing {@link PathTree}.
 */
public final class PathTreeUtils {

  private PathTreeUtils() {
    // Utility class
  }

  /**
   * Takes a string and generate a long PathTree.
   * @param json JSON input.
   * @return Path tree with longs.
   */
  public static PathTree<Long> toTreeLong(String json) {
    PathTree<String> treeString = new PathTree<String>(json);
    return toTreeLong(treeString);
  }

  /**
   * Takes a tree with strings and transforms it into longs.
   * @param in Input tree with Strings.
   * @return Output tree with longs.
   */
  public static PathTree<Long> toTreeLong(PathTree<String> in) {
    PathTree<Long> out = new PathTree<Long>();
    for (String path : in.getSubPaths("/")) {
      PathTreeNode<String> node = in.findNode(path);
      if (node != null && node.getValue() != null) {
        long value = Long.parseLong(node.getValue());
        out.add(path, value);
      }
    }
    return out;
  }

  /**
   * Add a tree into another tree.
   * @param base Base tree.
   * @param other Other tree.
   */
  public static void add(PathTree<Long> base, PathTree<Long> other) {
    List<String> paths = other.getSubPaths("/");
    for (String path : paths) {
      // Value in the base tree
      long baseValue = 0;
      boolean baseSet = false;
      PathTreeNode<Long> baseNode = base.findNode(path);
      if (baseNode != null && baseNode.getValue() != null) {
        baseValue = baseNode.getValue();
        baseSet = true;
      }
      // Value in the other tree
      long otherValue = 0;
      boolean otherSet = false;
      PathTreeNode<Long> otherNode = other.findNode(path);
      if (otherNode != null && otherNode.getValue() != null) {
        otherValue = otherNode.getValue();
        otherSet = true;
      }
      // Add to the base if one of the values were in the tree
      if (baseSet || otherSet) {
        base.add(path, baseValue + otherValue);
      }
    }
  }

  /**
   * Add the maximum values to another tree.
   * @param base Base tree.
   * @param other Other tree.
   */
  public static void max(PathTree<Long> base, PathTree<Long> other) {
    List<String> paths = other.getSubPaths("/");
    for (String path : paths) {
      // Value in the base tree
      long baseValue = Long.MIN_VALUE;
      boolean baseSet = false;
      PathTreeNode<Long> baseNode = base.findNode(path);
      if (baseNode != null && baseNode.getValue() != null) {
        baseValue = baseNode.getValue();
        baseSet = true;
      }
      // Value in the other tree
      long otherValue = Long.MIN_VALUE;
      boolean otherSet = false;
      PathTreeNode<Long> otherNode = other.findNode(path);
      if (otherNode != null && otherNode.getValue() != null) {
        otherValue = otherNode.getValue();
        otherSet = true;
      }
      // Set the maximum value to the tree
      if (baseSet || otherSet) {
        base.add(path, Math.max(baseValue, otherValue));
      }
    }
  }

  /**
   * Trim a tree by removing branches with smaller values than a threshold.
   *
   * @param tree Tree to trim.
   * @param minPct Minimum percentage per branch.
   */
  public static void trim(PathTree<Long> tree, float minPct) {
    // Get the total weight of the tree
    long total = 0;
    for (String path : tree.getSubPaths("/")) {
      PathTreeNode<Long> node = tree.findNode(path);
      if (node != null && node.getValue() != null) {
        total += node.getValue();
      }
    }

    for (String path : tree.getSubPaths("/")) {
      PathTreeNode<Long> node = tree.findNode(path);
      if (node != null && node.getValue() != null) {
        long value = node.getValue();
        if (value < minPct * total) {
          String parent = path.substring(
              0, path.lastIndexOf(PathTree.PATH_SEPARATOR));
          PathTreeNode<Long> nodeParent = tree.findNode(parent);
          long parentValue = 0;
          if (nodeParent != null && nodeParent.getValue() != null) {
            parentValue = nodeParent.getValue();
          }
          tree.remove(path);
          tree.add(parent, parentValue + value);
        }
      }
    }
  }
}
