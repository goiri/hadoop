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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Cleans up the mount tree by deleting nodes that are no longer required. Used
 * to parse upwards in a tree from a deleted node and detect garbage nodes that
 * need to be cleaned up.
 */
public class PathTreeRemoveNodeCleanup<T> implements PathTreeTraverse<T> {

  /** Tracks the nodes in the tree that need to be deleted. */
  private List<PathTreeNode<T>> nodesToDelete =
      new ArrayList<PathTreeNode<T>>();
  private boolean foundValidNode = false;

  public PathTreeRemoveNodeCleanup(PathTreeNode<T> node) {
    nodesToDelete.add(node);
  }

  @Override
  public boolean isEligible(PathTreeNode<T> node, int nodeLevel) {
    return true;
  }

  @Override
  public void traverse(PathTreeNode<T> node) {
    if (node.getValue() != null || node.getChildren().size() > 1) {
      // This is a mount table record or on the path to an record,
      // stop
      foundValidNode = true;
      return;
    }
    if (!foundValidNode) {
      // dangling node with no branches or mount table entry, delete
      nodesToDelete.add(node);
    }
  }

  public Collection<PathTreeNode<T>> getTraversedNodes() {
    return nodesToDelete;
  }
}