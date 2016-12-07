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

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.net.NodeBase;

/**
 * Tree node for the mount table tree.
 */
public class PathTreeNode<T> {
  /** Name of the node. */
  private String name;
  /** Value stored in the node. */
  private T value;

  private PathTreeNode<T> parent;

  private Map<String, PathTreeNode<T>> children;

  public PathTreeNode(String name) {
    this.name = name;
    this.children = new TreeMap<String, PathTreeNode<T>>();
  }

  public Map<String, PathTreeNode<T>> getChildren() {
    return this.children;
  }

  public String getName() {
    return name;
  }

  public PathTreeNode<T> getParent() {
    return parent;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public void setParent(PathTreeNode<T> newParent) {
    this.parent = newParent;
  }

  public void setChildren(Map<String, PathTreeNode<T>> newChildren) {
    this.children = newChildren;
  }

  public boolean add(String[] path, T pValue) {
    if (path.length == 0) {
      // Leaf node, represents an entry in the mount table
      // Overwrite the primary key if it exists
      this.value = pValue;
    } else {
      String curPath = path[0];
      PathTreeNode<T> child = this.children.get(curPath);
      if (child == null) {
        child = new PathTreeNode<T>(curPath);
        child.parent = this;
        this.children.put(curPath, child);
      }
      String[] childPath = Arrays.copyOfRange(path, 1, path.length);
      child.add(childPath, pValue);
    }
    return false;
  }

  /**
   * Depth-first search of the tree seeking the deepest eligible node.
   * 
   * @param path Path components at this level
   * @param traverse Interface to determine if this node is eligible
   * @param nodeLevel Numerical level of depth of the current test
   * @return PathTreeResult containing the deepest eligible node.
   */
  public PathTreeResult<T> traverseDown(String[] path,
      PathTreeTraverse<T> traverse, int nodeLevel) {

    if (path.length > 0) {
      // Search for the next path component
      String curPath = path[0];
      PathTreeNode<T> child = this.children.get(curPath);
      if (child != null) {
        // Found matching child
        String[] childPath = Arrays.copyOfRange(path, 1, path.length);
        // Evaluate the suitability of the child
        PathTreeResult<T> result =
            child.traverseDown(childPath, traverse, nodeLevel + 1);
        if (result != null) {
          // Match found lower in the tree, use it
          return result;
        }
      }
    }
    if (traverse.isEligible(this, nodeLevel)) {
      // This node is eligible
      return new PathTreeResult<T>(this, path);
    }

    // end of the line
    return null;
  }

  /**
   * Depth first search of all nodes below this node.
   *
   * @param traverse Callback for each node found.
   */
  public void traverseAll(PathTreeTraverse<T> traverse) {
    // Search everything below this node
    for (PathTreeNode<T> entry : this.children.values()) {
      entry.traverseAll(traverse);
    }
    traverse.traverse(this);
  }

  /**
   * Traverse updwards.
   *
   * @param traverse Callback for each node found.
   */
  public void traverseUp(PathTreeTraverse<T> traverse) {
    traverse.traverse(this);
    // Search everything above this node
    if(this.parent != null) {
      this.parent.traverseUp(traverse);
    }
  }

  /**
   * Get the path to this node.
   * @return Full path of the node.
   */
  public String getPath() {
    String aux = "";
    if (this.parent != null) {
      aux += this.parent.getPath();
    }
    if (aux.endsWith(NodeBase.PATH_SEPARATOR_STR)) {
      aux += this.name;
    } else {
      aux = aux + NodeBase.PATH_SEPARATOR + this.name;
    }
    return aux;
  }

  @Override
  public String toString() {
    return this.toString(0);
  }

  public String toString(int level) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<level; i++) {
      sb.append(" ");
    }
    sb.append(this.name);
    sb.append(" -> ");
    sb.append(this.value);
    // Output the children
    for (PathTreeNode<T> child : this.children.values()) {
      sb.append("\n");
      sb.append(child.toString(level+2));
    }
    return sb.toString();
  }

  /**
   * Get the value for this node.
   * @return Value for this node.
   */
  public T getValue() {
    return this.value;
  }

  /**
   * Update the value for this node.
   * 
   * @param updatedValue
   */
  public void setValue(T updatedValue) {
    this.value = updatedValue;
  }

  /**
   * Return the number of nodes under this node.
   * @return Number of nodes under this node.
   */
  public int size() {
    int ret = 1;
    for (PathTreeNode<T> child : children.values()) {
      ret += child.size();
    }
    return ret;
  }

  /**
   * Return the number of leaves under this node.
   * @return Number of nodes under this node.
   */
  public int getNumOfLeaves() {
    int ret = 0;
    if (this.value != null) {
      ret++;
    }
    for (PathTreeNode<T> child : children.values()) {
      ret += child.getNumOfLeaves();
    }
    return ret;
  }
}