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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.JSON;

/**
 * Tree for the mount table.
 */
public class PathTree<T> {

  private static final Log LOG = LogFactory.getLog(PathTree.class);

  /** Root of the tree. */
  private PathTreeNode<T> root;

  /** Separator in the path. */
  public static final String PATH_SEPARATOR = "/";

  /** Synchronization. */
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  /**
   * Create an empty tree.
   */
  public PathTree() {
    this.root = new PathTreeNode<T>("");
  }

  public PathTree(String json) {
    this();
    Object o = JSON.parse(json);
    if (o instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, T> map = (Map<String, T>) o;
      for (Map.Entry<String, T> entry : map.entrySet()) {
        this.add(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Remove the full tree.
   */
  public void clear() {
    writeLock.lock();
    try {
      this.root = new PathTreeNode<T>("");
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Split a path into an array.
   *
   * @param path Input path.
   * @return Split path.
   */
  private static String[] pathSplit(String path) {
    String[] ret = path.split(PATH_SEPARATOR);
    if (ret.length > 0 && ret[0].isEmpty()) {
      ret = Arrays.copyOfRange(ret, 1, ret.length);
    }
    return ret;
  }

  /**
   * Remove a path from the tree.
   * 
   * @param path The path to remove from the tree.
   * @return True if the path was located and successfully removed from the
   *         tree.
   */
  public boolean remove(String path) {
    writeLock.lock();
    try {
      // Find the node with this path
      PathTreeNode<T> locatedNode = findNode(path);
      if (locatedNode != null) {
        // Existing mounts may exist below and will be maintained.
        // If there are no children all nodes above this point should be
        // deleted until we find another mount point. This avoid leaving
        // dangling nodes that may get including in directory listings.
        if (locatedNode.getChildren().isEmpty()
            && locatedNode.getParent() != null) {
          PathTreeRemoveNodeCleanup<T> traverse =
              new PathTreeRemoveNodeCleanup<T>(locatedNode);

          // Traverse upwards starting with the node's parent
          locatedNode.getParent().traverseUp(traverse);

          // Delete
          for (PathTreeNode<T> deleteNode : traverse.getTraversedNodes()) {
            if (deleteNode.getParent() != null) {
              deleteNode.getParent().getChildren().remove(deleteNode.getName());
              deleteNode.setParent(null);
            }
          }
        }
        return true;
      } else {
        // Node to delete not found
        LOG.warn("Attempt to remove a non present tree node: " + path);
        return false;
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Add a path location to the tree.
   * @param path Global path in the tree.
   * @param value
   * @return True if the remote location was successfully added.
   */
  public boolean add(String path, T value) {
    writeLock.lock();
    try {
      String[] pathSplit = pathSplit(path);
      return this.root.add(pathSplit, value);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Locate the deepest mount point found for a path. This will always be equal
   * to or higher in the path tree than the requested path. The result contains
   * the remaining path beneath the deepest mount point.
   *
   * @param path The path components to search
   * @return The result or null if none is found.
   */
  public PathTreeResult<T> findDeepestReferencedNode(final String path) {
    readLock.lock();
    try {
      String[] pathSplit = pathSplit(path);
      PathTreeTraverse<T> traverse = new PathTreeTraverse<T>() {
        @Override
        public boolean isEligible(PathTreeNode<T> node, int nodeLevel) {
          return node.getValue() != null;
        }
        @Override
        public void traverse(PathTreeNode<T> node) { }
      };
      return this.root.traverseDown(pathSplit, traverse, 1);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Locate an exact node matching a path in the tree.
   * @param path The path components to search
   * @return The matching node in the tree or null if not found.
   */
  public PathTreeNode<T> findNode(final String path) {
    readLock.lock();
    try {
      final String[] pathSplit = pathSplit(path);
      PathTreeTraverse<T> traverse = new PathTreeTraverse<T>() {
        @Override
        public boolean isEligible(PathTreeNode<T> node, int nodeLevel) {
          // Find the node that matches our path
          return nodeLevel == pathSplit.length;
        }
        @Override
        public void traverse(PathTreeNode<T> node) { }
      };
      PathTreeResult<T> result = this.root.traverseDown(pathSplit, traverse, 0);
      if (result != null) {
        return result.getNode();
      }
      return null;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the child paths of the given path.
   * @param path Requested path.
   * @return child path points below the specified path or null if not found.
   */
  public List<String> getChildNames(String path) {
    readLock.lock();
    try {
      PathTreeNode<T> matchingNode = findNode(path);
      if (matchingNode != null) {
        LinkedList<String> ret = new LinkedList<String>();
        Set<Entry<String, PathTreeNode<T>>> entries =
            matchingNode.getChildren().entrySet();
        for (Entry<String, PathTreeNode<T>> entry : entries) {
          if (entry.getValue() != null) {
            ret.add(entry.getKey());
          }
        }
        return ret;
      }
      return null;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns a list of all paths at or beneath a path.
   * @param path The search path
   * @return A list of all paths beneath a path or null if not found.
   */
  public List<String> getSubPaths(String path) {
    readLock.lock();
    try {
      final PathTreeNode<T> matchingNode = findNode(path);
      if (matchingNode != null) {
        final List<String> ret = new LinkedList<String>();
        PathTreeTraverse<T> traverse = new PathTreeTraverse<T>() {
          @Override
          public boolean isEligible(PathTreeNode<T> node, int nodeLevel) {
            return true;
          }
          @Override
          public void traverse(PathTreeNode<T> node) {
            ret.add(node.getPath());
          }
        };

        matchingNode.traverseAll(traverse);
        return ret;
      }
      return null;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns a list of all node references beneath a path.
   * 
   * @param path
   * @return List of node references
   */
  public List<T> getAllReferences(String path) {
    readLock.lock();
    try {
      final PathTreeNode<T> matchingNode = findNode(path);
      if (matchingNode != null) {
        final List<T> ret = new LinkedList<T>();
        PathTreeTraverse<T> traverse = new PathTreeTraverse<T>() {
          @Override
          public boolean isEligible(PathTreeNode<T> node, int nodeLevel) {
            return true;
          }

          @Override
          public void traverse(PathTreeNode<T> node) {
            if (node.getValue() != null) {
              ret.add(node.getValue());
            }
          }
        };

        matchingNode.traverseAll(traverse);
        return ret;
      }
      return null;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the size of this tree.
   * @return Size of this tree.
   */
  public int size() {
    readLock.lock();
    try {
      int ret = 0;
      if (this.root != null) {
        ret = this.root.size();
      }
      return ret;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the number of values in the tree.
   * @return Number of values in the tree.
   */
  public int getNumOfLeaves() {
    readLock.lock();
    try {
      int ret = 0;
      if (this.root != null) {
        ret = this.root.getNumOfLeaves();
      }
      return ret;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String toString() {
    readLock.lock();
    try {
      return this.root.toString();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get a JSON representation of the tree.
   * @return JSON representation of the tree.
   */
  public String toJSON() {
    readLock.lock();
    try {
      Map<String, String> map = new TreeMap<String, String>();
      List<String> paths = this.getSubPaths("/");
      for (String path : paths) {
        PathTreeNode<T> node = this.findNode(path);
        if (node != null && node.getValue() != null) {
          map.put(path, node.getValue().toString());
        }
      }
      return JSON.toString(map);
    } finally {
      readLock.unlock();
    }
  }

}