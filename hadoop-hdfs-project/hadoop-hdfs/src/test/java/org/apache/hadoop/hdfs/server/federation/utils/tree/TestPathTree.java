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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeNode;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTreeUtils;
import org.junit.Test;

public class TestPathTree {

  /**
   * Basic operations in the tree.
   */
  @Test
  public void testBasic() {
    // Adding nodes
    PathTree<Integer> tree = new PathTree<Integer>();
    assertEquals(1, tree.size());
    assertEquals(0, tree.getNumOfLeaves());

    tree.add("/", 5);
    assertEquals(1, tree.getNumOfLeaves());

    tree.add("/a/a", 7);
    tree.add("/a/b", 23);
    tree.add("/b", 5);
    tree.add("/b/a", 9);
    tree.add("/b/b", 11);
    assertEquals(7, tree.size());
    assertEquals(6, tree.getNumOfLeaves());

    // Finding nodes
    PathTreeNode<Integer> node = tree.findNode("/b/a");
    assertEquals(9, node.getValue().intValue());

    tree.add("/b/a", node.getValue() + 1);
    node = tree.findNode("/b/a");
    assertEquals(node.getValue().intValue(), 10);

    // Parsing trees
    String json = tree.toJSON();
    PathTree<Integer> tree2 = new PathTree<Integer>(json);

    assertEquals(tree.toJSON(), tree2.toJSON());
  }

  /**
   * Merge multiple trees into a single one.
   */
  @Test
  public void testMerge() {
    PathTree<Long> tree1 = new PathTree<Long>();
    tree1.add("/", 5L);
    tree1.add("/a", 2L);
    tree1.add("/a/a", 3L);
    tree1.add("/a/b", 4L);
    tree1.add("/b/a", 1L);
    tree1.add("/b/b", 7L);
    tree1.add("/b/b/a", 3L);

    PathTree<Long> tree2 = new PathTree<Long>();
    tree2.add("/a/b", 1L);
    tree2.add("/b/b/a", 3L);

    // Merge trees into a single one
    PathTree<Long> treeMerge = new PathTree<Long>();
    PathTreeUtils.add(treeMerge, tree1);
    assertEquals(tree1.toJSON(), treeMerge.toJSON());

    PathTreeUtils.add(treeMerge, tree2);
    assertEquals(3L, treeMerge.findNode("/a/a").getValue().longValue());
    assertEquals(5L, treeMerge.findNode("/a/b").getValue().longValue());
    assertEquals(6L, treeMerge.findNode("/b/b/a").getValue().longValue());
  }

  @Test
  public void testMax() {
    PathTree<Long> tree1 = new PathTree<Long>();
    tree1.add("/", 5L);
    tree1.add("/a", 2L);
    tree1.add("/a/a", 3L);
    tree1.add("/a/b", 4L);
    tree1.add("/b/a", 1L);
    tree1.add("/b/b", 7L);
    tree1.add("/b/b/a", 3L);

    PathTree<Long> tree2 = new PathTree<Long>();
    tree2.add("/a/b", 1L);
    tree2.add("/b/b/a", 5L);

    // Adding the first tree
    PathTree<Long> treeMax = new PathTree<Long>();
    PathTreeUtils.max(treeMax, tree1);

    assertEquals(3L, treeMax.findNode("/a/a").getValue().longValue());
    assertEquals(4L, treeMax.findNode("/a/b").getValue().longValue());
    assertEquals(3L, treeMax.findNode("/b/b/a").getValue().longValue());

    // Adding the second tree
    PathTreeUtils.max(treeMax, tree2);
    assertEquals(3L, treeMax.findNode("/a/a").getValue().longValue());
    assertEquals(4L, treeMax.findNode("/a/b").getValue().longValue());
    assertEquals(5L, treeMax.findNode("/b/b/a").getValue().longValue());
  }

  @Test
  public void testTrim() {
    PathTree<Long> tree = new PathTree<Long>();
    tree.add("/", 500L);
    tree.add("/a", 200L);
    tree.add("/a/a", 300L);
    tree.add("/a/b", 400L);
    tree.add("/b/a", 100L);
    tree.add("/b/b/1", 20L);
    tree.add("/b/b/2", 3L);
    tree.add("/b/b/3", 6L);
    tree.add("/b/b/4", 4L);
    tree.add("/b/b/5", 9L);
    tree.add("/b/b/6", 9L);
    tree.add("/b/b/7", 7L);
    tree.add("/b/b/8", 6L);
    tree.add("/b/b/9", 5L);

    tree.add("/c/a/a/1", 3L);
    tree.add("/c/a/a/2", 3L);
    tree.add("/c/a/a/3", 3L);
    tree.add("/c/a/a/4", 3L);
    tree.add("/c/a/b/1", 4L);
    tree.add("/c/a/b/2", 4L);
    tree.add("/c/a/b/3", 4L);
    tree.add("/c/a/b/4", 4L);

    System.out.println("Pre");
    System.out.println(tree);

    PathTreeUtils.trim(tree, 0.01f);

    System.out.println("Post");
    System.out.println(tree);
  }
}