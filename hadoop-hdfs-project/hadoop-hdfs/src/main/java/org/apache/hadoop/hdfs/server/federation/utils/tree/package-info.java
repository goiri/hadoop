/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A library used to efficiently query and map hierarchical file paths. Each
 * file path is stored in a tree, one node for each path component. Assorted
 * query routines are provided to locate the closest matching node in the tree
 * for a given file path.
 * <p>
 * Each {@link PathTreeNode} contains an optional reference parameter to an
 * external object. This object is returned in the result of each path query. A
 * new tree can be instantiated, queried and updated using the {@link PathTree}.
 * <p>
 * Common APIs available:
 * <ul>
 * <li>public boolean add(String path, T value) -> Adds a path and reference
 * object to the tree
 * <li>public boolean remove(String path) -> Removes a path from the tree
 * <li>public PathTreeResult<T> findDeepestReferencedNode(final String path) ->
 * Find the deepest node in the tree that is a partial match for the supplied
 * path.
 * <li>public List<String> getSubPaths(String path) -> Get all defned paths in
 * the tree below a given path.
 * <li>public List<T> getAllReferences(String path) -> Get all reference objects
 * found below a given path.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
package org.apache.hadoop.hdfs.server.federation.utils.tree;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
