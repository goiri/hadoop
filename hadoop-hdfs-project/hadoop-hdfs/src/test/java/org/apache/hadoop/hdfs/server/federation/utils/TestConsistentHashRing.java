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
package org.apache.hadoop.hdfs.server.federation.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class TestConsistentHashRing {

  private final static int NUM_FILES = 1000;

  @Test
  public void testRing() {
    Set<String> locations = new HashSet<String>();
    for (int sc=0; sc<5; sc++) {
      locations.add("subcluster" + sc);
    }
    ConsistentHashRing ring = new ConsistentHashRing(locations);

    // Check that 1000 files are distributed homogeneously
    Map<String, Set<String>> results0 = assignFiles(ring);
    verifyHomogeneousDistribution(ring, results0);

    // Remove a location and check how many items moved
    ring.removeLocation("subcluster4");
    Map<String, Set<String>> results1 = assignFiles(ring);
    verifyHomogeneousDistribution(ring, results1);

    // Items in the old locations should stay there
    for (int sc=0; sc<4; sc++) {
      String subcluster = "subcluster" + sc;
      Set<String> files0 = results0.get(subcluster);
      Set<String> files1 = results1.get(subcluster);
      assertTrue(files1.containsAll(files0));
    }
  }

  /**
   * Assign a set of files across locations.
   * @param ring Consistent hash ring. 
   * @return Distribution of the files across locations.
   */
  public Map<String, Set<String>> assignFiles(ConsistentHashRing ring) {
    Map<String, Set<String>> results = new HashMap<String, Set<String>>();
    for (int f=0; f<NUM_FILES; f++) {
      String filename = "file-" + f;
      String location = ring.getLocation(filename);
      if (!results.containsKey(location)) {
        results.put(location, new HashSet<String>());
      }
      results.get(location).add(filename);
    }
    return results;
  }

  /**
   * Verify that a set of items are homogeneously distributed.
   * @param ring Consistent hash ring.
   * @param results Item assignment in the ring to verify.
   */
  public static void verifyHomogeneousDistribution(ConsistentHashRing ring,
      Map<String, Set<String>> results){
    Set<String> locations = ring.getLocations();
    int minExpected = NUM_FILES / (locations.size() + 1);
    int maxExpected = NUM_FILES / (locations.size() - 1);
    int sumFiles = 0;
    for (String location : locations) {
      int numFiles = results.get(location).size();
      assertTrue(numFiles > minExpected);
      assertTrue(numFiles < maxExpected);
      sumFiles += numFiles;
    }
    assertEquals(NUM_FILES, sumFiles);
  }

  /**
   * This generates an assignment of machines to subclusters that should match
   * with the C# version of the code.
   */
  @Test
  public void testComparissonCSharp() {
    Set<String> locations = new HashSet<String>();
    for (int sc=0; sc<4; sc++) {
      locations.add("subcluster" + sc);
    }
    ConsistentHashRing ring = new ConsistentHashRing(locations);

    for (int podset = 0; podset<5; podset++) {
      for (int pod = 0; pod<5; pod++) {
        String machine = String.format("%05d%02d", podset, pod);
        String location = ring.getLocation(machine);
        System.out.println("BN2SCH" + machine + "22 " + location);
      }
    }
  }
}
