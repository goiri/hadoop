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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Before;
import org.junit.Test;

public class TestMultipleDestinationResolver {

  private MultipleDestinationMountTableResolver resolver;

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    resolver = new MultipleDestinationMountTableResolver(conf, null);

    // We manually point /tmp to only subcluster0
    Map<String, String> map = new HashMap<String, String>();
    map.put("subcluster0", "/tmp");
    resolver.addEntry(MountTable.newInstance("/tmp", map));

    // We manually point / to subcluster0,1,2
    map = new HashMap<String, String>();
    map.put("subcluster0", "/");
    map.put("subcluster1", "/");
    map.put("subcluster2", "/");
    resolver.addEntry(MountTable.newInstance("/", map));
  }

  @Test
  public void testEqualDistribution() throws IOException {
    // Subcluster -> Files
    Map<String, Set<String>> results = new HashMap<String, Set<String>>();
    for (int f = 0; f < 10000; f++) {
      String filename = "/file" + f + ".txt";
      PathLocation destination = resolver.getDestinationForPath(filename);
      RemoteLocation loc = destination.getDefaultLocation();
      assertEquals(filename, loc.getDest());

      if (!results.containsKey(loc.getNameserviceId())) {
        results.put(loc.getNameserviceId(), new TreeSet<String>());
      }
      results.get(loc.getNameserviceId()).add(filename);
    }
    // Check that they are semi-equally distributed
    int count = 0;
    for (Set<String> files : results.values()) {
      count = count + files.size();
    }
    int avg = count / results.keySet().size();
    for (Set<String> files : results.values()) {
      int filesCount = files.size();
      // Check that the count in each namespace is within 20% of avg over 10000
      // files.
      assertTrue(filesCount > 0);
      assertTrue(Math.abs(filesCount - avg) < (avg / 5));
    }
  }

  @Test
  public void testMountDistribution() throws IOException {
    // All the files in /tmp should be in subcluster0
    for (int f=0; f<100; f++) {
      String filename = "/tmp/b/c/file" + f + ".txt";
      PathLocation destination = resolver.getDestinationForPath(filename);
      RemoteLocation loc = destination.getDefaultLocation();
      assertEquals("subcluster0", loc.getNameserviceId());
      assertEquals(filename, loc.getDest());
    }
  }

  @Test
  public void testResolveSubdirectories() throws Exception {
    // Simulate a testdir under a multi-destination mount.
    Random r = new Random();
    String testDir = "/sort/testdir" + r.nextInt();
    String file1 = testDir + "/file1" + r.nextInt();
    String file2 = testDir + "/file2" + r.nextInt();

    // Verify both files resolve to the same namespace as the parent dir.
    PathLocation testDirLocation = resolver.getDestinationForPath(testDir);
    String testDirNamespace =
        testDirLocation.getDefaultLocation().getNameserviceId();

    PathLocation file1Location = resolver.getDestinationForPath(file1);
    assertEquals(testDirNamespace,
        file1Location.getDefaultLocation().getNameserviceId());

    PathLocation file2Location = resolver.getDestinationForPath(file2);
    assertEquals(testDirNamespace,
        file2Location.getDefaultLocation().getNameserviceId());
  }
}
