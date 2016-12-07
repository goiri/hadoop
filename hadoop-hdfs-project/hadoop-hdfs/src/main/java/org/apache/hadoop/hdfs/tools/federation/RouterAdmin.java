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
package org.apache.hadoop.hdfs.tools.federation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.FederationMountTableStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.FederationProtocolFactory;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.utils.tree.PathTree;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides some DFS administrative access shell commands.
 */
@Private
public class RouterAdmin extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(RouterAdmin.class);
  private RouterClient client;

  public static long ROUTER_PERF_ALL = 0xFFFFFFFF;
  public static long ROUTER_PERF_FLAG_RPC = 0x01;
  public static long ROUTER_PERF_FLAG_STATESTORE = 0x02;
  public static long ROUTER_PERF_FLAG_ERRORS = 0x04;

  public static void main(String[] argv) throws Exception {
    Configuration conf = new HdfsConfiguration();
    RouterAdmin admin = new RouterAdmin(conf);

    int res = ToolRunner.run(admin, argv);
    System.exit(res);
  }

  public RouterAdmin(Configuration conf) {
    super(conf);
  }

  public void printUsage() {
    String usage = "Federation Admin Tools:\n"
        + "\t[-addMount <source> <nameservice> <destination>]\n"
        + "\t[-removeMount <source>]\n"
        + "\t[-addDestination <source> <nameservice> <destination>]\n"
        + "\t[-mounts <path>]\n"
        + "\t[-stats <time>]\n"
        + "\t[-perfBenchmark <router_path> <namenode_path> <num_reps> [-stabilize <stabilization_reps>] [-test <get/alter/create/read/write>]\n"
        + "\t[-resetPerfCounters [rpc, statestore, error]\n"
        + "\t[-setConfiguration <key> <value>\n"
        + "\t[-restartRpcServer\n";
    System.out.println(usage);
  }

  public int run(String[] argv) throws Exception {
    if (argv.length < 1) {
      System.err.println("Not enough parameters specificed");
      printUsage();
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-addMount".equals(cmd) || "-addDestination".equals(cmd)
        || "-moveMount".equals(cmd)) {
      if (argv.length < 4) {
        System.err.println("Not enough parameters specificed for cmd - " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-removeMount".equalsIgnoreCase(cmd)
        || "-getMount".equalsIgnoreCase(cmd)) {
      if (argv.length < 2) {
        System.err.println("Not enough parameters specificed for cmd - " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-perfBenchmark".equalsIgnoreCase(cmd)) {
      if (argv.length < 4) {
        System.err.println("Not enough parameters specificed for cmd - " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-setConfiguration".equalsIgnoreCase(cmd)) {
      if (argv.length < 3) {
        System.err.println("Not enough parameters specificed for cmd - " + cmd);
        printUsage();
        return exitCode;
      }
    }

    // initialize RouterClient
    try {
      InetSocketAddress routerSocket =
          FederationUtil.getRouterAdminServerAddress(getConf());
      client = new RouterClient(routerSocket, getConf());
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server"
          + "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to Router... command aborted.");
      return exitCode;
    }

    Exception debugException = null;
    exitCode = 0;
    try {
      if ("-addMount".equals(cmd)) {
        addMount(argv, i);
        System.err.println("Successfuly added mount point - " + argv[i]);
      } else if ("-addDestination".equals(cmd)) {
        addDestination(argv, i);
        System.err.println("Successfuly edited mount point - " + argv[i]);
      } else if ("-removeMount".equals(cmd)) {
        removeMount(argv[i]);
        System.err.println("Successfully removed mount point - " + argv[i]);
      } else if ("-mounts".equals(cmd)) {
        if (argv.length > 1) {
          listMounts(argv[i]);
        } else {
          listMounts("/");
        }
      } else if ("-stats".equals(cmd)) {
        long timeWindowMillis = 0L;
        if (i < argv.length) {
          String timeWindowHoursStr = argv[i];
          long timeWindowHours = Long.parseLong(timeWindowHoursStr);
          timeWindowMillis = TimeUnit.HOURS.toMillis(timeWindowHours);
        }
        getPathStats(timeWindowMillis, true);
      } else if ("-resetPerfCounters".equals(cmd)) {
        if (argv.length > 1) {
          resetPerfCounters(argv[i]);
        } else {
          resetPerfCounters("all");
        }
      } else if ("-setConfiguration".equals(cmd)) {
        setConfiguration(argv[i], argv[i + 1]);
      } else if ("-restartRpcServer".equals(cmd)) {
        restartRpcServer();
      } else {
        printUsage();
        return exitCode;
      }
    } catch (IllegalArgumentException arge) {
      debugException = arge;
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage();
    } catch (RemoteException e) {
      // This is a error returned by hadoop server.
      // Print out the first line of the error message, ignore the stack trace.
      exitCode = -1;
      debugException = e;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " + content[0]);
        e.printStackTrace();
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage());
        e.printStackTrace();
        debugException = ex;
      }
    } catch (Exception e) {
      exitCode = -1;
      debugException = e;
      System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage());
      e.printStackTrace();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exception encountered:", debugException);
    }
    return exitCode;
  }

  /**
   * Add a mount table entry.
   * @param parameters
   * @param i
   * @return
   */
  public void addMount(String[] parameters, int i) throws IOException {
    String mount = parameters[i++];
    String ns = parameters[i++];
    String dest = parameters[i++];
    Map<String, String> destMap = new HashMap<String, String>();
    destMap.put(ns, dest);
    MountTable newEntry = MountTable.newInstance(mount, destMap);
    AddMountTableEntryRequest request =
        FederationProtocolFactory.newInstance(AddMountTableEntryRequest.class);
    request.setEntry(newEntry);
    AddMountTableEntryResponse response =
        client.getMountTableProtocol().addMountTableEntry(request);
    if (response.getStatus()) {
      System.out.println("Failed to add mount point - " + mount);
    }
  }

  public void addDestination(String[] parameters, int i) throws IOException {
    String mount = parameters[i++];
    String ns = parameters[i++];
    String dest = parameters[i++];
    // Get the existing entry
    MountTable existingEntry = FederationMountTableStoreUtils
        .getMountTableEntry(client.getMountTableProtocol(), mount);
    if (existingEntry == null) {
      throw new IOException("Unable to locate existing mount - " + mount);
    }
    existingEntry.addDestination(ns, dest);
    UpdateMountTableEntryRequest request = FederationProtocolFactory
        .newInstance(UpdateMountTableEntryRequest.class);
    request.setEntry(existingEntry);
    UpdateMountTableEntryResponse response =
        client.getMountTableProtocol().updateMountTableEntry(request);
    if (!response.getStatus()) {
      System.out.println("Failed to update mount point - " + mount);
    } else {
      System.out.println("Updated mount point - " + mount
          + " to include destinations - " + existingEntry.getDestinations());
    }
  }

  public void removeMount(String mount) throws IOException {
    RemoveMountTableEntryRequest request = FederationProtocolFactory
        .newInstance(RemoveMountTableEntryRequest.class);
    request.setSrcPath(mount);
    RemoveMountTableEntryResponse response =
        client.getMountTableProtocol().removeMountTableEntry(request);
    if (response.getStatus()) {
      System.out.println("Failed to remove mount point - " + mount);
    }
  }

  public void listMounts(String path) throws IOException {
    GetMountTableEntriesRequest request = FederationProtocolFactory
        .newInstance(GetMountTableEntriesRequest.class);
    request.setSrcPath(path);
    GetMountTableEntriesResponse response =
        client.getMountTableProtocol().getMountTableEntries(request);
    printMounts(response.getEntries());
  }

  /**
   * Get path statistics.
   * @param time Time window in milliseconds
   * @param max If we report the maximum or the aggregate.
   * @throws IOException
   */
  public void getPathStats(long time, boolean max) throws IOException {
    PathTree<Long> tree = client.getAdminProtocol().getPathStats(time, max);
    System.out.println(tree);
  }

  public void resetPerfCounters(String flags) throws IOException {
    long reset = RouterAdmin.ROUTER_PERF_ALL;
    if (flags.equals("rpc")) {
      reset = RouterAdmin.ROUTER_PERF_FLAG_RPC;
    } else if (flags.equals("statestore")) {
      reset = RouterAdmin.ROUTER_PERF_FLAG_STATESTORE;
    } else if (flags.equals("errors")) {
      reset = RouterAdmin.ROUTER_PERF_FLAG_ERRORS;
    }
    client.getAdminProtocol().resetPerfCounters(reset);
  }

  private void restartRpcServer() throws IOException {
    client.getAdminProtocol().restartRpcServer();
  }

  private void setConfiguration(String key, String value) throws IOException {
    client.getAdminProtocol().setConfiguration(key, value);
  }

  private void printMounts(List<MountTable> entries) {
    System.out.println("Mount Table Entries:");
    System.out.println(String.format(
        "%-25s %-25s",
        "Source", "Destinations"));
    for (MountTable entry : entries) {
      StringBuilder destBuilder = new StringBuilder();
      for(RemoteLocation location : entry.getDestinations()) {
        if(destBuilder.length() > 0) {
          destBuilder.append(",");
        }
        destBuilder.append(String.format("%s->%s", location.getNameserviceId(),
            location.getDest()));
      }
      System.out.println(String.format("%-25s %-25s", entry.getSourcePath(),
          destBuilder.toString()));
    }
  }
}