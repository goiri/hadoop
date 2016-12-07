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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.util.Time.now;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * Utilities for managing HDFS federation.
 */
public final class FederationUtil {

  private static final Log LOG = LogFactory.getLog(FederationUtil.class);

  private FederationUtil() {
    // Utility Class
  }

  /**
   * Get a JMX data from a web endpoint.
   *
   * @param beanQuery JMX bean.
   * @param webAddress Web address of the JMX endpoint.
   * @return JSON with the JMX data
   */
  public static JSONArray getJmx(String beanQuery, String webAddress) {
    JSONArray ret = null;
    BufferedReader reader = null;
    try {
      URL jmxURL = new URL("http://" + webAddress + "/jmx?qry=" + beanQuery);
      InputStream in = jmxURL.openConnection().getInputStream();
      InputStreamReader isr = new InputStreamReader(in, "UTF-8");
      reader = new BufferedReader(isr);
      StringBuilder sb = new StringBuilder();
      String line = null;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      String jmxOutput = sb.toString();

      // Parse JSON
      JSONObject json = new JSONObject(jmxOutput);
      ret = json.getJSONArray("beans");
    } catch (Exception ex) {
      LOG.error("Unable to read JMX bean " + beanQuery + " from server " +
          webAddress, ex);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.info("Problem closing " + webAddress, e);
        }
      }
    }
    return ret;
  }

  /**
   * Create an instance of an interface with a constructor using a state store
   * constructor.
   *
   * @param conf Configuration
   * @param context Context object to pass to the instance.
   * @param contextType Type of the context passed to the constructor.
   * @param configurationKeyName Configuration key to retrieve the class to load
   * @param defaultClassName Default class to load if the configuration key is
   *          not set
   * @param clazz Class/interface that must be implemented by the instance.
   * @return New instance of the specified class that implements the desired
   *         interface and a single parameter constructor containing a
   *         StateStore reference.
   */
  public static Object createInstance(Configuration conf, Object context,
      Class<?> contextType, String configurationKeyName,
      String defaultClassName, Class<?> clazz) {

    String className = conf.get(configurationKeyName, defaultClassName);
    try {
      Class<?> clusterResolverClass = conf.getClassByName(className);
      if (clazz.isAssignableFrom(clusterResolverClass)) {
        Constructor<?> constructor = clusterResolverClass
            .getConstructor(Configuration.class, contextType);
        return constructor.newInstance(conf, context);
      } else {
        throw new RuntimeException("Class " + className + " not instance of "
            + clazz.getCanonicalName());
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: " + className, e);
      return null;
    }
  }

  /**
   * Create an instance of an interface with a constructor using a Router
   * constructor.
   *
   * @param conf Configuration
   * @param router Implementation of a Router
   * @param configurationKeyName Configuration key to retrieve the class to load
   * @param defaultClassName Default class to load if the configuraiton key is
   *          not set
   * @param clazz Type/interface that must be implemented by the class
   * @return New instance of the specified class that implements the desired
   *         interface and a single parameter constructor containing a
   *         StateStore reference.
   */
  public static Object createInstance(Configuration conf, Router router,
      String configurationKeyName, String defaultClassName, Class<?> clazz) {

    String className = conf.get(configurationKeyName, defaultClassName);
    try {
      Class<?> clusterResolverClass = conf.getClassByName(className);
      if (clazz.isAssignableFrom(clusterResolverClass)) {
        Constructor<?> constructor =
            clusterResolverClass.getConstructor(Router.class);
        return constructor.newInstance(router);
      } else {
        throw new RuntimeException("Class " + className + " not instance of "
            + clazz.getCanonicalName());
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: " + className, e);
      return null;
    }
  }

  /**
   * Create an instance of an interface using a default constructor.
   *
   * @param conf Configuration
   * @param configuredClassName
   * @param defaultClassName
   * @param type
   * @return New instance of the specified class that implements the specified
   *         interface and a default constructor.
   */
  public static Object createInstance(Configuration conf,
      String configuredClassName, String defaultClassName, Class<?> clazz) {

    String className = conf.get(configuredClassName, defaultClassName);
    try {
      Class<?> clusterResolverClass = conf.getClassByName(className);
      if (clazz.isAssignableFrom(clusterResolverClass)) {
        Constructor<?> constructor = clusterResolverClass.getConstructor();
        return constructor.newInstance();
      } else {
        throw new RuntimeException("Class " + className + " not instance of "
            + clazz.getCanonicalName());
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: " + className, e);
      return null;
    }
  }

  /**
   * Get the name of the method that is calling this function.
   *
   * @return Name of the method calling this function.
   */
  public static String getMethodName() {
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    String methodName = stack[3].getMethodName();
    return methodName;
  }

  /**
   * Get the number of seconds passed since a date.
   *
   * @param date to use as a reference.
   * @return Seconds since the date.
   */
  public static long getSecondsSince(Date date) {
    return (now() - date.getTime()) / 1000;
  }

  public static long getSecondsSince(long timeMs) {
    return (now() - timeMs) / 1000;
  }

  /**
   * Fetch the compile timestamp for this jar.
   *
   * @return Date compiled
   */
  public static String getCompiledDate() {
    return VersionInfo.getDate();
  }

  /**
   * Fetch the build/compile information for this jar.
   *
   * @return String
   */
  public static String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() + " from "
        + VersionInfo.getBranch();
  }

  /**
   * Get the router RPC server address.
   *
   * @param conf Configuration for the router.
   * @return Router RPC server address.
   */
  public static InetSocketAddress getRouterRpcServerAddress(
      Configuration conf) {
    String address = conf.getTrimmed(
        DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(address);
  }

  /**
   * Get the router RPC bind server address.
   *
   * @param conf Configuration for the router.
   * @return Router RPC bind server address.
   */
  public static String getRouterRpcServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf, DFSConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY);
  }

  public static InetSocketAddress getRouterAdminServerAddress(
      Configuration conf) {
    String address = conf.getTrimmed(
        DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(address);
  }

  public static String getRouterAdminServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf,
        DFSConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY);
  }

  public static String getRouterHttpServerHostAndPort(Configuration conf) {
    return conf.getTrimmed(
        DFSConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_ROUTER_HTTP_ADDRESS_DEFAULT);
  }

  public static InetSocketAddress getRouterHttpServerAddress(
      Configuration conf) {
    String address = getRouterHttpServerHostAndPort(conf);
    return NetUtils.createSocketAddr(address);
  }

  public static String getRouterHttpServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf,
        DFSConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY);
  }

  public static InetSocketAddress getRouterHttpsServerAddress(
      Configuration conf) {
    String address = conf.getTrimmed(
        DFSConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(address);
  }

  public static String getRouterHttpsServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf,
        DFSConfigKeys.DFS_ROUTER_HTTPS_BIND_HOST_KEY);
  }

  /**
   * Gets a trimmed value from configuration, or null if no value is defined.
   *
   * @param conf Configuration for the Router.
   * @param key Configuration key to get.
   * @return Trimmed value, or null if no value is defined.
   */
  public static String getTrimmedOrNull(Configuration conf, String key) {
    String addr = conf.getTrimmed(key);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return addr;
  }

  /**
   * Generate the identifier for a Namenode in the HDFS federation.
   *
   * @param ns Nameservice of the Namenode.
   * @param nn Namenode within the Nameservice (HA).
   * @return Namenode identifier within the federation.
   */
  public static String generateNamenodeId(String ns, String nn) {
    return ns + "-" + nn;
  }

  /**
   * Creates an instance of a FileSubclusterResolver from the configuration.
   *
   * @param conf Configuration that defines the file resolver class.
   * @param obj Context object passed to class constructor.
   * @return FileSubclusterResolver
   */
  public static FileSubclusterResolver createFileSubclusterResolver(
      Configuration conf, Object obj, Class<?> objType) {
    return (FileSubclusterResolver) FederationUtil.createInstance(
        conf, obj, objType,
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT,
        FileSubclusterResolver.class);
  }

  /**
   * Creates an instance of an ActiveNamenodeResolver from the configuration.
   *
   * @param conf Configuration that defines the namenode resolver class.
   * @param obj Context object passed to class constructor.
   * @return ActiveNamenodeResolver
   */
  public static ActiveNamenodeResolver createActiveNamenodeResolver(
      Configuration conf, Object obj, Class<?> objType) {
    return (ActiveNamenodeResolver) FederationUtil.createInstance(
        conf, obj, objType,
        DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS_DEFAULT,
        ActiveNamenodeResolver.class);
  }
}
