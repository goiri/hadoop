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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.CancelDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryServlet;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.FileDataServlet;
import org.apache.hadoop.hdfs.server.namenode.FsckServlet;
import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.server.namenode.ListPathsServlet;
import org.apache.hadoop.hdfs.server.namenode.RenewDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.StartupProgressServlet;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;

/**
 * Web interface for the {@link Router}. It exposes the Web UI and the WebHDFS
 * methods from {@link RouterWebHdfsMethods}.
 */
public class RouterHttpServer extends AbstractService {
  private static final Log LOG =
      LogFactory.getLog(RouterHttpServer.class);

  private HttpServer2 httpServer;

  private Router router;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;

  private Configuration conf;

  protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";

  public RouterHttpServer(Router router) {
    super(RouterHttpServer.class.getName());
    this.router = router;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;

    // Get HTTP address
    this.httpAddress = FederationUtil.getRouterHttpServerAddress(conf);
    String httpBindHost = FederationUtil.getRouterHttpServerBindHost(conf);
    if (httpBindHost != null) {
      this.httpAddress =
          new InetSocketAddress(httpBindHost, httpAddress.getPort());
    }

    // Get HTTPS address
    this.httpsAddress = FederationUtil.getRouterHttpsServerAddress(conf);
    String httpsBindHost = FederationUtil.getRouterHttpsServerBindHost(conf);
    if (httpsBindHost != null) {
      this.httpsAddress =
          new InetSocketAddress(httpsBindHost, httpsAddress.getPort());
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // Build and start server
    String webApp = "router";
    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(
        this.conf, this.httpAddress, this.httpsAddress, webApp,
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY);

    this.httpServer = builder.build();

    initWebHdfs();

    this.httpServer.setAttribute(NAMENODE_ATTRIBUTE_KEY, this.router);
    this.httpServer.setAttribute(JspHelper.CURRENT_CONF, this.conf);
    setupServlets(this.httpServer, this.conf);

    this.httpServer.start();

    // The server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.httpServer.getConnectorAddress(0);
    if (listenAddress != null) {
      this.httpAddress = new InetSocketAddress(this.httpAddress.getHostName(),
          listenAddress.getPort());
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if(this.httpServer != null) {
      this.httpServer.stop();
    }
    super.serviceStop();
  }

  // TODO this is repeated from NameNodeHttpServer
  private void initWebHdfs() throws IOException {
    LOG.info("Initializing WebHDFS...");
    if (WebHdfsFileSystem.isEnabled(conf, HttpServer2.LOG)) {
      // Set user pattern based on configuration file
      UserParam.setUserPattern(conf.get(
          DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
          DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));

      // Add authentication filter for webhdfs
      final String className = conf.get(
          DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY,
          DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT);
      final String name = className;

      final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";
      Map<String, String> params = getAuthFilterParams();
      HttpServer2.defineFilter(this.httpServer.getWebAppContext(), name,
          className, params, new String[] {pathSpec});
      HttpServer2.LOG.info("Added filter '" + name + "' (class=" + className
          + ")");

      // Add webhdfs packages
      this.httpServer.addJerseyResourcePackage(RouterWebHdfsMethods.class
          .getPackage().getName() + ";" + Param.class.getPackage().getName(),
          pathSpec);
      LOG.info("WebHDFS enabled");
    } else {
      LOG.info("WebHDFS not enabled");
    }
  }

  // TODO this is repeated from NameNodeHttpServer
  private Map<String, String> getAuthFilterParams()
      throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    String principalInConf = conf
        .get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
    if (principalInConf != null && !principalInConf.isEmpty()) {
      params
          .put(
              DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
              SecurityUtil.getServerPrincipal(principalInConf,
                  httpAddress.getHostName())); // bindAddress TODO
    } else if (UserGroupInformation.isSecurityEnabled()) {
      HttpServer2.LOG.error(
          "WebHDFS and security are enabled, but configuration property '" +
          DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY +
          "' is not set.");
    }
    String httpKeytab = conf.get(DFSUtil.getSpnegoKeytabKey(conf,
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put(
          DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
          httpKeytab);
    } else if (UserGroupInformation.isSecurityEnabled()) {
      HttpServer2.LOG.error(
          "WebHDFS and security are enabled, but configuration property '"
              + DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY
              + "' is not set.");
    }
    String anonymousAllowed =
        conf.get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED);
    if (anonymousAllowed != null && !anonymousAllowed.isEmpty()) {
      params.put(
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
        anonymousAllowed);
    }
    return params;
  }

  // TODO this is repeated from NameNodeHttpServer
  private static void setupServlets(
      HttpServer2 httpServer, Configuration conf) {
    httpServer.addInternalServlet("startupProgress",
        StartupProgressServlet.PATH_SPEC, StartupProgressServlet.class);
    httpServer.addInternalServlet("getDelegationToken",
        GetDelegationTokenServlet.PATH_SPEC,
        GetDelegationTokenServlet.class, true);
    httpServer.addInternalServlet("renewDelegationToken",
        RenewDelegationTokenServlet.PATH_SPEC,
        RenewDelegationTokenServlet.class, true);
    httpServer.addInternalServlet("cancelDelegationToken",
        CancelDelegationTokenServlet.PATH_SPEC,
        CancelDelegationTokenServlet.class, true);
    httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class,
        true);
    httpServer.addInternalServlet("imagetransfer", ImageServlet.PATH_SPEC,
        ImageServlet.class, true);
    httpServer.addInternalServlet("listPaths", "/listPaths/*",
        ListPathsServlet.class, false);
    httpServer.addInternalServlet("data", "/data/*",
        FileDataServlet.class, false);
    httpServer.addInternalServlet("checksum", "/fileChecksum/*",
        FileChecksumServlets.RedirectServlet.class, false);
    httpServer.addInternalServlet("contentSummary", "/contentSummary/*",
        ContentSummaryServlet.class, false);
  }

  public InetSocketAddress getHttpAddress() {
    return this.httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return this.httpsAddress;
  }
}