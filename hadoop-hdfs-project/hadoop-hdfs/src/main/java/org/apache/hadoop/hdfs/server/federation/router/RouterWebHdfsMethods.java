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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.ExcludeDatanodesParam;
import org.apache.hadoop.hdfs.web.resources.FsActionParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.TokenKindParam;
import org.apache.hadoop.hdfs.web.resources.TokenServiceParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.hdfs.web.resources.XAttrEncodingParam;
import org.apache.hadoop.hdfs.web.resources.XAttrNameParam;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.collect.Lists;
import com.sun.jersey.spi.container.ResourceFilters;

/**
 * WebHDFS Router implementation. This is basically a copy of
 * {@link NamenodeWebHdfsMethods} which should be reused as much as possible
 * TODO POST/PUT methods
 */
@Path("")
@ResourceFilters(ParamFilter.class)
public class RouterWebHdfsMethods extends NamenodeWebHdfsMethods {
  public static final Log LOG =
      LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  private static final ThreadLocal<String> REMOTE_ADDRESS =
      new ThreadLocal<String>();

  private @Context ServletContext context;
  private @Context HttpServletRequest request;
  private @Context HttpServletResponse response;

  private void init(final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + ", " + username + ", " + doAsUser
          + Param.toSortedString(", ", parameters));
    }

    // Clear content type
    response.setContentType(null);

    // Cet the remote address, if coming in via a trust proxy server then
    // the address with be that of the proxied client
    REMOTE_ADDRESS.set(JspHelper.getRemoteAddr(request));
  }

  private void reset() {
    REMOTE_ADDRESS.set(null);
  }

  private static ClientProtocol getRPCServer(Router router)
      throws IOException {
     final ClientProtocol np = router.getRpcServer();
     if (np == null) {
       throw new RetriableException("Router is in startup mode");
     }
     return np;
  }

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response getRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT)
          final List<XAttrNameParam> xattrNames,
      @QueryParam(XAttrEncodingParam.NAME) @DefaultValue(XAttrEncodingParam.DEFAULT)
          final XAttrEncodingParam xattrEncoding,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(FsActionParam.NAME) @DefaultValue(FsActionParam.DEFAULT)
          final FsActionParam fsAction,
      @QueryParam(TokenKindParam.NAME) @DefaultValue(TokenKindParam.DEFAULT)
          final TokenKindParam tokenKind,
      @QueryParam(TokenServiceParam.NAME) @DefaultValue(TokenServiceParam.DEFAULT)
          final TokenServiceParam tokenService
      ) throws IOException, InterruptedException {
    return get(ugi, delegation, username, doAsUser, ROOT, op, offset, length,
        renewer, bufferSize, xattrNames, xattrEncoding, excludeDatanodes, fsAction,
        tokenKind, tokenService);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT)
          final List<XAttrNameParam> xattrNames,
      @QueryParam(XAttrEncodingParam.NAME) @DefaultValue(XAttrEncodingParam.DEFAULT)
          final XAttrEncodingParam xattrEncoding,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(FsActionParam.NAME) @DefaultValue(FsActionParam.DEFAULT)
          final FsActionParam fsAction,
      @QueryParam(TokenKindParam.NAME) @DefaultValue(TokenKindParam.DEFAULT)
          final TokenKindParam tokenKind,
      @QueryParam(TokenServiceParam.NAME) @DefaultValue(TokenServiceParam.DEFAULT)
          final TokenServiceParam tokenService
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, offset, length,
        renewer, bufferSize, xattrEncoding, excludeDatanodes, fsAction,
        tokenKind, tokenService);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        try {
          return get(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, offset, length, renewer, bufferSize,
              xattrNames, xattrEncoding, excludeDatanodes, fsAction, tokenKind,
              tokenService);
        } finally {
          reset();
        }
      }
    });
  }

  private Response get(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final GetOpParam op,
      final OffsetParam offset,
      final LengthParam length,
      final RenewerParam renewer,
      final BufferSizeParam bufferSize,
      final List<XAttrNameParam> xattrNames,
      final XAttrEncodingParam xattrEncoding,
      final ExcludeDatanodesParam excludeDatanodes,
      final FsActionParam fsAction,
      final TokenKindParam tokenKind,
      final TokenServiceParam tokenService
      ) throws IOException, URISyntaxException {
    final Router router = (Router)context.getAttribute("name.node");
    final Configuration conf = (Configuration) context
        .getAttribute(JspHelper.CURRENT_CONF);
    final ClientProtocol np = getRPCServer(router);

    // TODO methods:
    // OPEN
    // GETFILECHECKSUM
    // GETDELEGATIONTOKEN
    switch(op.getValue()) {
    case GET_BLOCK_LOCATIONS:
    {
      final long offsetValue = offset.getValue();
      final Long lengthValue = length.getValue();
      final LocatedBlocks locatedblocks = np.getBlockLocations(fullpath,
          offsetValue, lengthValue != null? lengthValue: Long.MAX_VALUE);
      final String js = JsonUtil.toJsonString(locatedblocks);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILESTATUS:
    {
      final HdfsFileStatus status = np.getFileInfo(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status, true);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS:
    {
      final StreamingOutput streaming = getListingStream(np, fullpath);
      return Response.ok(streaming).type(MediaType.APPLICATION_JSON).build();
    }
    case GETCONTENTSUMMARY:
    {
      final ContentSummary contentsummary = np.getContentSummary(fullpath);
      final String js = JsonUtil.toJsonString(contentsummary);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETHOMEDIRECTORY: {
      final String js = JsonUtil.toJsonString("Path",
          FileSystem.get(conf != null ? conf : new Configuration())
              .getHomeDirectory().toUri().getPath());
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETACLSTATUS: {
      AclStatus status = np.getAclStatus(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETXATTRS: {
      List<String> names = null;
      if (xattrNames != null) {
        names = Lists.newArrayListWithCapacity(xattrNames.size());
        for (XAttrNameParam xattrName : xattrNames) {
          if (xattrName.getXAttrName() != null) {
            names.add(xattrName.getXAttrName());
          }
        }
      }
      List<XAttr> xAttrs = np.getXAttrs(fullpath, (names != null &&
          !names.isEmpty()) ? XAttrHelper.buildXAttrs(names) : null);
      final String js = JsonUtil.toJsonString(xAttrs,
          xattrEncoding.getEncoding());
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTXATTRS: {
      final List<XAttr> xAttrs = np.listXAttrs(fullpath);
      final String js = JsonUtil.toJsonString(xAttrs);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CHECKACCESS: {
      np.checkAccess(fullpath, FsAction.getFsAction(fsAction.getValue()));
      return Response.ok().build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }
}
