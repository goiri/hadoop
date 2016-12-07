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
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/**
 * Determines the remote client protocol method and the parameter list for a
 * specific location.
 */
public class RemoteMethod {

  private static final Log LOG = LogFactory.getLog(RemoteMethod.class);

  /** List of parameters: static and dynamic values, matchings types. */
  private Object[] params;
  /** List of method parameters types, matches params. */
  private Class<?>[] types;
  /** String name of the ClientProtocol method. */
  private String methodName;

  /**
   * Create a method with no parameters.
   *
   * @param method The string name of the ClientProtocol method.
   */
  public RemoteMethod(String method) {
    this.params = null;
    this.types = null;
    this.methodName = method;
  }

  /**
   * Creates a remote method generator.
   *
   * @param method The string name of the ClientProtocol method.
   * @param pTypes A list of types to use to locate the specific method.
   * @param pParams A list of parameters for the method. The order of the
   *          parameter list must match the order and number of the types.
   *          Parameters are grouped into 2 categories:
   *          <ul>
   *          <li>Static params that are immutable accross locations.
   *          <li>Dynamic params that are detemrined for each location by a
   *          RemoteParam object. To specify a dynamic parameter, pass an
   *          instance of RemoteParam in place of the parameter value.
   *          </ul>
   * @throws IOException If the types and parameter lists are not valid.
   */
  public RemoteMethod(String method, Class<?>[] pTypes, Object... pParams)
      throws IOException {

    if (pParams.length != pTypes.length) {
      throw new IOException("Invalid parameters for method " + method);
    }
    if (LOG.isDebugEnabled()) {
      // Validate params
      for (int i = 0; i < pTypes.length; i++) {
        Object obj = pParams[i];
        Class<?> clz = pTypes[i];

        if (obj != null && !clz.isInstance(obj)) {
          throw new IOException("Parameter " + obj.getClass()
              + " does not match the requested type " + clz);
        }
      }
    }
    this.params = pParams;
    this.types = pTypes;
    this.methodName = method;
  }

  /**
   * Get the represented java method.
   *
   * @return Method
   * @throws IOException If the method cannot be found.
   */
  public Method getMethod() throws IOException {
    try {
      if (types != null) {
        return ClientProtocol.class.getDeclaredMethod(methodName, types);
      } else {
        return ClientProtocol.class.getDeclaredMethod(methodName);
      }
    } catch (Exception ex) {
      // Re-throw as an IOException
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Get the calling types for this method.
   *
   * @return An array of calling types.
   */
  public Class<?>[] getTypes() {
    return this.types;
  }

  /**
   * Generate a list of parameters for this specific location using no context.
   *
   * @return A list of parameters for the method customized for the location.
   */
  public Object[] getParams() {
    return this.getParams(null);
  }

  /**
   * Get the name of the method.
   *
   * @return Name of the method.
   */
  public String getMethodName() {
    return this.methodName;
  }

  /**
   * Generate a list of parameters for this specific location. Parameters are
   * grouped into 2 categories:
   * <ul>
   * <li>Static params that are immutable across locations.
   * <li>Dynamic params that are determined for each location by a RemoteParam
   * object.
   * </ul>
   *
   * @param context The context identifying the location.
   * @return A list of parameters for the method customized for the location.
   */
  public Object[] getParams(RemoteLocationContext context) {
    if (this.params == null) {
      return new Object[] {};
    }
    Object[] objList = new Object[this.params.length];
    for (int i = 0; i < this.params.length; i++) {
      Object currentObj = this.params[i];
      if (currentObj instanceof RemoteParam) {
        // Map the parameter using the context
        RemoteParam paramGetter = (RemoteParam) currentObj;
        objList[i] = paramGetter.getParameterForContext(context);
      } else {
        objList[i] = currentObj;
      }
    }
    return objList;
  }
}
