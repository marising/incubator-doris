// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.http.rest;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class StreamLoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(StreamLoadAction.class);

    private ExecuteEnv execEnv;

    public StreamLoadAction(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv execEnv = ExecuteEnv.getInstance();
        StreamLoadAction action = new StreamLoadAction(controller, execEnv);
        controller.registerHandler(HttpMethod.PUT,
                "/api/{" + LoadAction.DB_KEY + "}/{" + LoadAction.TABLE_KEY + "}/_stream_load",
                new StreamLoadAction(controller, execEnv));
    }

    @Override
    public void executeWithoutPassword(ActionAuthorizationInfo authInfo,
                                       BaseRequest request, BaseResponse response) throws DdlException {

        // A 'StreamLoad' request must have 100-continue header
        if (!request.getRequest().headers().contains(HttpHeaders.Names.EXPECT)) {
            throw new DdlException("There is no 100-continue header");
        }

        final String clusterName = authInfo.cluster;
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new DdlException("No cluster selected.");
        }

        String dbName = request.getSingleParameter(LoadAction.DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(LoadAction.TABLE_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No table selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, dbName);

        String label = request.getRequest().headers().get(LoadAction.LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            LOG.error("You MUST specify a label name for a hive load request");
            throw new DdlException("No label specified.");
        }

        // check auth
        checkTblAuth(authInfo, fullDbName, tableName, PrivPredicate.LOAD);

        // Choose a backend sequentially.
        List<Long> backendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(1, true, false, clusterName);
        if (backendIds == null) {
            throw new DdlException("No backend alive.");
        }

        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new DdlException("No backend alive.");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());

        LOG.info("redirect stream load action to destination={}, db: {}, tbl: {}, label: {}",
                redirectAddr.toString(), dbName, tableName, label);
        redirectTo(request, response, redirectAddr);
    }
}
