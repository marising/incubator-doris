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
import com.jd.olap.thrift.TJDOLapLoadRequest;
import com.jd.olap.thrift.TJDOlapLoadStatus;
import com.jd.olap.thrift.TJDOlapOperationStatusCode;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.StringUtils;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.load.HiveLoadClient;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class HiveLoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(HiveLoadAction.class);

    //
    public static final String SQL_PARAM = "sql";
    public static final String COLUMN_SEPARATOR_PARAM = "columnSeparator";
    public static final String MAX_FILTER_RATIO_RARAM = "maxFilterRatio";
    public static final String SPARK_MARKET = "sparkMarket";
    public static final String SPARK_PRODUCTION = "sparkProduction";
    public static final String SPARK_QUEUE = "sparkQueue";
    public static final String SPARK_BEE_USER = "erp";
    public static final String SPARK_USER = "sparkBusiness_line";
    public static final String SPARK_HEX = "hex";
    public static final String HADOOP_USER_CERTIFICATE = "HADOOP_USER_CERTIFICATE";
    private ExecuteEnv execEnv;

    public HiveLoadAction(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv execEnv = ExecuteEnv.getInstance();
        HiveLoadAction action = new HiveLoadAction(controller, execEnv);
        controller.registerHandler(HttpMethod.PUT,
                "/api/{" + LoadAction.DB_KEY + "}/{" + LoadAction.TABLE_KEY + "}/_hive_load", action);
    }

    public ExecuteEnv getExecEnv() {
        return execEnv;
    }

    @Override
    public void executeWithoutPassword(ActionAuthorizationInfo authInfo,
                                       BaseRequest request, BaseResponse response) throws DdlException {

        final String clusterName = authInfo.cluster;
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.error("You MUST select a cluster for a hive load request");
            throw new DdlException("No cluster selected.");
        }

        final String dbName = request.getSingleParameter(LoadAction.DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            LOG.error("You MUST select a target DB for a hive load request");
            throw new DdlException("No database selected.");
        }

        final String tableName = request.getSingleParameter(LoadAction.TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            LOG.error("You MUST select a target table for a hive load request");
            throw new DdlException("No table selected.");
        }

        final String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, dbName);

        //  As agreed, the end user must specify a unique label name for a hive load job.
        final String label = request.getRequest().headers().get(LoadAction.LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            LOG.error("You MUST specify a label name for a hive load request");
            throw new DdlException("No label specified.");
        }

        //hive sql
        String sql = request.getRequest().headers().get(SQL_PARAM);
        if (Strings.isNullOrEmpty(sql)) {
            LOG.error("You MUST specify a sql for a hive load request");
            throw new DdlException("No sql specified.");
        }
        final String sparkhex = request.getRequest().headers().get(SPARK_HEX);
        if (!Strings.isNullOrEmpty(sparkhex) && sparkhex.equals("1")) {
            try {
                sql = new String(Hex.decodeHex(sql.toCharArray()), "UTF-8");
            } catch (UnsupportedEncodingException | DecoderException e) {
                throw new DdlException("Failed to convert hex string to string.", e);
            }
        }
        final String colSeparator = request.getRequest().headers().get(COLUMN_SEPARATOR_PARAM);
        final String maxFilterRatio = request.getRequest().headers().get(MAX_FILTER_RATIO_RARAM);
        final String sparkMarket = request.getRequest().headers().get(SPARK_MARKET);
        if (Strings.isNullOrEmpty(sparkMarket)) {
            LOG.error("You MUST specify a sparkMarket name for a hive load request");
            throw new DdlException("No sparkMarket specified.");
        }
        final String sparkProduction = request.getRequest().headers().get(SPARK_PRODUCTION);
        if (Strings.isNullOrEmpty(sparkProduction)) {
            LOG.error("You MUST specify a sparkProduction name for a hive load request");
            throw new DdlException("No sparkProduction specified.");
        }
        final String sparkQueue = request.getRequest().headers().get(SPARK_QUEUE);
        if (Strings.isNullOrEmpty(sparkQueue)) {
            LOG.error("You MUST specify a sparkQueue name for a hive load request");
            throw new DdlException("No sparkQueue specified.");
        }
        final String sparkBee = request.getRequest().headers().get(SPARK_BEE_USER);
        if (Strings.isNullOrEmpty(sparkBee)) {
            LOG.error("You MUST specify a sparkBee'erp' name for a hive load request");
            throw new DdlException("No sparkBee'erp' specified.");
        }
        final String sparkuser = request.getRequest().headers().get(SPARK_USER);
        if (Strings.isNullOrEmpty(sparkuser)) {
            LOG.error("You MUST specify a sparkuser  for a hive load request");
            throw new DdlException("No sparkuser specified.");
        }
        final String hadoopUserCertificate = request.getRequest().headers().get(HADOOP_USER_CERTIFICATE);
        if (Strings.isNullOrEmpty(hadoopUserCertificate)) {
            LOG.error("You MUST specify a HADOOP_USER_CERTIFICATE  for a hive load request");
            throw new DdlException("No HADOOP_USER_CERTIFICATE specified.");
        }

        // check auth
        //  TODO Support fine-grained privileges for Hive Load, currently pick the load's
        checkTblAuth(authInfo, fullDbName, tableName, PrivPredicate.LOAD);

        // Choose a backend sequentially.
        List<Long> backendIds = Catalog.getCurrentSystemInfo()
                .seqChooseBackendIds(1, true, false, clusterName);
        if (backendIds == null) {
            throw new DdlException("No backend alive.");
        }

        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new DdlException("No backend alive.");
        }

        TJDOLapLoadRequest loadRequest = new TJDOLapLoadRequest();
        loadRequest.setSql(sql);
        loadRequest.setTargetHost(backend.getHost());
        loadRequest.setTargetPort(String.valueOf(backend.getHttpPort()));
        loadRequest.setTargetDB(dbName);
        loadRequest.setTargetTable(tableName);
        loadRequest.setOlapUser(authInfo.fullUserName);
        loadRequest.setOlapPasswd(authInfo.password);
        loadRequest.setHiveLoadLabel(label);
        loadRequest.setColumnSeparator(colSeparator);
        loadRequest.setMaxFilterRatio(maxFilterRatio);
        loadRequest.setSparkMarket(sparkMarket);
        loadRequest.setSparkProduction(sparkProduction);
        loadRequest.setSparkQueue(sparkQueue);
        loadRequest.setBeeUser(sparkBee);
        loadRequest.setUser(sparkuser);
        loadRequest.setHadoopUserCertificate(hadoopUserCertificate);

        LOG.info("received hive load request to sql={}, db: {}, tbl: {}, label: {}",
                sql, dbName, tableName, label);
        TJDOlapLoadStatus status = null;
        try {
            status = HiveLoadClient.startHiveLoad(loadRequest);
        } catch (TException e) {
            LOG.error("Hive load failed!", e);
            if (status != null) {
                LOG.error("Thrift RPC response with status code " + status.getOpStatus() + ", message " + status.getMessage() + ", detail:" + status.getPayload());
            }
            response.appendContent(StringUtils.normalizeForHttp("failed with Exception " + e.getMessage()));
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            throw new DdlException("Hive load failed", e);
        }

        // Check if it's submitted successfully
        if (status != null && !status.getOpStatus().equals(TJDOlapOperationStatusCode.SUBMITTED_PENDING)
                && !status.getOpStatus().equals(TJDOlapOperationStatusCode.SUBMITTED_RUNNABLE)) {
            // TODO DO NOT Expose internal information, except experimental situation
            LOG.error("Load job failed with RC " + status.getOpStatus() + ", message " + status.getMessage() + ", detail:" + status.getPayload());
            response.appendContent(StringUtils.normalizeForHttp("failed with RC " + status.getOpStatus() + ", message " + status.getMessage() + ", detail:" + status.getPayload()));
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        //fix INFO of failed Job
        LOG.info("Load job with RC " + status.getOpStatus() + ", message " + status.getMessage() + ", detail:" + status.getPayload());
        //Return enough information for management operation
        response.appendContent("{hive_load_label:" + status.getHiveLoadLabel() +
                ",brokerIP:" + Config.hive_broker_host +
                ",brokerPort:" + Config.hive_broker_port +
                ",sparkJobId:" + status.getSparkAppId() +
                ",streamLoadLabel:" + status.getStreamloadJobLabel() +
                ",message:" + status.getMessage() + "}");
        writeResponse(request, response, HttpResponseStatus.OK);
    }
}

