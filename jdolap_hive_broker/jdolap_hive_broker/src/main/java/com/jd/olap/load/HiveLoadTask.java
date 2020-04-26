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

package com.jd.olap.load;

import java.util.HashMap;

import com.jd.olap.thrift.TJDOLapLoadRequest;
import com.jd.olap.thrift.TJDOlapLoadStatus;
import com.jd.olap.thrift.TJDOlapOperationStatusCode;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import static com.jd.olap.broker.hive.BrokerConfig.*;
import static com.jd.olap.broker.hive.BrokerConfig.spark_app_jars;
import static com.jd.olap.thrift.TJDOlapOperationStatusCode.*;
import static org.apache.spark.launcher.SparkLauncher.*;

public class HiveLoadTask implements Runnable {
    private static Logger logger = Logger.getLogger(HiveLoadTask.class);

    public static TJDOLapLoadRequest request;
    private TJDOlapLoadStatus status;
    private SparkAppHandle sparkAppHandle;

    public HiveLoadTask(TJDOLapLoadRequest request, TJDOlapLoadStatus status) {
        this.request = request;
        this.status = status;
    }

    public synchronized TJDOlapLoadStatus getStatus() {
        return status;
    }

    @Override
    public void run() {
        submitLoadJob(request, status);
    }

    public synchronized void submitLoadJob(TJDOLapLoadRequest request, TJDOlapLoadStatus status) {
        if (!isPending()) {
            // Probably cancelled before scheduled
            logger.warn("Skip job: " + request.getHiveLoadLabel() + ", status: " + status.getOpStatus().toString());
            return;
        }
        logger.info("SPARK_CONF_DIR: " + "/software/conf/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/" + request.getSparkQueue() + "/spark_conf");
        HashMap<String, String> envParams = new HashMap<>();
        envParams.put("JDHXXXXX_CLUSTER_NAME", request.getSparkMarket());
        envParams.put("JDHXXXXX_USER", request.getSparkProduction());
        envParams.put("JDHXXXXX_QUEUE", request.getSparkQueue());
        envParams.put("HADOOP_USERNAME", request.getSparkProduction());
        envParams.put("BEE_USER", request.getBeeUser());
        envParams.put("USER", request.getUser());
        envParams.put("HADOOP_USER_CERTIFICATE", request.getHadoopUserCertificate());
        envParams.put("SPARK_HOME", spark_home + "/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/spark");
        envParams.put("HADOOP_HOME", spark_home + "/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/hadoop");
        envParams.put("HIVE_HOME", spark_home + "/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/hive");
        envParams.put("HIVE_CONF_DIR", "/software/conf/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/" + request.getSparkQueue() + "/hive_conf");
        envParams.put("SPARK_CONF_DIR", "/software/conf/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/" + request.getSparkQueue() + "/spark_conf");
        envParams.put("HADOOP_CONF_DIR", "/software/conf/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/" + request.getSparkQueue() + "/hadoop_conf");
        SparkLauncher sparkLauncher = new SparkLauncher(envParams);
        try {
            sparkAppHandle = sparkLauncher.setMaster(spark_mode)
                    .setJavaHome(java_home)
                    .setSparkHome(spark_home + "/" + request.getSparkMarket() + "/" + request.getSparkProduction() + "/spark")
                    .setDeployMode("client")
                    .setConf(EXECUTOR_MEMORY, spark_executor_memory)
                    .setConf(EXECUTOR_CORES, spark_executor_cores)  //cpu
                    .setConf(DRIVER_MEMORY, spark_driver_memory)  //driver collect(rdd)
                    .setConf("spark.driver.maxResultSize", spark_driver_maxResultSize)
                    .setConf("spark.kryoserializer.buffer", spark_kryoserializer_buffer)
                    .setConf("spark.kryoserializer.buffer.max", spark_kryoserializer_buffer_max)
                    .setConf("spark.shuffle.service.enabled", spark_shuffle_service_enabled)
                    .setConf("spark.sql.shuffle.partitions", spark_sql_shuffle_partitions)
                    .setConf("spark.dynamicAllocation.enabled", spark_dynamicAllocation_enabled)
                    .setConf("spark.dynamicAllocation.minExecutors", spark_dynamicAllocation_minExecutors)
                    .setConf("spark.dynamicAllocation.maxExecutors", spark_dynamicAllocation_maxExecutor)
                    .setVerbose(true)
                    .setAppResource(spark_app_jars)
                    .setMainClass(hive_query_main_class)
                    .addAppArgs(request.getSql(),
                            request.getTargetHost(),
                            request.getTargetPort(),
                            request.getTargetDB(),
                            request.getTargetTable(),
                            request.getOlapUser(),
                            request.getOlapPasswd(),
                            request.getHiveLoadLabel(),
                            request.getColumnSeparator(),
                            request.getMaxFilterRatio(),
                            hive_load_sample_sizek)
                    .startApplication(new SparkAppHandle.Listener() {
                        @Override
                        public void stateChanged(SparkAppHandle sparkAppHandle) {
                            SparkAppHandle.State state = sparkAppHandle.getState();
                            logger.info("Spark App Id [" + sparkAppHandle.getAppId() + "] Info Changed.  State [" + state + "]");
                            updateStatus(toStatusCode(state), sparkAppHandle.getAppId(), null, null);
                        }

                        @Override
                        public void infoChanged(SparkAppHandle sparkAppHandle) {
                            //TODO what is it? Currently do nothing
                        }
                    });
            logger.info("The task is executing, please wait ....");
        } catch (Throwable e) { // Avoid runtime failure to guarantee serviceability in both client and server side.
            logger.error("Failed to start spark application!", e);
            status.setOpStatus(FAILURE_START_SPARK_APP);
            status.setMessage(e.getMessage());
            status.setPayload(ExceptionUtils.getFullStackTrace(e));
            return;
        }
        updateStatus(TJDOlapOperationStatusCode.SUBMITTED_RUNNABLE, sparkAppHandle.getAppId(), "Submitted Hive Load job successfully", null);
    }

    /**
     * Check if job is terminated
     *
     * @return
     */
    public boolean isTerminated() {
        // TODO Double check state to avoid missing event? Not sure if spurious disconnect will emit event correctly.
        TJDOlapOperationStatusCode statusCode = this.getStatus().getOpStatus();
        return FINISHED_SUCCESSFULLY.equals(statusCode)
                || FAILURE_START_SPARK_APP.equals(statusCode)
                || FAILURE_USER_ABORTED.equals(statusCode)
                || FAILURE_OTHER.equals(statusCode)
                || FAILURE_USER_CANCELLED_BEFORE_START.equals(statusCode);
    }

    /**
     * Check whether the job is pending for execution
     *
     * @return
     */
    public boolean isPending() {
        return this.getStatus().getOpStatus().equals(SUBMITTED_PENDING);
    }

    /**
     * Check whether the job is pending for execution
     *
     * @return
     */
    public boolean isCancelled() {
        TJDOlapOperationStatusCode status = this.getStatus().getOpStatus();
        return status.equals(FAILURE_USER_CANCELLED_BEFORE_START) || status.equals(FAILURE_USER_ABORTED);
    }

    /**
     * Try to cancel if the task is not terminated
     */
    public synchronized TJDOlapLoadStatus cancel() {
        if (isPending()) {
            updateStatus(FAILURE_USER_CANCELLED_BEFORE_START, null, "Successfully cancelled the pending job", null);
        } else if (!isTerminated()) {
            SparkAppHandle.State state = sparkAppHandle.getState();
            try {
                if (!state.isFinal()) {
                    sparkAppHandle.stop();
                }
            } catch (Exception e) {
                // Only log the exception
                logger.error("Failed to stop spark app" + status.getSparkAppId(), e);
            } finally {
                // Double check its state
                state = sparkAppHandle.getState();
                if (!state.isFinal()) {
                    sparkAppHandle.kill();
                }
            }
            updateStatus(FAILURE_USER_ABORTED, null, "Aborted running job", null);
        }
        return status;
    }

    /**
     * Update status information atomically
     *
     * @param statusCode
     * @param message
     * @param payload
     */
    public synchronized void updateStatus(TJDOlapOperationStatusCode statusCode, String appId, String message, String payload) {
        status.setOpStatus(statusCode);
        if (appId != null && appId.length() > 0) {
            status.setSparkAppId(appId);
        }
        status.setMessage(message);
        status.setPayload(payload);
    }

    private static TJDOlapOperationStatusCode toStatusCode(SparkAppHandle.State state) {
        TJDOlapOperationStatusCode statusCode;
        if (SparkAppHandle.State.FINISHED.equals(state)) {
            statusCode = FINISHED_SUCCESSFULLY;
        } else if (SparkAppHandle.State.FAILED.equals(state)
                || SparkAppHandle.State.LOST.equals(state)) {
            statusCode = FAILURE_OTHER;
        } else if (SparkAppHandle.State.KILLED.equals(state)) {
            statusCode = FAILURE_USER_ABORTED;
        } else {
            statusCode = SUBMITTED_RUNNABLE;
        }

        return statusCode;
    }
}

