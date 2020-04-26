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

package com.jd.olap.broker.hive;

import com.jd.olap.load.HiveLoadTask;
import com.jd.olap.thrift.TJDOLapLoadRequest;
import com.jd.olap.thrift.TJDOlapLoadService;
import com.jd.olap.thrift.TJDOlapLoadStatus;
import com.jd.olap.thrift.TJDOlapOperationStatusCode;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class HiveBrokerServiceImpl implements TJDOlapLoadService.Iface {

    private static Logger logger = Logger.getLogger(HiveBrokerServiceImpl.class.getName());

    //TODO cleam historical jobs over a period, such as 1 day?
    private ConcurrentSkipListMap<String, HiveLoadTask> jobHistory = new ConcurrentSkipListMap<>();

    // Currently, only run once at a time
    private Executor executor = Executors.newSingleThreadExecutor();

    /**
     * Construct Hive broker service
     */
    public HiveBrokerServiceImpl() {
    }

    @Override
    public TJDOlapLoadStatus startHiveLoad(TJDOLapLoadRequest request) throws TException {
        logger.info("Receive hive load request: sql [" + request.getSql()
                + "]\n\t host [" + request.getTargetHost()
                + "]\n\t port [" + request.getTargetPort()
                + "]\n\t db [" + request.getTargetDB()
                + "]\n\t table [" + request.getTargetTable()
                + "]\n\t user [" + request.getOlapUser()
                + "]\n\t market [" + request.getSparkMarket()
                + "]\n\t production[" + request.getSparkProduction()
                + "]\n\t queue[" + request.getSparkQueue()
                + "]\n\t label [" + request.getHiveLoadLabel()
                + "]\n\t column separator [" + request.getColumnSeparator()
                + "]\n\t bee user [" + request.getBeeUser()
                + "]\n\t user [" + request.getUser()
                + "]\n\t HADOOP_USER_CERTIFICATE [" + request.getHadoopUserCertificate()
                + "]\n\t max filter ratio [" + request.getMaxFilterRatio() + "]");

        TJDOlapLoadStatus status = new TJDOlapLoadStatus();
        status.setOpStatus(TJDOlapOperationStatusCode.SUBMITTED_PENDING);
        status.setHiveLoadLabel(request.getHiveLoadLabel());
        HiveLoadTask task = new HiveLoadTask(request, status);
        jobHistory.put(request.getHiveLoadLabel(), task);
        executor.execute(task);
        return status;
    }

    @Override
    public synchronized TJDOlapLoadStatus getStatus(String hiveLoadLabel) throws TException {
        logger.info("Receive getStatus request, label:" + hiveLoadLabel);
        return jobHistory.get(hiveLoadLabel).getStatus();
    }

    @Override
    public TJDOlapLoadStatus cancelHiveLoad(String hiveLoadLabel) throws TException {
        logger.info("Receive cancel Hive Load request, label:" + hiveLoadLabel);
        HiveLoadTask task = jobHistory.get(hiveLoadLabel);
        TJDOlapLoadStatus status;
        if (task == null) {
            // TODO exception
            status = null;
        } else {
            status = task.cancel();
        }
        return status;
    }

}

