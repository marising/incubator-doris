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

package org.apache.doris.load;

import com.jd.olap.thrift.TJDOLapLoadRequest;
import com.jd.olap.thrift.TJDOlapLoadService;
import com.jd.olap.thrift.TJDOlapLoadStatus;
import org.apache.doris.common.Config;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * A hive load client sample to provide basic interaction with a single hive load broker.
 */
public class HiveLoadClient {

    /**
     * @param request
     * @return
     */
    public static TJDOlapLoadStatus startHiveLoad(TJDOLapLoadRequest request) throws TException {
        TJDOlapLoadStatus status;
        try (TTransport transport = new TSocket(Config.hive_broker_host, Config.hive_broker_port);) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            TJDOlapLoadService.Client client = new TJDOlapLoadService.Client(protocol);
            status = client.startHiveLoad(request);
        }

        return status;
    }

    /**
     * Get recent status of a specific hive load job
     *
     * @param hiveLoadLabel
     * @return
     * @throws TException
     */
    public static TJDOlapLoadStatus getStatus(String hiveLoadLabel) throws TException {
        TJDOlapLoadStatus status;
        try (TTransport transport = new TSocket(Config.hive_broker_host, Config.hive_broker_port);) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            TJDOlapLoadService.Client client = new TJDOlapLoadService.Client(protocol);
            status = client.getStatus(hiveLoadLabel);
        }

        return status;
    }

    /**
     * Cancel a hive load job. If it has terminated, then just return its final status.
     *
     * @param hiveLoadLabel
     * @return
     * @throws TException
     */
    public static TJDOlapLoadStatus cancelJob(String hiveLoadLabel) throws TException {
        TJDOlapLoadStatus status;
        try (TTransport transport = new TSocket(Config.hive_broker_host, Config.hive_broker_port);) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            TJDOlapLoadService.Client client = new TJDOlapLoadService.Client(protocol);
            status = client.cancelHiveLoad(hiveLoadLabel);
        }

        return status;
    }
}
