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

import com.jd.olap.thrift.TJDOLapLoadRequest;
import com.jd.olap.thrift.TJDOlapLoadService;
import com.jd.olap.thrift.TJDOlapLoadStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;


import junit.framework.TestCase;

/**
 * TODO
 */
public class TestHiveBrokerService extends TestCase {
    private TJDOlapLoadService.Client client;
    
    protected void setUp() throws Exception {
        TTransport transport;
        
        transport = new TSocket("host", 9031);
        transport.open();

        TProtocol protocol = new  TBinaryProtocol(transport);
        client = new TJDOlapLoadService.Client(protocol);
    }
    
    @Test
    public void testLaunchJob() throws TException {
        TJDOLapLoadRequest request = new TJDOLapLoadRequest();
        TJDOlapLoadStatus status = client.startHiveLoad(request);
        System.out.println(status);
    }
}
