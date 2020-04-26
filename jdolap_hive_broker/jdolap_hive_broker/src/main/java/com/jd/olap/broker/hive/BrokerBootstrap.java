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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import com.jd.olap.thrift.TJDOlapLoadService;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;

import com.jd.olap.load.common.ThriftServer;

public class BrokerBootstrap {
    private static Logger logger = Logger.getLogger(BrokerBootstrap.class);
    public static final String HIVE_BROKER_HOME = "HIVE_BROKER_HOME";
    public static final String HIVE_BROKER_LOG_DIR = "HIVE_BROKER_LOG_DIR";
    public static void main(String[] args) {
        try {
            final String brokerHome = System.getenv(HIVE_BROKER_HOME);
            if (brokerHome == null || StringUtils.isEmpty(brokerHome)) {
                System.out.println("HIVE_BROKER_HOME is not set, exit");
                return;
            }
            System.setProperty("HIVE_BROKER_LOG_DIR", System.getenv("HIVE_BROKER_LOG_DIR"));
            PropertyConfigurator.configure(brokerHome + "/conf/log4j.properties");
            logger.info("starting JDOLAP Hive Broker....");
            new BrokerConfig().init(brokerHome + "/conf/jdolap_hive_broker.conf");

            TProcessor tProcessor = new TJDOlapLoadService.Processor<TJDOlapLoadService.Iface>(new HiveBrokerServiceImpl());
            ThriftServer server = new ThriftServer(BrokerConfig.broker_ipc_port, tProcessor);
            server.start();
            logger.info("JDOLAP Hive Broker launched");
            while (true) {
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            logger.error("Unexpected failure during launching Hive broker", e);
            System.exit(-1);
        }
    }

    private static boolean createAndLockPidFile(String pidFilePath)
            throws IOException {
        File pid = new File(pidFilePath);

        try (RandomAccessFile file = new RandomAccessFile(pid, "rws");){
            FileLock lock = file.getChannel().tryLock();
            if (lock == null) {
                return false;
            }

            // if system exit abnormally, file will not be deleted
            pid.deleteOnExit();

            String name = ManagementFactory.getRuntimeMXBean().getName();
            file.write(name.split("@")[0].getBytes(Charsets.UTF_8));
            return true;
        } catch (OverlappingFileLockException e) {
            logger.warn("Unexpected pid file overlapping exception", e);
            return false;
        } catch (IOException e) {
            throw e;
        }
    }
}
