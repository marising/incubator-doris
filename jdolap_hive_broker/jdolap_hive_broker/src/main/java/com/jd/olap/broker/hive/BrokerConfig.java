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

import com.jd.olap.load.common.ConfigBase;


public class BrokerConfig extends ConfigBase {
    @ConfField
    public static int broker_ipc_port = 9031;

    @ConfField
    public static String hive_load_sample_sizek = "1024";

    @ConfField
    public static String java_home = "/software/servers/jdk1.8.0_121";

    @ConfField
    public static String spark_mode = "yarn";

    @ConfField
    public static String spark_home = "/software/servers";

    @ConfField
    public static String spark_app_jars = "/home/mart_bag/songenjie/jdolap/hive/lib/jdolap_hive_broker.jar";

    @ConfField
    public static String hive_query_main_class = "com.jd.olap.load.StreamLoader";

    @ConfField
    public static String spark_executor_memory = "20g";

    @ConfField
    public static String spark_driver_memory = "24g";

    @ConfField
    //must less than spark.driver.memory
    public static String spark_driver_maxResultSize = "20g";

    @ConfField
    public static String spark_executor_cores = "2";

    @ConfField
    public static String spark_kryoserializer_buffer_max = "1900m";

    @ConfField
    public static String spark_kryoserializer_buffer = "64m";

    @ConfField
    public static String spark_shuffle_service_enabled = "true";

    @ConfField
    public static String spark_sql_shuffle_partitions = "500";

    @ConfField
    public static String spark_dynamicAllocation_enabled = "true";

    @ConfField
    public static String spark_dynamicAllocation_minExecutors = "10";

    @ConfField
    public static String spark_dynamicAllocation_maxExecutor = "50";
}
