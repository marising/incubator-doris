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

package com.jd.olap.load.sample;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;

import static com.jd.olap.load.HiveLoadTask.*;

public class SampleConstants {

  public static String DORIS_HOST;
  public static String DORIS_DB;
  public static String DORIS_TABLE;
  public static String DORIS_USER;
  public static String DORIS_PASSWORD;
  public static String DORIS_HTTP_PORT;

  public static String HIVE_SELECT;
  public static int HIVE_RESULT_COLUMN_NUM;

  static {
    try {
      Properties p = new Properties();
      FileInputStream is = new FileInputStream("db.properties");
      p.load(is);
      DORIS_HOST = p.getProperty("DORIS_HOST", "10.196.108.12");
      DORIS_DB = p.getProperty("DORIS_DB", "yxf_tst");
      DORIS_TABLE = p.getProperty("DORIS_TABLE", "stream_test");
      DORIS_USER = p.getProperty("DORIS_USER", "root");
      DORIS_PASSWORD = p.getProperty("DORIS_PASSWORD", "");
      DORIS_HTTP_PORT = p.getProperty("DORIS_HTTP_PORT", "8330");

      HIVE_SELECT = p.getProperty("HIVE_SELECT");
      HIVE_RESULT_COLUMN_NUM = Integer.parseInt(p.getProperty("HIVE_RESULT_COLUMN_NUM"));

      System.out.print("DORIS_HOST:" + DORIS_HOST);
      System.out.print("DORIS_DB:" + DORIS_DB);
      System.out.print("DORIS_TABLE:" + DORIS_TABLE);
      System.out.print("DORIS_USER:" + DORIS_USER);

      System.out.print("DORIS_PASSWORD:" + DORIS_PASSWORD);
      System.out.print("DORIS_HTTP_PORT:" + DORIS_HTTP_PORT);
    } catch (Exception e) {
      System.out.print(e.getMessage());
    }
  }
}
