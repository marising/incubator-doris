
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

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static java.lang.System.getProperty;
import static com.jd.olap.load.HiveLoadTask.*;

/**
 * a basic JDOlap stream load task implementation
 */
public class StreamLoader {

    private static Logger logger = Logger.getLogger(StreamLoader.class);


    public static final String DEFAULT_MAX_FILTER_RATIO = "0.2";
    public static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private String sql;
    private String host;
    private String port;
    private String db;
    private String table;
    private String userName;
    private String password;
    private String label;
    private String columnSeparator;
    private String maxFilterRatio;

    /**
     * Prepare for stream load
     */
    public StreamLoader(String sql, String host, String port, String db, String table, String userName, String password,
                        String label, String columnSeparator, String maxFilterRatio) {
        this.sql = sql;
        this.host = host;
        this.port = port;
        this.db = db;
        this.table = table;
        this.userName = userName;
        this.password = password;
        this.label = label;
        this.columnSeparator = (columnSeparator == null || columnSeparator.length() == 0) ? DEFAULT_COLUMN_SEPARATOR : columnSeparator;
        this.maxFilterRatio = (maxFilterRatio == null || maxFilterRatio.length() == 0) ? DEFAULT_MAX_FILTER_RATIO : maxFilterRatio;

        //fix error hard code
        logger.info("User input, sql [" + sql + "]\n\t host [" + host + "]\n\t port ["
                + port + "]\n\t db [" + db + "]\n\t table [" + table + "]\n\t user ["
                + userName + "]\n\t label [" + label + "]\n\t column separator ["
                + columnSeparator + "]\n\t max filter ratio [" + maxFilterRatio + "]");
    }

    /**
     * Query and load in stream mode
     */
    public void load() {
        logger.info("start Spark builder");
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .appName("StreamLoader")
                .getOrCreate();
        logger.info("start spark sql to Iterator");
        Iterator<Row> it = spark.sql(sql).toLocalIterator();
        sendData(new RowAsInputStream(it, "\t"));
        spark.stop();
    }


    /**
     * Send plain string
     *
     * @param content
     * @return
     * @throws Exception
     */
    public boolean sendData(String content) throws Exception {
        StringEntity entity = new StringEntity(content);
        return sendData(entity);
    }

    /**
     * Send stream data
     *
     * @param inputStream
     * @return
     * @throws Exception
     */
    public boolean sendData(InputStream inputStream) {
        HttpEntity entity = EntityBuilder.create()
                .setStream(inputStream)
                .chunked()
                .build();
        return sendData(entity);
    }

    /**
     * Send data
     *
     * @param content
     * @return
     * @throws Exception
     */
    private boolean sendData(HttpEntity content) {
        boolean resultFlag = false;

        logger.info("Start send data to backend ");
        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                host,
                port,
                db,
                table);
        System.out.print("Stream load URL: " + loadUrl);
        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        logger.info("User " + userName + "\r\n"
                + " Parmater" + columnSeparator + "\r\n"
                + " MaxFilterRatio: " + maxFilterRatio + "\r\n"
                + " Label" + label);
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));
            put.setHeader("max_filter_ratio", maxFilterRatio);
            put.setHeader("label", label);
            put.setEntity(content);


            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }

                System.out.print(loadResult);
            } catch (Exception e) {
                // TODO add logger support
                e.printStackTrace(System.out);
                return resultFlag;
            }
            resultFlag = true;
        } catch (Exception e) {
            e.printStackTrace(System.out);
            return resultFlag;
        } finally {
            return resultFlag;
        }
    }

    public static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .appName("StreamLoader")
                .getOrCreate();
        Iterator<Row> it = spark.sql(args[0]).toLocalIterator();
        try {
            StreamLoader loader = new StreamLoader(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9]);
            System.out.print("Start streamloader load");
            loader.sendData(new RowAsInputStream(it));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
