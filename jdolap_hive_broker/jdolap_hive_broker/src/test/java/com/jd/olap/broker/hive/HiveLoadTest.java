
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
import junit.framework.TestCase;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.runners.Parameterized.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

/**
 * Test Hive Load with various inputs
 */
@RunWith(Parameterized.class)
public class HiveLoadTest extends TestCase {

  @Parameters
  public static Collection<Object[]> getData() {
    return Arrays.asList(new Object[][]{
            {
                    "sql", "host", "port", "db", "table", "user", "password", "colSeparator", "maxFilterRatio", false // expectSuccess
            },
            {
                    "sql", "host", "port", "db", "table", "user", "password", "colSeparator", "maxFilterRatio", true
            }

    });
  }

  @Parameter(0)
  private String sql;

  @Parameter(1)
  private String host;

  @Parameter(2)
  private String port;

  @Parameter(3)
  private String db;

  @Parameter(4)
  private String table;

  @Parameter(5)
  private String userName;

  @Parameter(6)
  private String password;

  @Parameter(7)
  private String colSeparator;

  @Parameter(8)
  private String maxFilterRatio;

  @Parameter(9)
  private boolean expectedSuccess;

  @Test
  public void doTest() throws IOException {
    boolean succeeded = false;
    final String loadUrl = String.format("http://%s:%s/api/%s/%s/_hive_load",
            host,
            port,
            db,
            table);
    System.out.print(loadUrl);
    final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
              @Override
              protected boolean isRedirectable(String method) {
                return true;
              }
            });

    try (CloseableHttpClient client = httpClientBuilder.build()) {
      HttpPost post = new HttpPost(loadUrl);
      post.setHeader(HttpHeaders.EXPECT, "100-continue");
      post.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));
      if (colSeparator != null) {
        post.setHeader("columnSeparator", colSeparator);
      }
      post.setHeader("max_filter_ratio", maxFilterRatio);

      try (CloseableHttpResponse response = client.execute(post)) {
        String loadResult = "";
        if (response.getEntity() != null) {
          loadResult = EntityUtils.toString(response.getEntity());
        }
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
          throw new IOException(
                  String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
        }

        succeeded = true;
      } catch (Exception e) {
        // TODO add logger support
        e.printStackTrace(System.out);
        ;
      }

      Assert.assertEquals("Unexpected hive load task result", succeeded, expectedSuccess);
    }
  }

  public static String basicAuthHeader(String user, String password) {
    final String tobeEncode = user + ":" + password;
    byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
    return "Basic " + new String(encoded);
  }
}
