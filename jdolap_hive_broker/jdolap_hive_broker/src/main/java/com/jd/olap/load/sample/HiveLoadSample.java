package com.jd.olap.load.sample;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * This is a sample app based on doris stream load sample.
 * Load dummy data into a test DB/table, which has only two columns.
 */
public class HiveLoadSample {

    private String sql;
    private String host;
    private String port;
    private String db;
    private String table;
    private String userName;
    private String password;
    private String colSeparator;
    private String maxFilterRatio;

    /**
     * Prepare for stream load
     *
     * @param host
     * @param port
     * @param db
     * @param table
     * @param userName
     * @param password
     * @param maxFilterRatio
     */
    public HiveLoadSample(String sql, String host, String port, String db, String table, String userName, String password, String colSeparator, String maxFilterRatio) {
        this.sql = sql;
        this.host = host;
        this.port = port;
        this.db = db;
        this.table = table;
        this.userName = userName;
        this.password = password;
        this.colSeparator = colSeparator;
        this.maxFilterRatio = maxFilterRatio;
    }

    private boolean doTest() throws Exception {
        boolean resultFlag = false;
        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            URIBuilder uriBuilder = new URIBuilder();
            URI uri = uriBuilder.setScheme("http")
                    .setHost(host)
                    .setPort(Integer.valueOf(port))
                    .setPath("/api/" + db + "/" + table + "/_hive_load")
                    .build();

            System.out.println("Load URL: " + uri.toString());
            HttpPut put = new HttpPut(uri);
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));
            put.setHeader("sql", sql);
            put.setHeader("columnSeparator", colSeparator);
            put.setHeader("maxFilterRatio", maxFilterRatio);
            put.setHeader("label", "hiveLoadLabel" + Instant.now().getEpochSecond());
            put.setHeader("sparkMarket", "10k");
            put.setHeader("sparkProduction", "mart_scr");
            put.setHeader("sparkQueue", "bdp_jmart_bag_union.bdp_jmart_bag_dev");

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Hive load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }
                System.out.println("Hive load submitted!");
                resultFlag = true;
            }
        }

        return resultFlag;
    }

    public static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) throws Exception {
        HiveLoadSample sample = new HiveLoadSample(
                "sql",
                "10.196.108.12",
                "8330",
                "sej_tst",
                "stream_test",
                "root",
                "",
                "\t",
                "0.2"
        );
        sample.doTest();
    }
}
