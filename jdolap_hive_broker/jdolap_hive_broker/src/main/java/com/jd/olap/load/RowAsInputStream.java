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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Read Spark Rows as a plain Java InputStream. It will convert each row values as string with user-specified separation character.
 */
public class RowAsInputStream extends InputStream {
    // Default column separator to format the load data
    public static final String DEFAULT_COLUMN_SEPARATOR = "\t";

    private Iterator<Row> iterator;
    // A batch boundary of handling row string. We will attempt to handle multiple rows in one batch
    // Its value is related with Apache http client buffer size. The magic word '4096' is not a documented behavior
    // but a number grepped from source codes.
    private int batchBufferSize = 4096 * 16;
    private int position;
    private int limit = batchBufferSize;
    private String separator;
    private ByteArrayInputStream stream;
    private static final char LINE_BREAK = '\n';

    /**
     * Construct InputStream with default separation character '\t'
     *
     * @param it
     */
    public RowAsInputStream(Iterator<Row> it) {
        this(it, DEFAULT_COLUMN_SEPARATOR);
    }

    /**
     * Construct InputStream with specified separation character '\t'
     *
     * @param it
     */
    public RowAsInputStream(Iterator<Row> it, String separator) {
        this.iterator = it;
        this.separator = separator;
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Read next byte is not supported by RowAsInputStream");
    }

    /**
     * Currently, only implement such a read method. We will convert rows in to formatted strings, and then read as a typical InputStream.
     *
     * @param b
     * @return
     * @throws IOException
     */
    @Override
    public int read(byte[] b) throws IOException {
        int readLen = -1;
        if (stream != null) {
            readLen = stream.read(b);
        }

        if (readLen == -1) {
            int nextBatch = fetchNextBatch(iterator);
            // if -1, then no more data
            if (nextBatch == -1) {
                return -1;
            }

            readLen = stream.read(b);
        }

        position += readLen;
        return readLen;
    }

    /**
     * Fetch next batch of rows to read
     *
     * @param iterator
     * @return
     */
    private int fetchNextBatch(Iterator<Row> iterator) {
        if (!iterator.hasNext()) {
            return -1;
        }

        StringBuilder sb = null;
        int remainCapacity = batchBufferSize;
        int rowLength = 0;
        sb = new StringBuilder(batchBufferSize);
        while (iterator.hasNext() && remainCapacity >= rowLength) {
            // Converts rows into a batch cache
            Row row = iterator.next();
            String rowStr = row.mkString(separator);
            // Probably row length is not a fixed number. Just pick one for rough usage currently.
            // TODO: Consider whether to enlarge batchSize, if each row is bigger than half of the current size.
            rowLength = rowStr.length() + 1;
            sb.append(rowStr).append(LINE_BREAK);
            remainCapacity -= rowLength;
        }

        byte[] rowsData = sb.toString().getBytes(StandardCharsets.UTF_8);
        stream = new ByteArrayInputStream(rowsData);
        position = 0;
        limit = rowsData.length;
        return limit;
    }

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .appName("StreamLoadTestApp")
                .getOrCreate();
        String tmpTable = "tmp";
        Dataset data = spark.range(1, 10000).selectExpr("id", "id * 10  as c1", "id * 100  as c2", "id * 1000  as c3");
        data.createOrReplaceTempView(tmpTable);
        Iterator<Row> it = spark.sql("select id,c1,c2,c3 from " + tmpTable).toLocalIterator();
        RowAsInputStream stream = new RowAsInputStream(it);
        byte[] buffer = new byte[4096];
        int readLen = stream.read(buffer);
        while (readLen != -1) {
            System.out.println(new String(buffer));
            readLen = stream.read(buffer);
        }
    }

}
