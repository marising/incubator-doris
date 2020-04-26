package com.jd.olap.broker.hive;

import com.jd.olap.load.RowAsInputStream;
import com.jd.olap.load.StreamLoader;
import com.jd.olap.load.sample.SampleConstants;
import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.time.Instant;
import java.util.Iterator;

public class RowAsStreamTest extends TestCase {

    @Test
    public void stringDataTest() throws Exception {
        int id1 = 1;
        int id2 = 10;
        int rowNumber = 10;
        String oneRow = id1 + "\t" + id2 + "\n";

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < rowNumber; i++) {
            stringBuilder.append(oneRow);
        }

        //in doris 0.9 version, you need to comment this line
        //refer to https://github.com/apache/incubator-doris/issues/783
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);

        String loadData = stringBuilder.toString();
        StreamLoader streamLoader = new StreamLoader(
                "sql",
                SampleConstants.DORIS_HOST,
                SampleConstants.DORIS_HTTP_PORT,
                SampleConstants.DORIS_DB,
                SampleConstants.DORIS_TABLE,
                SampleConstants.DORIS_USER,
                SampleConstants.DORIS_PASSWORD,
                "xxlabelname",
                null,
                null);
        streamLoader.sendData(loadData);
    }

    @Test
    public void rowAsStreamTest() {
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .appName("StreamLoadTestApp")
                .getOrCreate();
        String tmpTable = "tmp";
        Dataset data = spark.range(1, 1024*1024*128).selectExpr("id", "id * 10  as c1", "id * 100  as c2", "id * 1000  as c3");
        data.createOrReplaceTempView(tmpTable);
        Iterator<Row> it = spark.sql("select c1,c2 from " + tmpTable).toLocalIterator();
        try {
            StreamLoader streamLoader = new StreamLoader(
                    "sql",
                    SampleConstants.DORIS_HOST,
                    SampleConstants.DORIS_HTTP_PORT,
                    SampleConstants.DORIS_DB,
                    SampleConstants.DORIS_TABLE,
                    SampleConstants.DORIS_USER,
                    SampleConstants.DORIS_PASSWORD,
                    "streamloadTestLabel" + Instant.now().toString(),
                    null,
                    null);
            streamLoader.sendData(new RowAsInputStream(it));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
