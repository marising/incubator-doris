package com.jd.olap.load.sample;

import com.jd.olap.load.StreamLoader;
import com.jd.olap.load.RowAsInputStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.util.Iterator;

import static com.jd.olap.broker.hive.BrokerConfig.*;


/**
 * Load dummy data into a test DB/table, which has only two columns.
 */
public class StreamLoadSample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .appName("StreamLoadSample")
                .getOrCreate();
        String tmpTable = "tmp";

        System.out.print("Hive load size " + Long.parseLong(args[10]) * 10240L + "K");
        Dataset data = spark.range(1, Long.parseLong(args[10]) * 64L, 1, 10).selectExpr("id",
                "id * 1  as c1", "id * 2  as c2", "id * 3  as c3", "id * 4  as c4", "id * 5  as c5",
                "id * 6  as c6", "id * 7  as c7", "id * 8  as c8", "id * 9  as c9", "id * 10  as c10",
                "id * 11  as c11", "id * 12  as c12", "id * 13  as c13", "id * 14  as c14", "id * 15  as c15",
                "id * 16  as c16", "id * 17  as c17", "id * 17  as c18", "id * 19  as c19", "id * 20  as c20");
        data.createOrReplaceTempView(tmpTable);
        Iterator<Row> it = spark.sql("select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20 from " + tmpTable).toLocalIterator();
        try {
            StreamLoader loader = new StreamLoader(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9]);
            System.out.print("Start streamloader load");
            loader.sendData(new RowAsInputStream(it));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
