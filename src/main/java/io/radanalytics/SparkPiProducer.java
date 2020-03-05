package io.radanalytics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPiProducer implements Serializable {

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "expertuser";
    private static final String MYSQL_PWD = "expertuser123";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://mysql:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;


    public String GetPi(int scale) {
        JavaSparkContext jsc = SparkContextProvider.getContext();

        int n = 100000 * scale;
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, scale);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);


        //Data source options
        Map<String, String> options = new HashMap<>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("dbtable",
                    "(select transaction_type, concat_ws(' ', customer_first_name, customer_last_name) as full_name from payment_event) as payment_event");
        //options.put("partitionColumn", "emp_no");
        options.put("lowerBound", "100");
        options.put("upperBound", "499");
        //options.put("numPartitions", "10");

        //Load MySQL query result as DataFrame
        DataFrame jdbcDF = sqlContext.load("jdbc", options);

        List<Row> transactionRows = jdbcDF.collectAsList();

        for (Row transactionRow : transactionRows) {
            LOGGER.info(transactionRow);
        }

        String ret = "Pi is rouuuughly " + 4.0 * count / n;

        return ret;
    }
}
