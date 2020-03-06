package io.radanalytics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.DataSet;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkPiProducer implements Serializable {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(SparkPiProducer.class);

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "E7MlnA47t8jrBskm";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://mysql:3306/payment";

    // private static final JavaSparkContext sc =
    //         new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

    // private static final SQLContext sqlContext = new SQLContext(sc);
    
    private static final SparkSession sparkSession =
            SparkSession.builder().master("local[*]").appName("Spark2JdbcDs").getOrCreate();
    
    public String GetPi(int scale) {
        JavaSparkContext jsc = SparkContextProvider.getContext();

/*        int n = 100000 * scale;
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

*/
        //Data source options
        Map<String, String> options = new HashMap<>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("user", MYSQL_USERNAME);
        options.put("password", MYSQL_PWD);
        options.put("dbtable",
                    "(select transaction_type, concat_ws(' ', customer_first_name, customer_last_name) as full_name from payment_event) as payment_event");
        //options.put("partitionColumn", "emp_no");
        //options.put("lowerBound", "100");
        //options.put("upperBound", "499");
        //options.put("numPartitions", "10");
        
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

         //SQLContext sqlContext = new SQLContext(jsc);

        //Load MySQL query result as DataFrame
        //DataFrame jdbcDF = sqlContext.load("jdbc", options);
        //Dataset<Row> jdbcRows = sparkSession.read().format("jdbc").options(options).load();
        Dataset<Row> jdbcDF = null;
        
        try {
			Class.forName("com.mysql.jdbc.Driver");
			final String dbTable =
	                "(select transaction_type, concat_ws(' ', customer_first_name, customer_last_name) as full_name from payment_event) as payment_event";
	       jdbcDF = sparkSession.read().jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
                
        List<Row> transactionRows = jdbcDF.collectAsList();
        //List<Row> transactionRows = jdbcRows.collectAsList();

        for (Row transactionRow : transactionRows) {
            LOGGER.info(transactionRow);
        }


        String ret = "Pi is rouuuughly " + 4.0;

        if(transactionRows != null){
            ret = "Size: " + transactionRows.size();
        }

        return ret;
    }
}
