package com.infoworks;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Test_DynamoDB {
    static Logger LOGGER = Logger.getLogger(Test_DynamoDB.class);
    public Test_DynamoDB() {
    }

    public static void main(String[] args) throws Exception {
        //PropertyConfigurator.configure("src/log4j.properties");
        DynamoDBCustomTarget cstmtgt = new DynamoDBCustomTarget();
        Map<String, String> userProps = new HashMap();
        userProps.put("ddb_table_name", "testcases_demo");
        //userProps.put("endpoint", "http://localhost:8000");
        userProps.put("endpoint", "https://dynamodb.us-east-1.amazonaws.com");
        userProps.put("region", "us-east-1");
        //userProps.put("key_schema", "customer_id:HASH:N");
        userProps.put("key_schema", "customer:HASH:S,salary:RANGE:N");
        userProps.put("mode", "overwrite");
        userProps.put("table_exists", "false");
        UserProperties userProperties = new UserProperties(userProps);
        SparkSession sparkSession = getSparkSession();
        //Dataset dataset = getDataset(sparkSession, "/Users/infoworks/Downloads/GitHub/mongodb_target/data/customer.csv");
        //Dataset dataset = getDataset(sparkSession, "/Users/infoworks/Downloads/GitHub/mongodb_target/data/test.csv");
        Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("./Downloads/vertica.csv");
        // Dataset dataset = getDataset(sparkSession, "/Users/infoworks/Downloads/data_files_ddb/");
        dataset.show();
        System.out.println(dataset.schema());


        cstmtgt.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        cstmtgt.writeToTarget(dataset);
        //cstmtgt.tableBackup("https://dynamodb.us-east-1.amazonaws.com", "us-east-1", "customer");

    }

    private static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.extraJavaOptions", "-Daws.dynamodb.endpoint=http://localhost:8000");
        conf.set("spark.executor.extraJavaOptions", "-Daws.dynamodb.endpoint=http://localhost:8000");
        return SparkSession.builder().master("local[1]").config(conf).getOrCreate();
    }

    private static Dataset getDataset(SparkSession sparkSession, String csvFile) {
        return sparkSession.read().option("header", "true").option("inferschema", "true").csv(csvFile);
        // return sparkSession.read().parquet(csvFile);
    }
}
