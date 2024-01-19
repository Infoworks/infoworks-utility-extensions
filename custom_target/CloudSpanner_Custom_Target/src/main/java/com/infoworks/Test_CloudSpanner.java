package com.infoworks;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class Test_CloudSpanner {
    public Test_CloudSpanner() {
    }

    public static void main(String[] args) {
        CloudSpannerCustomTarget cstmtgt = new CloudSpannerCustomTarget();
        Map<String, String> userProps = new HashMap<>();
        userProps.put("projectId", "gcp-cs-shared-resources");
        userProps.put("instanceId", "google-spanner-poc");
        userProps.put("database", "finance-db");
        userProps.put("table", "CSV_DEMO_DATATYPES_TESTING_2");
        userProps.put("credentialPath", "");
        userProps.put("syncMode", "overwrite");
        userProps.put("primary_keys", "id");
        userProps.put("indexColumns", "string_col");
        userProps.put("batchSize", "100");
        UserProperties userProperties = new UserProperties(userProps);
        SparkSession sparkSession = getSparkSession();
        Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("./GitHub/InfoworksCustomTargets/data/sample_data_full_load.csv");
        dataset = dataset.withColumn("timestamp_col", functions.to_timestamp(functions.col("timestamp_col"),"yyyy-MM-dd HH:mm:ss"));
        dataset = dataset.withColumn("date_col",functions.to_date(functions.col("date_col"),"yyyy-MM-dd"));
        dataset = dataset.withColumn("decimal_col",functions.col("decimal_col").cast(DataTypes.createDecimalType(38,18)));
        dataset = dataset.withColumn("long_col",functions.col("long_col").cast(DataTypes.LongType));
        dataset = dataset.withColumn("float_col",functions.col("float_col").cast(DataTypes.FloatType));
        dataset = dataset.withColumn("double_col",functions.col("double_col").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn("boolean_col",functions.col("boolean_col").cast(DataTypes.BooleanType));

//        dataset = dataset.withColumn("insert_ts", functions.to_timestamp(functions.col("insert_ts"),"yyyy-MM-dd HH:mm:ss"));
//        dataset = dataset.withColumn("temp_date",functions.to_date(functions.col("temp_date"),"yyyy-MM-dd"));
//        dataset = dataset.withColumn("salary",functions.col("salary").cast(DataTypes.createDecimalType(20,7)));
//        dataset = dataset.withColumn("isActive",functions.col("isActive").cast(DataTypes.BooleanType));
//        dataset = dataset.withColumn("float_col",functions.col("float_col").cast(DataTypes.FloatType));
//        dataset = dataset.withColumn("long_col",functions.col("id").cast(DataTypes.LongType));
//        dataset = dataset.withColumn("short_col",functions.col("id").cast(DataTypes.ShortType));
        dataset.show();
        cstmtgt.initialiseContext(getSparkSession(), userProperties, (ProcessingContext)null);
        cstmtgt.writeToTarget(dataset);

    }

    private static SparkSession getSparkSession() {
        return SparkSession.builder().config("spark.master", "local").getOrCreate();
    }

    private static Dataset getDataset(SparkSession sparkSession, String csvFile) {
        return sparkSession.read().option("header", "true").option("inferschema", "true").csv(csvFile);
    }
}
// https://stackoverflow.com/questions/69698113/jar-file-error-could-not-find-or-load-main-class-with-intellij
// https://dkbalachandar.wordpress.com/2018/01/04/how-to-use-maven-shade-plugin/#:~:text=Maven%20Shade%20Plugin%20provides%20the,create%20one%20big%20JAR(Uber.
