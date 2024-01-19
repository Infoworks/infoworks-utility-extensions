package com.infoworks;
import static org.junit.Assert.assertEquals;

import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class VerticaTest {

    VerticaCustomTarget customTarget = new VerticaCustomTarget();
    SparkSession sparkSession = getSparkSession();
    Dataset overwrite_dataset = getDataset(sparkSession, "./GitHub/InfoworksCustomTargets/data/sample_data_full_load.csv");
    Dataset append_dataset = getDataset(sparkSession, "./GitHub/InfoworksCustomTargets/data/sample_data_append.csv");
    Dataset merge_dataset = getDataset(sparkSession, "./GitHub/InfoworksCustomTargets/data/sample_data_merge.csv");


    public Map<String, String> setUserProps(String build_mode, String staging_schema, String user_managed_table, String table_name) {
        Map<String, String> userProps = new HashMap();
        userProps.put("jdbc_url", "jdbc:vertica://host_name:5433/vdb");
        userProps.put("user_name", "dbadmin");
        userProps.put("encrypted_password", "");
        userProps.put("schema_name", "public");
        userProps.put("table_name", table_name);
        userProps.put("build_mode", build_mode);
        userProps.put("natural_key", "id");
        userProps.put("spark_write_options", "numPartitions=2;batchsize=10000");
        userProps.put("partition_expression", "YEAR(date_col)");
        if (!Objects.equals(staging_schema, "")){
            userProps.put("staging_schema_name", staging_schema);
        }
        userProps.put("user_managed_table", user_managed_table);
        return userProps;
    }

    @Test
    public void iwxManagedOverwriteWithStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("overwrite", "stage_schema", "false", "iwx_managed_with_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(overwrite_dataset);
    }
    @Test
    public void iwxManagedAppendWithStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("append", "stage_schema", "false", "iwx_managed_with_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(append_dataset);
    }
    @Test
    public void iwxManagedMergeWithStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("merge", "stage_schema", "false", "iwx_managed_with_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        merge_dataset.createOrReplaceTempView("tempTable");
        Dataset deduplicatedDataset = sparkSession.sql(
                "SELECT * " +
                        "FROM ( " +
                        "    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_col) AS rownum " +
                        "    FROM tempTable " +
                        ") ranked " +
                        "WHERE rownum = 1"
        );
        deduplicatedDataset = deduplicatedDataset.drop("rownum");
        customTarget.writeToTarget(deduplicatedDataset);
    }

    @Test
    public void iwxManagedOverwriteWithoutStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("overwrite", "", "false", "iwx_managed_without_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        overwrite_dataset = overwrite_dataset.withColumn("nullColumn", functions.lit(null).cast("long"));
        customTarget.writeToTarget(overwrite_dataset);
    }
    @Test
    public void iwxManagedAppendWithoutStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("append", "", "false", "iwx_managed_without_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);

        customTarget.writeToTarget(append_dataset);
    }
    @Test
    public void iwxManagedMergeWithoutStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("merge", "", "false", "iwx_managed_without_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        merge_dataset.createOrReplaceTempView("tempTable");
        Dataset deduplicatedDataset = sparkSession.sql(
                "SELECT * " +
                        "FROM ( " +
                        "    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_col) AS rownum " +
                        "    FROM tempTable " +
                        ") ranked " +
                        "WHERE rownum = 1"
        );
        deduplicatedDataset = deduplicatedDataset.drop("rownum");
        customTarget.writeToTarget(deduplicatedDataset);
    }

    @Test
    public void userManagedOverwriteWithStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("overwrite", "stage_schema", "true", "user_managed_with_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(overwrite_dataset);
    }
    @Test
    public void userManagedAppendWithStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("append", "stage_schema", "true", "user_managed_with_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(append_dataset);
    }
    @Test
    public void userManagedMergeWithStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("merge", "stage_schema", "true", "user_managed_with_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        merge_dataset.createOrReplaceTempView("tempTable");
        Dataset deduplicatedDataset = sparkSession.sql(
                "SELECT * " +
                        "FROM ( " +
                        "    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_col) AS rownum " +
                        "    FROM tempTable " +
                        ") ranked " +
                        "WHERE rownum = 1"
        );
        deduplicatedDataset = deduplicatedDataset.drop("rownum");
        customTarget.writeToTarget(deduplicatedDataset);
    }

    @Test
    public void userManagedOverwriteWithoutStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("overwrite", "", "true", "user_managed_without_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(overwrite_dataset);
    }
    @Test
    public void userManagedAppendWithoutStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("append", "", "true", "user_managed_without_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(append_dataset);
    }
    @Test
    public void userManagedMergeWithoutStaging() throws ExtensionsProcessingException {
        Map<String, String> userProps = setUserProps("merge", "", "true", "user_managed_without_staging");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        merge_dataset.createOrReplaceTempView("tempTable");
        Dataset deduplicatedDataset = sparkSession.sql(
                "SELECT * " +
                        "FROM ( " +
                        "    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_col) AS rownum " +
                        "    FROM tempTable " +
                        ") ranked " +
                        "WHERE rownum = 1"
        );
        deduplicatedDataset = deduplicatedDataset.drop("rownum");
        customTarget.writeToTarget(deduplicatedDataset);
    }

    @Test
    public void customTest1() throws ExtensionsProcessingException {
        // Specify MERGE mode even though target table doesn't exist.
        Map<String, String> userProps = setUserProps("merge", "", "false", "ar_custom_table1");
        UserProperties userProperties = new UserProperties(userProps);
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(overwrite_dataset);
    }
    @Test
    public void customTest2() throws ExtensionsProcessingException {
        // Multiple natural keys
        Map<String, String> userProps = setUserProps("overwrite", "", "false", "ar_custom_table_multi_primary_keys");
        UserProperties userProperties = new UserProperties(userProps);
        userProps.put("natural_columns", "id,int_col");
        userProps.put("schema", "public");
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(overwrite_dataset);
    }

    @Test
    public void customTest3() throws ExtensionsProcessingException {
        // Create Schema If not exists is True
        Map<String, String> userProps = setUserProps("overwrite", "", "false", "ar_custom_table3");
        UserProperties userProperties = new UserProperties(userProps);
        userProps.put("natural_key", "id,int_col");
        userProps.put("schema_name", "public_to_delete");
        userProps.put("create_schema_if_not_exists", "true");
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(overwrite_dataset);
    }

    @Test
    public void customTest4() throws ExtensionsProcessingException {
        Dataset testDs = sparkSession.read().option("header", "true").option("inferschema","true").csv("sf_data_sample100.csv");
        Map<String, String> userProps = setUserProps("overwrite", "", "false", "ar_custom_table4");
        UserProperties userProperties = new UserProperties(userProps);
        userProps.put("default_varchar_size", "100");
        userProps.put("default_long_varchar_size", "100");
        userProps.put("varchar_columns", "C_NAME:30,C_PHONE:50,c_comment:10000");
        userProps.put("long_varchar_columns", "C_ADDRESS:100");
        userProps.put("all_string_columns_as_long_varchar", "false");
        userProps.remove("partition_expression");
        userProps.remove("natural_key");
        customTarget.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        customTarget.writeToTarget(testDs);
    }

    private static SparkSession getSparkSession() {
        return SparkSession.builder().config("spark.master", "local").getOrCreate();
    }

    private static Dataset getDataset(SparkSession sparkSession, String csvFile) {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType,false)
                .add("string_col", DataTypes.StringType,false)
                .add("int_col", DataTypes.IntegerType,false)
                .add("long_col", DataTypes.LongType,false)
                .add("float_col", DataTypes.FloatType,false)
                .add("decimal_col", DataTypes.createDecimalType(38,8),false)
                .add("double_col", DataTypes.DoubleType,false)
                .add("date_col", DataTypes.DateType,false)
                .add("timestamp_col", DataTypes.TimestampType,false)
                .add("boolean_col", DataTypes.BooleanType,false);
        return sparkSession.read().option("header", "true").option("dateFormat","yyyy-MM-dd").option("timestampFormat","yyyy-MM-dd HH:mm:ss").schema(schema).csv(csvFile);
    }

}
