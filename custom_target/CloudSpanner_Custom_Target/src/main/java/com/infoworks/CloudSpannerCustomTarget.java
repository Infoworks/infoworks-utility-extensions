package com.infoworks;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.base.Throwables;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.*;
import java.util.*;


public class CloudSpannerCustomTarget implements SparkCustomTarget, AutoCloseable {

    private Logger LOGGER = LoggerFactory.getLogger(CloudSpannerCustomTarget.class);
    private String CLOUD_SPANNER_DRIVER_NAME = "com.google.cloud.spanner.jdbc.JdbcDriver";
    private String projectId;
    private String instanceId;
    private String credentialPath;
    private String database;
    private String table;
    private TargetSyncMode syncMode;
    private List<String> primaryKeys;
    private boolean userManagedTable = false;
    private boolean coalesce = false;
    private Connection jdbConnection;

    private String indexColumns;

    private Integer numOfPartitions = 10;

    private SparkSession spark;
    private UserProperties userProperties;
    private Integer batch_size;
    private List<String> floatColumns =  new ArrayList<>();
    private LongAccumulator dbClientacc;

    private void validateAndInitConfigs(UserProperties userProperties) throws IOException {
        this.projectId = userProperties.getValue("projectId");
        Preconditions.checkNotNull(this.projectId, String.format("Project Id url can not be null. Please set property : %s ", "projectId"));
        this.instanceId = userProperties.getValue("instanceId");
        Preconditions.checkNotNull(this.instanceId, String.format("Instance Id can not be null. Please set property : %s ", "instanceId"));
        this.credentialPath = userProperties.getValue("credentialPath");
        Preconditions.checkNotNull(this.credentialPath, String.format("Credential path  can not be null. Please set property : %s ", "credentialPath"));
        this.database = userProperties.getValue("database");
        Preconditions.checkNotNull(this.database, String.format("Database name can not be null. Please set property : %s ", "database"));
        this.table = userProperties.getValue("table");
        Preconditions.checkNotNull(this.table, String.format("Table name can not be null. Please set property : %s ", "table"));
        this.syncMode = CloudSpannerCustomTarget.TargetSyncMode.valueOf(userProperties.getValue("syncMode"));
        Preconditions.checkNotNull(userProperties.getValue("syncMode"), String.format("Sync mode can not be null. Please set property : %s ", "syncMode"));

        String naturalColumnsStr = userProperties.getValue("primary_keys");
        if (naturalColumnsStr != null) {
            this.primaryKeys = Arrays.asList(naturalColumnsStr.split(","));
        }

        if (userProperties.getValue("user_managed_table") != null) {
            this.userManagedTable = Boolean.parseBoolean(userProperties.getValue("user_managed_table"));
        }

        if (userProperties.getValue("coalesce") != null) {
            this.coalesce = Boolean.parseBoolean(userProperties.getValue("coalesce"));
        }

        this.indexColumns = userProperties.getValue("indexColumns");
        String no_of_partitions = userProperties.getValue("numOfPartitions");
        if (no_of_partitions != null) {
            this.numOfPartitions = Integer.valueOf(userProperties.getValue("numOfPartitions"));
        }
        if (userProperties.getValue("batchSize") != null) {
            this.batch_size = Integer.valueOf(userProperties.getValue("batchSize"));
        }

    }

    public Connection getJDBCConnection() throws SQLException {
        String connectionUrl =
                String.format(
                        "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                        this.projectId, this.instanceId, this.database);
        try
        {
            Class.forName(this.CLOUD_SPANNER_DRIVER_NAME);
        }
        catch (Exception ex)
        {
            System.err.println("Driver not found");
        }
        connectionUrl = connectionUrl + "?credentials="+this.credentialPath+";autocommit=true";
        System.out.println("Trying to connect to "+connectionUrl);
        return DriverManager.getConnection(connectionUrl);
    }
    @Override
    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties, ProcessingContext processingContext) {
        this.spark = sparkSession;
        this.dbClientacc = this.spark.sparkContext().longAccumulator("DbClientAccumulator");
        this.userProperties = userProperties;
        try {
            this.validateAndInitConfigs(userProperties);
            this.jdbConnection = this.getJDBCConnection();
            this.jdbConnection.createStatement().execute("SET STATEMENT_TIMEOUT = '300s'");
        } catch (Exception var5) {
            var5.printStackTrace();
            Throwables.propagate(var5);
        }
    }

    private Map<String, String> getSpark2MutationMapping() {
        Map<String, String> dataTypeMapping = new HashMap();
        dataTypeMapping.put("DecimalType", "decimal");
        dataTypeMapping.put("IntegerType", "integer");
        dataTypeMapping.put("FloatType", "float");
        dataTypeMapping.put("DoubleType", "double");
        dataTypeMapping.put("LongType", "long");
        dataTypeMapping.put("StringType", "string");
        dataTypeMapping.put("DateType", "date");
        dataTypeMapping.put("TimestampType", "timestamp");
        dataTypeMapping.put("ArrayType", "array");
        dataTypeMapping.put("BooleanType", "boolean");
        dataTypeMapping.put("ShortType", "short");
        dataTypeMapping.put("ByteType", "byte");
        return dataTypeMapping;
    }

    private Map<String, String> getSpark2SQLDataTypeMapping() {
        Map<String, String> dataTypeMapping = new HashMap();
        dataTypeMapping.put("DecimalType", "NUMERIC");
        dataTypeMapping.put("IntegerType", "INT64");
        dataTypeMapping.put("FloatType", "FLOAT64");
        dataTypeMapping.put("DoubleType", "FLOAT64");
        dataTypeMapping.put("LongType", "INT64");
        dataTypeMapping.put("ShortType", "INT64");
        dataTypeMapping.put("ByteType", "BYTES(1)");
        dataTypeMapping.put("StringType", "STRING(MAX)");
        dataTypeMapping.put("DateType", "DATE");
        dataTypeMapping.put("TimestampType", "TIMESTAMP");
        dataTypeMapping.put("BooleanType", "BOOL");
        return dataTypeMapping;
    }


    protected LinkedHashMap<String, String> getMutationMapping(Dataset dataset) {
        LinkedHashMap<String, String> dataframeColumnsDataMapping = new LinkedHashMap();
        LinkedHashMap<String, String> sqlColumnsDataMapping = new LinkedHashMap();
        Arrays.stream(dataset.schema().fields()).forEach((field) -> {
            dataframeColumnsDataMapping.put(field.name(), field.dataType().toString());
        });
        Map<String, String> getSpark2MutationMapping = this.getSpark2MutationMapping();

        String columnName;
        String dataTypeModified;
        for(Iterator var5 = dataframeColumnsDataMapping.keySet().iterator(); var5.hasNext(); sqlColumnsDataMapping.put(columnName, dataTypeModified)) {
            columnName = (String)var5.next();
            String sparkDataType = (String)dataframeColumnsDataMapping.get(columnName);
            dataTypeModified = sparkDataType;
            String columnTypeWithoutPrecision = sparkDataType;
            String columnTypePrecision = "";
            int precisionIndex = sparkDataType.indexOf("(");
            if (precisionIndex != -1) {
                columnTypeWithoutPrecision = sparkDataType.substring(0, precisionIndex);
                columnTypePrecision = sparkDataType.substring(precisionIndex);
            }
            String sqlType = (String)getSpark2MutationMapping.get(columnTypeWithoutPrecision);
            dataTypeModified = String.format("%s%s", sqlType, columnTypePrecision);

        }

        return sqlColumnsDataMapping;
    }

    protected void findFloatColumns(Dataset dataset) {
        LinkedHashMap<String, String> dataframeColumnsDataMapping = new LinkedHashMap();
        Arrays.stream(dataset.schema().fields()).forEach((field) -> {
            dataframeColumnsDataMapping.put(field.name(), field.dataType().toString());
        });
        String columnName;
        for (Iterator var5 = dataframeColumnsDataMapping.keySet().iterator(); var5.hasNext();) {
            columnName = (String) var5.next();
            String sparkDataType = (String) dataframeColumnsDataMapping.get(columnName);
            if (sparkDataType.toLowerCase().startsWith("float")) {
                this.floatColumns.add(columnName);
            }
        }
    }

    protected LinkedHashMap<String, String> getSqlColumnsDataTypeMapping(Dataset dataset) {
        LinkedHashMap<String, String> dataframeColumnsDataMapping = new LinkedHashMap();
        LinkedHashMap<String, String> sqlColumnsDataMapping = new LinkedHashMap();
        Arrays.stream(dataset.schema().fields()).forEach((field) -> {
            dataframeColumnsDataMapping.put(field.name(), field.dataType().toString());
        });
        Map<String, String> spark2SQLDataTypeMapping = this.getSpark2SQLDataTypeMapping();

        String columnName;
        String dataTypeModified;
        for(Iterator var5 = dataframeColumnsDataMapping.keySet().iterator(); var5.hasNext(); sqlColumnsDataMapping.put(columnName, dataTypeModified)) {
            columnName = (String)var5.next();
            String sparkDataType = (String)dataframeColumnsDataMapping.get(columnName);
            dataTypeModified = sparkDataType;
            if (sparkDataType.toLowerCase().startsWith("float")){
                this.floatColumns.add(columnName);
            }
            String columnTypeWithoutPrecision = sparkDataType;
            String columnTypePrecision = "";
            int precisionIndex = sparkDataType.indexOf("(");
            if (precisionIndex != -1) {
                columnTypeWithoutPrecision = sparkDataType.substring(0, precisionIndex);
                columnTypePrecision = sparkDataType.substring(precisionIndex);
            }
            String sqlType = (String)spark2SQLDataTypeMapping.get(columnTypeWithoutPrecision);
            //dataTypeModified = String.format("%s%s", sqlType, columnTypePrecision);
            dataTypeModified = String.format("%s", sqlType);

        }

        return sqlColumnsDataMapping;
    }

    private void overwrite(Dataset dataset, String spannerUrl) {
        if (this.userManagedTable) {
            // This table is managed by user. So do not drop the table. Just delete all the contents from table as it is in overwrite mode.
            // Before that check if table exists. If not break
            this.LOGGER.info("This table is managed by user. So do not drop the table. Just delete all the contents from table as it is in overwrite mode.");
            if (this.checkIfTableExists(this.table)) {
                this.deleteAllRows(this.table);
                this.LOGGER.info("Deleted all rows from table "+this.table);
            } else {
                this.LOGGER.error("Table does not exist. Please create the table manually or run the job by setting userManagedTable as false");
                throw new RuntimeException("Table does not exist. Please create the table manually or run the job by setting userManagedTable as false");
            }

        } else {
            this.createTable(dataset, this.table);
        }

        for (String column : this.floatColumns) {
            dataset = dataset.withColumn(column, functions.col(column).cast(DataTypes.StringType)).withColumn(column, functions.col(column).cast(DataTypes.DoubleType));
        }
        dataset.write().format("jdbc").mode("append")
                .option(JDBCOptions.JDBC_URL(), spannerUrl)
                .option(JDBCOptions.JDBC_TABLE_NAME(), this.table)
                .option(JDBCOptions.JDBC_DRIVER_CLASS(), this.CLOUD_SPANNER_DRIVER_NAME)
                .option(
                        JDBCOptions.JDBC_TXN_ISOLATION_LEVEL(),
                        "NONE")
                .save();
        this.LOGGER.info("Data has been inserted to the table "+this.table);
        if (this.indexColumns != null) {
            List<String> indexCols = Arrays.asList(this.indexColumns.split(","));
            try {
                this.createIndex(this.table, indexCols);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
    @Override
    public void writeToTarget(Dataset dataset) {
        JdbcDialects.registerDialect(new SpannerJdbcDialect());
        String spannerUrl =
                String.format(
                        "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                        this.projectId, this.instanceId, this.database);
        spannerUrl = spannerUrl + "?credentials="+this.credentialPath+";autocommit=true;lenient=true";

        if (this.syncMode == CloudSpannerCustomTarget.TargetSyncMode.overwrite) {
            this.LOGGER.info("Running in OVERWRITE mode");
            this.overwrite(dataset, spannerUrl);

        } else if (this.syncMode == CloudSpannerCustomTarget.TargetSyncMode.append) {

            if (this.checkIfTableExists(this.table)) {
                this.LOGGER.info("Running in APPEND mode");
                this.findFloatColumns(dataset);
                for (String column : this.floatColumns) {
                    dataset = dataset.withColumn(column, functions.col(column).cast(DataTypes.StringType)).withColumn(column, functions.col(column).cast(DataTypes.DoubleType));
                }
                dataset.write().format("jdbc").mode("append")
                        .option(JDBCOptions.JDBC_URL(), spannerUrl)
                        .option(JDBCOptions.JDBC_TABLE_NAME(), this.table)
                        .option(JDBCOptions.JDBC_DRIVER_CLASS(), this.CLOUD_SPANNER_DRIVER_NAME)
                        .option(
                                JDBCOptions.JDBC_TXN_ISOLATION_LEVEL(),
                                "NONE")
                        .save();
                this.LOGGER.info("Data has been written to the table "+this.table);
            } else {
                this.LOGGER.info(String.format("Table with name: %s doesn't exist in Spanner.",this.table));
                this.LOGGER.info("Running in OVERWRITE mode");
                this.overwrite(dataset, spannerUrl);
            }


        }
        else {
            if (this.checkIfTableExists(this.table)) {
                this.LOGGER.info("Running in MERGE mode");
                LinkedHashMap<String, String> MutationDataMapping = getMutationMapping(dataset);
                if (this.batch_size != null) {
                    this.LOGGER.info("Copying the data to Cloud Spanner in batches to avoid mutation quota limit i.e <40k transactions");
                    long row_count = dataset.count();
                    this.LOGGER.info("Total row count: "+row_count);
                    int num_of_chunks = (int) Math.round(Math.ceil((double) row_count / this.batch_size));
                    this.LOGGER.info("Total number of chunks: "+num_of_chunks);
                    //Dataset copy_dataset = dataset;

                    dataset.createOrReplaceTempView("iwx_spanner_internal_merge_temp_view");
                    Dataset copy_dataset = this.spark.sql("select *,row_number() over (order by 1) as row_num from iwx_spanner_internal_merge_temp_view");
                    copy_dataset.persist();
                    int start = 1;
                    int end = 0;
                    for (int number = 1; number <= num_of_chunks; number++) {
                        this.LOGGER.info("Upserting data Batch :" + Integer.toString(number));
                        end = this.batch_size + start;
                        System.out.println(String.format("start %s end %s",start,end));
                        Dataset temp_dataset = copy_dataset.where(String.format("row_num >= %s and row_num < %s",start,end));
                        if (! this.coalesce)
                            temp_dataset.repartition(this.numOfPartitions).foreachPartition(SpannerForEachPartition.upsert(this.table, MutationDataMapping, this.projectId,this.instanceId,this.database,this.credentialPath,this.dbClientacc));
                        else
                            temp_dataset.coalesce(this.numOfPartitions).foreachPartition(SpannerForEachPartition.upsert(this.table, MutationDataMapping, this.projectId,this.instanceId,this.database,this.credentialPath,this.dbClientacc));
                        start = end;
                    }

//                    for (int number = 1; number <= num_of_chunks; number++) {
//                        this.LOGGER.info("Upserting data Batch :" + Integer.toString(number));
//                        Dataset temp_dataset = copy_dataset.limit(this.batch_size);
//                        temp_dataset.persist();
//                        copy_dataset = copy_dataset.exceptAll(temp_dataset);
//                        if (! this.coalesce)
//                            temp_dataset.repartition(this.numOfPartitions).foreachPartition(SpannerForEachPartition.upsert(this.table, MutationDataMapping, this.projectId,this.instanceId,this.database,this.credentialPath,this.dbClientacc));
//                        else
//                            temp_dataset.coalesce(this.numOfPartitions).foreachPartition(SpannerForEachPartition.upsert(this.table, MutationDataMapping, this.projectId,this.instanceId,this.database,this.credentialPath,this.dbClientacc));
//                    }
                } else {
                    if (! this.coalesce)
                        dataset.coalesce(this.numOfPartitions).foreachPartition(SpannerForEachPartition.upsert(this.table, MutationDataMapping, this.projectId,this.instanceId,this.database,this.credentialPath,this.dbClientacc));
                    else
                        dataset.repartition(this.numOfPartitions).foreachPartition(SpannerForEachPartition.upsert(this.table, MutationDataMapping, this.projectId,this.instanceId,this.database,this.credentialPath,this.dbClientacc));
                }
            }
            else {
                this.LOGGER.info(String.format("Table with name: %s doesn't exist in Spanner.",this.table));
                this.LOGGER.info("Running in OVERWRITE mode");
                this.overwrite(dataset, spannerUrl);
            }
        }
    this.LOGGER.info("Accumulator value is "+this.dbClientacc.value());
    }

    private boolean checkIfTableExists(String tableName) {
        String query = String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' AND TABLE_NAME = '%s'",  tableName);
        return this.checkIfResultSetNonZero(this.executeQuery(query));
    }
    private boolean checkIfResultSetNonZero(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                return resultSet.next();
            } catch (Exception var3) {
                var3.printStackTrace();
                Throwables.propagate(var3);
            }
        }

        return false;
    }
    private ResultSet executeQuery(String sqlStatement) {
        ResultSet resultSet = null;

        try {
            this.LOGGER.info("Executing query : \n " + sqlStatement);
            Statement statement = this.jdbConnection.createStatement();
            resultSet = statement.executeQuery(sqlStatement);
        } catch (Exception var4) {
            var4.printStackTrace();
            Throwables.propagate(var4);
        }

        return resultSet;
    }

    private String getCreateTableColumnsWithDataType(Dataset dataset) {
        LinkedHashMap<String, String> sqlColumnsDataTypeMapping = this.getSqlColumnsDataTypeMapping(dataset);
        List<String> columnDataTypeList = new ArrayList();
        Iterator var4 = sqlColumnsDataTypeMapping.keySet().iterator();

        while(var4.hasNext()) {
            String columnName = (String)var4.next();
            columnDataTypeList.add(String.format("%s %s", columnName, sqlColumnsDataTypeMapping.get(columnName)));
        }

        return String.join(",", columnDataTypeList);
    }

    private String getPrimaryKeyExpression( List<String> primaryKeys) {
        String primaryKeyExpr = String.join(",", primaryKeys);
        return String.format("PRIMARY KEY (%s)", primaryKeyExpr);
    }

    private void createTable(Dataset dataset, String tableName) {
        this.dropTableIfExists(tableName);
        String columnsWithDataType = this.getCreateTableColumnsWithDataType(dataset);
        String createTableStatement = null;
        if (this.primaryKeys == null) {
            createTableStatement = String.format("CREATE TABLE %s (%s)", tableName, columnsWithDataType);
        } else {
            String primarykeyExpression = getPrimaryKeyExpression(this.primaryKeys);
            createTableStatement = String.format("CREATE TABLE %s (%s) %s",tableName, columnsWithDataType, primarykeyExpression);
        }

        this.executeDDLStatement(createTableStatement);
    }

    private void dropTableIfExists(String tableName) {
        try {
            this.dropIndex(tableName);
            String dropTableQuery = String.format("DROP TABLE %s ", tableName);
            this.executeDDLStatement(dropTableQuery);
        } catch (Exception exp)
        {
            exp.printStackTrace();
        }
    }

    private void executeDDLStatement(String sqlStatement) {
        try {
            this.LOGGER.info("Executing query : \n " + sqlStatement);
            Statement statement = this.jdbConnection.createStatement();
            statement.executeUpdate(sqlStatement);
        } catch (Exception var3) {
            var3.printStackTrace();
            Throwables.propagate(var3);
        }

    }

    private void createIndex(String TableName, List<String> IndexColumns) throws SQLException{
        String IndexColumnString;
        IndexColumnString = String.join(",",IndexColumns);
        String query = "CREATE Index "+TableName+"_Index"+" ON "+TableName+"("+IndexColumnString+");";
        try {
            this.LOGGER.info("Executing query : \n " + query);
            Statement statement = this.jdbConnection.createStatement();
            statement.execute(query);
        } catch (Exception var4) {
            var4.printStackTrace();
            Throwables.propagate(var4);
        }
        this.LOGGER.info("Created index successfully");
    }

    private void dropIndex(String TableName) throws SQLException{
        String query = "DROP Index "+TableName+"_Index";
        try {
            this.LOGGER.info("Executing query : \n " + query);
            Statement statement = this.jdbConnection.createStatement();
            statement.execute(query);
        } catch (Exception var4) {
            var4.printStackTrace();
            //Throwables.propagate(var4);
        }
        this.LOGGER.info("Dropped Infoworks created index successfully");
    }

    private void deleteAllRows(String TableName) {
        try {
            List<Mutation> mutations = new ArrayList<>();
            mutations.add(Mutation.delete(TableName, KeySet.all()));
            DatabaseClient dbClient = SpannerDbClient.getDbClient(this.projectId,this.instanceId, this.database,this.credentialPath);
            dbClient.write(mutations);
        } catch (Exception e) {
            e.printStackTrace(

            );
        }


    }
    @Override
    public void close() throws Exception {
        if (this.jdbConnection != null) {
            this.jdbConnection.close();
        }
    }

    public static enum TargetSyncMode {
        overwrite,
        append,
        merge;

        private TargetSyncMode() {
        }
    }

}


