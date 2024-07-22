package com.infoworks;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;
import io.infoworks.platform.common.encryption.AES256;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class MySqlCustomTarget implements SparkCustomTarget, AutoCloseable {
    private Logger LOGGER = LoggerFactory.getLogger(MySqlCustomTarget.class);
    private String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private String jdbcUrl;
    private String userName;
    private String password;
    private String driverName;
    private String schema;
    private String stagingSchema;
    private String table;
    private TargetSyncMode syncMode;
    private List<String> naturalColumns;
    private String primaryColumnExpr;
    private String partitionExpression;
    private Connection jdbConnection;
    private boolean shouldCreateTableIfNotExists = true;
    private boolean shouldCreateSchemaIfNotExists = false;
    private boolean isUserManagedTable = false;
    private Properties sparkWriteOptions;
    private boolean convertColumnUppercase = false;
    private List<String> uppercaseColumns;
    private Integer defaultVarcharSize;
    private Map<String, Integer> varcharColumnsMappings;
    private Map<String, Integer> textColumnsMappings;
    private boolean allStringColsAsText = false;


    public MySqlCustomTarget() {
    }

    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties, ProcessingContext processingContext) {
        try {
            this.validateAndInitConfigs(userProperties);
            this.jdbConnection = this.getJDBCConnection(this.jdbcUrl, this.userName, this.password);

        } catch (Exception var5) {
            var5.printStackTrace();
            Throwables.propagate(var5);
        }

    }

    private void validateAndInitConfigs(UserProperties userProperties) throws IOException {
        this.jdbcUrl = userProperties.getValue("jdbc_url");
        Preconditions.checkNotNull(this.jdbcUrl, String.format("JDBC url can not be null. Please set property : %s ", "jdbc_url"));
        this.userName = userProperties.getValue("user_name");
        Preconditions.checkNotNull(this.userName, String.format("User name can not be null. Please set property : %s ", "user_name"));
        String encryptedPassword = userProperties.getValue("encrypted_password");
        Preconditions.checkNotNull(encryptedPassword, String.format("Encrypted password can not be null. Please set property : %s ", "encrypted_password"));
        this.password = AES256.decryptData((new BASE64Decoder()).decodeBuffer(encryptedPassword));

        this.driverName = userProperties.getValue("driver_name");
        if (this.driverName == null) {
            this.LOGGER.warn("Driver name is not set. Using default driver : {}. To set the driver name, set property : {}", this.MYSQL_DRIVER_NAME, "driver_name");
            this.driverName = this.MYSQL_DRIVER_NAME;
        }

        this.schema = userProperties.getValue("schema_name");
        Preconditions.checkNotNull(this.schema, String.format("Schema name can not be null. Please set property : %s ", "schema_name"));
        this.stagingSchema = this.schema;
        if (userProperties.getValue("staging_schema_name") != null) {
            this.stagingSchema = userProperties.getValue("staging_schema_name");
        }
        this.table = userProperties.getValue("table_name");
        Preconditions.checkNotNull(this.table, String.format("Table name can not be null. Please set property : %s ", "table_name"));

        Preconditions.checkNotNull(userProperties.getValue("build_mode"), String.format("Schema mode can not be null. Please set property : %s ", "build_mode"));
        this.syncMode = TargetSyncMode.valueOf(userProperties.getValue("build_mode"));

        String naturalColumnsStr = userProperties.getValue("natural_key");
        if (naturalColumnsStr != null) {
            this.primaryColumnExpr = naturalColumnsStr;
        }
        if (this.syncMode == TargetSyncMode.merge) {
            if (naturalColumnsStr == null) {
                throw new IllegalArgumentException("No natural columns specified for merge. To set the natural columns, set property : natural_key");
            }
            this.naturalColumns = Arrays.asList(naturalColumnsStr.split(","));
        }

        this.partitionExpression = userProperties.getValue("partition_expression");

        String upperCaseColumnsStr;

        if (userProperties.getValue("create_table_if_not_exists") != null) {
            this.shouldCreateTableIfNotExists = Boolean.parseBoolean(userProperties.getValue("create_table_if_not_exists"));
        }
        if (userProperties.getValue("create_database_if_not_exists") != null) {
            this.shouldCreateSchemaIfNotExists = Boolean.parseBoolean(userProperties.getValue("create_database_if_not_exists"));
        }
        if (userProperties.getValue("user_managed_table") != null) {
            this.isUserManagedTable = Boolean.parseBoolean(userProperties.getValue("user_managed_table"));
        }

        this.sparkWriteOptions = this.getSparkWriteOptions(userProperties.getValue("spark_write_options"));
        this.LOGGER.info("Spark write options are  : {} ", this.sparkWriteOptions);
        this.sparkWriteOptions.setProperty("user", this.userName);
        this.sparkWriteOptions.setProperty("password", this.password);
        this.sparkWriteOptions.setProperty("driver", this.driverName);

        if (userProperties.getValue("convert_columns_uppercase") != null) {
            this.convertColumnUppercase = Boolean.parseBoolean(userProperties.getValue("convert_columns_uppercase"));
            if (userProperties.getValue("uppercase_columns") != null) {
                upperCaseColumnsStr = userProperties.getValue("uppercase_columns");
                this.uppercaseColumns = Arrays.asList(upperCaseColumnsStr.split(","));
            }
        }

        if (userProperties.getValue("all_string_columns_as_text") != null) {
            this.allStringColsAsText = Boolean.parseBoolean(userProperties.getValue("all_string_columns_as_text"));
        }

        if (userProperties.getValue("default_varchar_size") != null) {
            this.defaultVarcharSize = Integer.valueOf(userProperties.getValue("default_varchar_size"));
        }
        if (userProperties.getValue("varchar_columns") != null) {
            this.varcharColumnsMappings = this.getColumnMap(userProperties.getValue("varchar_columns"));
        }
        if (userProperties.getValue("text_columns") != null) {
            this.textColumnsMappings = this.getColumnMap(userProperties.getValue("text_columns"));
        }

    }

    private Properties getSparkWriteOptions(String connectionPropertiesStr) {
        Properties sparkWriteOptions = new Properties();
        if (connectionPropertiesStr != null) {
            String[] properties = connectionPropertiesStr.split(";");
            String[] var4 = properties;
            int var5 = properties.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                String property = var4[var6];
                String[] keyValuePair = property.split("=");
                if (keyValuePair.length == 2) {
                    sparkWriteOptions.setProperty(keyValuePair[0], keyValuePair[1]);
                }
            }
        }

        return sparkWriteOptions;
    }

    public void writeToTarget(Dataset dataset) throws ExtensionsProcessingException {
        if (this.syncMode == MySqlCustomTarget.TargetSyncMode.overwrite) {
            if (!this.isUserManagedTable) {
                // Infoworks Managed Table
                this.overwrite(dataset, this.schema, this.stagingSchema, this.table,"overwrite");
            } else {
                // If it is user managed table then truncate the existing table and append the data
                if (this.checkIfTableExists(this.schema, this.table)) {
                    this.truncateTable(this.schema, this.table);
                    this.LOGGER.info("Deleted all rows from table "+this.table);
                } else {
                    this.LOGGER.error("Table does not exist. Please create the table manually or run the job by setting user_managed_table as false");
                    throw new RuntimeException("Table does not exist. Please create the table manually or run the job by setting user_managed_table as false");
                }
                this.append(dataset, this.schema, this.table);
            }

        } else if (!this.checkIfTableExists(this.schema, this.table)) {
            if (!this.shouldCreateTableIfNotExists) {
                throw new IllegalArgumentException(String.format("Target table %s.%s does not exists. Make sure that table exists or set option '%s' to true", this.schema, this.table, "create_table_if_not_exists"));
            }

            this.LOGGER.info("Target table does not exists, switching to overwrite mode.");
            this.overwrite(dataset, this.schema, this.stagingSchema, this.table,"overwrite");
        } else if (this.syncMode == MySqlCustomTarget.TargetSyncMode.append) {
            this.append(dataset, this.schema, this.table);
        } else {
            this.merge(dataset, this.schema, this.stagingSchema, this.table, this.naturalColumns);
        }

        this.LOGGER.info("Finished writing to the target.");
    }

    private void overwrite(Dataset dataset, String schemaName, String stagingSchema, String tableName, String mode) {
        String workingTable = this.getTempTableName(tableName);
        this.createTable(dataset, stagingSchema, workingTable);
        if (this.primaryColumnExpr!=null && !this.isUserManagedTable && this.syncMode == MySqlCustomTarget.TargetSyncMode.overwrite) {
            this.setPrimaryKey(stagingSchema, workingTable, primaryColumnExpr);
        }
        this.LOGGER.info("Inserting data to the target table {}.{}", stagingSchema, workingTable);
        String fullyQualifiedTableName = String.format("%s.%s", stagingSchema, workingTable);
        dataset = this.renameColumns(dataset);
        dataset.write().format(this.driverName).mode(SaveMode.Append).options(this.sparkWriteOptions).jdbc(this.jdbcUrl, fullyQualifiedTableName, this.sparkWriteOptions);
        if (!Objects.equals(schemaName, stagingSchema)) {
            // If stage and target schema are not same
            this.dropTableIfExists(stagingSchema, tableName);
            if (Objects.equals(mode, "overwrite")) {
                try {
                    this.dropTableIfExists(schemaName, tableName);
                } catch (RuntimeException e) {
                    this.LOGGER.error("Drop Table {}.{} has failed. Check if you have DROP privileges on schema {}. Continuing to rename the staging table {}.{} to main table {}.{}",schemaName,tableName,schemaName,stagingSchema,tableName,schemaName,tableName);
                }
            }
        }
        else
            this.dropTableIfExists(schemaName, tableName);
        this.renameTable(schemaName, stagingSchema, workingTable, tableName, mode);

    }


    private void append(Dataset dataset, String schemaName, String tableName) {
        this.LOGGER.info("Appending data to the target table {}.{}", schemaName, tableName);
        String fullyQualifiedTableName = String.format("%s.%s", schemaName, tableName);
        dataset.write().format(this.driverName).mode(SaveMode.Append).options(this.sparkWriteOptions).jdbc(this.jdbcUrl, fullyQualifiedTableName, this.sparkWriteOptions);
    }

    private void merge(Dataset deltaDataset, String schemaName,String stagingSchema, String tableName, List<String> naturalColumns) {
        this.LOGGER.info("Merging data to the target table {}.{}", schemaName, tableName);
        String deltaTable = this.getDeltaTableName(tableName);
        this.overwrite(deltaDataset, schemaName, stagingSchema, deltaTable, "merge");
        String fullyQualifiedTargetTableName = String.format("%s.%s", schemaName, tableName);
        String fullyQualifiedDeltaTableName = String.format("%s.%s", stagingSchema, deltaTable);
        List<String> tableColumns = this.getTableColumns(deltaDataset);
        String mergeTableStatement = this.getMergeTableStatement(fullyQualifiedTargetTableName, fullyQualifiedDeltaTableName, tableColumns, naturalColumns);
        this.executeDDLStatement(mergeTableStatement);
        this.dropTableIfExists(stagingSchema, deltaTable);
    }

    private void createTable(Dataset dataset, String schemaName, String tableName) {
        this.dropTableIfExists(schemaName, tableName);
        if (this.shouldCreateSchemaIfNotExists)
            this.createSchemaIfNotExists(schemaName);
        String columnsWithDataType = this.getCreateTableColumnsWithDataType(dataset);
        String createTableStatement = null;
        String baseStatement = String.format("CREATE TABLE %s.%s (%s)", schemaName, tableName, columnsWithDataType);
        if (this.partitionExpression == null || this.syncMode != TargetSyncMode.overwrite) {
            createTableStatement = baseStatement;
        } else {
            String partitionExpression = this.getPartitionExpression(this.partitionExpression);
            createTableStatement = baseStatement + " " + partitionExpression;
        }

        this.LOGGER.info(String.format("createTableStatement:%s",createTableStatement));
        System.out.printf("createTableStatement:%s%n",createTableStatement);
        this.executeDDLStatement(createTableStatement);
    }

    private void setPrimaryKey(String schemaName, String targetTable, String pkExpr) {
        String setPrimaryKeyStatement = String.format("ALTER TABLE %s.%s ADD PRIMARY KEY (%s)", schemaName, targetTable, pkExpr);
        this.executeDDLStatement(setPrimaryKeyStatement);
    }
    private void renameTable(String schemaName, String stagingSchema, String sourceTable, String targetTable, String mode) {
        String renameTableStatement;
        if (mode.equalsIgnoreCase("overwrite") && !stagingSchema.equalsIgnoreCase(schemaName)) {
            renameTableStatement = String.format("RENAME TABLE %s.%s TO %s.%s", stagingSchema, sourceTable, schemaName, targetTable);
        } else {
            renameTableStatement = String.format("RENAME TABLE %s.%s TO %s.%s", stagingSchema, sourceTable, stagingSchema, targetTable);
        }
        this.executeDDLStatement(renameTableStatement);
    }
    private Dataset renameColumns(Dataset dataset) {
        LinkedHashMap<String, String> dataFrame2SqlColumnsMapping = this.getDataFrame2SqlColumnsMapping(dataset);
        String dataFrameColumn;
        if (dataFrame2SqlColumnsMapping != null) {
            for(Iterator var3 = dataFrame2SqlColumnsMapping.keySet().iterator(); var3.hasNext(); dataset = dataset.withColumnRenamed(dataFrameColumn, (String)dataFrame2SqlColumnsMapping.get(dataFrameColumn))) {
                dataFrameColumn = (String)var3.next();
            }
        }
        return dataset;
    }

    private String getMergeTableStatement(String targetTable, String deltaTable, List<String> tableColumns, List<String> naturalColumns) {
        String insertColumnNamesExpression = String.join(",", tableColumns);
        String updateRowExpression = this.getUpdateExpression(tableColumns);

        String mergeTableStatement = String.format("INSERT INTO %s (%s)\n SELECT %s FROM %s\n ON DUPLICATE KEY UPDATE\n %s", targetTable, insertColumnNamesExpression, insertColumnNamesExpression, deltaTable, updateRowExpression);
        return mergeTableStatement;
    }

    private String getUpdateExpression(List<String> columns) {
        List<String> updateColumnExpressionList = new ArrayList<>();
        Iterator<String> var5 = columns.iterator();

        while(var5.hasNext()) {
            String columnName = (String)var5.next();
            updateColumnExpressionList.add(String.format("%s = VALUES(%s)", columnName, columnName));
        }
        return String.join(" , ", updateColumnExpressionList);
    }

    private void createSchemaIfNotExists(String schemaName) {
        String dropSchemaQuery = String.format("CREATE DATABASE IF NOT EXISTS %s", schemaName);
        this.executeDDLStatement(dropSchemaQuery);
    }

    private void dropTableIfExists(String schemaName, String tableName) {
        String dropTableQuery = String.format("DROP TABLE IF EXISTS %s.%s ", schemaName, tableName);
        this.executeDDLStatement(dropTableQuery);
    }

    private boolean checkIfTableExists(String schemaName, String tableName) {
        String query = String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", schemaName, tableName);
        return this.checkIfResultSetNonZero(this.executeQuery(query));
    }

    private String getPartitionExpression(String partitionColumnName) {
        return String.format("PARTITION BY %s ", partitionColumnName);
    }

    private void truncateTable(String schemaName, String tableName) {
        String truncateTableStatement = String.format("TRUNCATE TABLE %s.%s", schemaName, tableName);
        this.executeDDLStatement(truncateTableStatement);
    }
    private String getTempTableName(String targetTable) {
        return String.format("%s_temp", targetTable);
    }

    private String getDeltaTableName(String targetTable) {
        return String.format("%s_delta", targetTable);
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

    public Connection getJDBCConnection(String jdbcUrl, String username, String password) {
        Connection connection = null;

        try {
            Class.forName(this.driverName);
            connection = DriverManager.getConnection(jdbcUrl,username,password);
            return connection;
        } catch (Exception var4) {
            var4.printStackTrace();
            Throwables.propagate(var4);
            return connection;
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
            String columnTypeWithoutPrecision = sparkDataType;
            String columnTypePrecision = "";
            int precisionIndex = sparkDataType.indexOf("(");
            if (precisionIndex != -1) {
                columnTypeWithoutPrecision = sparkDataType.substring(0, precisionIndex);
                columnTypePrecision = sparkDataType.substring(precisionIndex);
            }

            if (spark2SQLDataTypeMapping.containsKey(columnTypeWithoutPrecision)) {
                String sqlType = (String)spark2SQLDataTypeMapping.get(columnTypeWithoutPrecision);
                if ("StringType".equals(columnTypeWithoutPrecision)) {
                    sqlType = this.getVarcharType(columnName, sqlType);
                }

                dataTypeModified = String.format("%s%s", sqlType, columnTypePrecision);
            }
        }

        if (this.convertColumnUppercase) {
            return this.convertColumnNamesToUpperCase(sqlColumnsDataMapping);
        } else {
            return sqlColumnsDataMapping;
        }
    }

    private String getVarcharType(String columnName, String defaultSqlType) {
        columnName = columnName.toUpperCase();

        // Check if all_string_cols_as_text is set
        if (this.allStringColsAsText) {
            return "TEXT";
        }

        // Check if the column name is part of varchar_columns
        if (this.varcharColumnsMappings!=null && this.varcharColumnsMappings.containsKey(columnName)) {
            int size = this.varcharColumnsMappings.get(columnName);
            return "VARCHAR(" + size + ")";
        }

        // Check if the column name is part of long_varchar_columns
        if (this.textColumnsMappings!=null && this.textColumnsMappings.containsKey(columnName)) {
            int size = this.textColumnsMappings.get(columnName);
            return "LONG VARCHAR(" + size + ")";
        }

        // Check if default_varchar_size is set
        if (this.defaultVarcharSize!=null && this.defaultVarcharSize > 0) {
            return "VARCHAR(" + this.defaultVarcharSize + ")";
        }

        return "VARCHAR(100)";
    }

    private  Map<String, Integer> getColumnMap(String columnString) {
        Map<String, Integer> columnMap = new HashMap<>();
        String[] columns = columnString.split(",");
        try {
            for (String column : columns) {
                String[] parts = column.split(":");
                if (parts.length == 2) {
                    String name = parts[0].trim().toUpperCase();
                    int size = Integer.parseInt(parts[1].trim());
                    columnMap.put(name, size);
                } else {
                    // Handle invalid format
                    this.LOGGER.error("Invalid format for column: " + column);
                }
            }
        } catch (NumberFormatException e) {
            // Handle parsing errors
            this.LOGGER.error("Error parsing column size: " + e.getMessage());
        }
        return columnMap;
    }

    private LinkedHashMap<String, String> getDataFrame2SqlColumnsMapping(Dataset dataset) {
        LinkedHashMap<String, String> sqlDataTypeMapping = this.getSqlColumnsDataTypeMapping(dataset);
        LinkedHashMap<String, String> columnsMapping = new LinkedHashMap();
        if (sqlDataTypeMapping == null) {
            return columnsMapping;
        } else {
            if (dataset != null) {
                Arrays.stream(dataset.schema().fields()).forEach((field) -> {
                    Iterator var3 = sqlDataTypeMapping.keySet().iterator();

                    while(var3.hasNext()) {
                        String sqlColumn = (String)var3.next();
                        if (field.name().equalsIgnoreCase(sqlColumn)) {
                            columnsMapping.put(field.name(), sqlColumn);
                        }
                    }

                });
            }

            return columnsMapping;
        }
    }

    private LinkedHashMap<String, String> convertColumnNamesToUpperCase(LinkedHashMap<String, String> sqlColumnsDataMapping) {
        LinkedHashMap<String, String> upperCaseSqlColumnsDataMapping = new LinkedHashMap();
        Iterator var3;
        String columnName;
        if (this.uppercaseColumns != null && !this.uppercaseColumns.isEmpty()) {
            if (sqlColumnsDataMapping != null && !sqlColumnsDataMapping.isEmpty()) {
                var3 = sqlColumnsDataMapping.keySet().iterator();

                while(var3.hasNext()) {
                    columnName = (String)var3.next();
                    if (this.uppercaseColumns.contains(columnName)) {
                        upperCaseSqlColumnsDataMapping.put(columnName.toUpperCase(), sqlColumnsDataMapping.get(columnName));
                    } else {
                        upperCaseSqlColumnsDataMapping.put(columnName, sqlColumnsDataMapping.get(columnName));
                    }
                }
            }
        } else if (sqlColumnsDataMapping != null && !sqlColumnsDataMapping.isEmpty()) {
            var3 = sqlColumnsDataMapping.keySet().iterator();

            while(var3.hasNext()) {
                columnName = (String)var3.next();
                upperCaseSqlColumnsDataMapping.put(columnName.toUpperCase(), sqlColumnsDataMapping.get(columnName));
            }
        }

        return upperCaseSqlColumnsDataMapping;
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

    private List<String> getTableColumns(Dataset dataset) {
        LinkedHashMap<String, String> sqlColumnsDataTypeMapping = this.getSqlColumnsDataTypeMapping(dataset);
        List<String> columnsList = new ArrayList();
        Iterator var4 = sqlColumnsDataTypeMapping.keySet().iterator();

        while(var4.hasNext()) {
            String columnName = (String)var4.next();
            columnsList.add(columnName);
        }

        return columnsList;
    }

    private Map<String, String> getSpark2SQLDataTypeMapping() {
        Map<String, String> dataTypeMapping = new HashMap();
        dataTypeMapping.put("DecimalType", "DECIMAL");
        dataTypeMapping.put("IntegerType", "INT");
        dataTypeMapping.put("FloatType", "FLOAT");
        dataTypeMapping.put("DoubleType", "DOUBLE");
        dataTypeMapping.put("LongType", "BIGINT");
        dataTypeMapping.put("ShortType", "SMALLINT");
        dataTypeMapping.put("StringType", "VARCHAR");
        dataTypeMapping.put("DateType", "DATE");
        dataTypeMapping.put("TimestampType", "TIMESTAMP");
        dataTypeMapping.put("ArrayType", "TEXT");
        dataTypeMapping.put("BinaryType", "BLOB");
        dataTypeMapping.put("BooleanType", "BOOLEAN");
        dataTypeMapping.put("ByteType", "TINYINT");
        dataTypeMapping.put("CalendarIntervalType", "INTERVAL");
        dataTypeMapping.put("NullType", "null");
        return dataTypeMapping;
    }

    public void close() throws Exception {
        if (this.jdbConnection != null) {
            this.jdbConnection.close();
        }

    }

    public static void main(String[] args) {
        System.out.println("Please run the test suite for test cases.");
    }

    public static enum TargetSyncMode {
        overwrite,
        append,
        merge;

        private TargetSyncMode() {
        }
    }
}