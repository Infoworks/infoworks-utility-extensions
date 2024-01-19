package com.infoworks;


import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;


import java.util.*;
import java.util.concurrent.TimeUnit;


public class DynamoDBCustomTarget implements SparkCustomTarget {
    private SparkSession spark;
    private UserProperties userProperties;
    static Logger LOGGER = Logger.getLogger(DynamoDBCustomTarget.class);


    @Override
    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties, ProcessingContext processingContext) {
        this.spark = sparkSession;
        this.userProperties  = userProperties;
    }

    String tableBackup(String serviceEndpoint, String region, String tableName) throws InterruptedException {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .build();

        CreateBackupRequest var = new CreateBackupRequest();
        var.setBackupName(tableName+"_"+DateTime.now().toString("yyyyMMddHHmmss"));
        var.setTableName(tableName);
        String backupArn = client.createBackup(var).getBackupDetails().getBackupArn();
        DescribeBackupRequest var_req = new DescribeBackupRequest();
        var_req.setBackupArn(backupArn);
        while (true)
        {
            if (client.describeBackup(var_req).getBackupDescription().getBackupDetails().getBackupStatus().equals("AVAILABLE")) {
                System.out.println("Backup done");
                break;
            }
            else {
                System.out.println("Backup unavailable");
                TimeUnit.SECONDS.sleep(10);
            }
        }
        return backupArn;

    }
    private void truncateTable(String serviceEndpoint, String region, String tableName) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        //May be we can make it look generic by reading key schema first as below
        String strPartitionKey = null;
        String strSortKey = null;
        TableDescription description = table.describe();
        List<KeySchemaElement> schema = description.getKeySchema();
        for (KeySchemaElement element : schema) {
            if (element.getKeyType().equalsIgnoreCase("HASH"))
                strPartitionKey = element.getAttributeName();
            if (element.getKeyType().equalsIgnoreCase("RANGE"))
                strSortKey = element.getAttributeName();
        }

        ItemCollection<ScanOutcome> deleteoutcome = table.scan();
        for (Item next : deleteoutcome) {
            if (strSortKey == null && strPartitionKey != null)
                table.deleteItem(strPartitionKey, next.get(strPartitionKey));
            else if (strPartitionKey != null)
                table.deleteItem(strPartitionKey, next.get(strPartitionKey), strSortKey, next.get(strSortKey));
        }
        System.out.println("Truncated the table");
    }

    private void createTable(String serviceEndpoint, String region, String tableName, String key_schema, String ReadCapacityUnits, String WriteCapacityUnits) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        try {
            System.out.println("Attempting to create table; please wait...");
            String[] key_schemas_hash = key_schema.split(",");
            List<KeySchemaElement> keyschemaarray = new ArrayList<>();
            List<AttributeDefinition> attrarray = new ArrayList<>();
            for (String s: key_schemas_hash){
                String[] i = s.split(":");
                String name = i[0];
                String keytype = i[1];
                String attrtype = i[2];
                if (keytype.equals("HASH")) keyschemaarray.add(new KeySchemaElement(name, KeyType.HASH));
                else keyschemaarray.add(new KeySchemaElement(name, KeyType.RANGE));
                if (attrtype.equals("S")) attrarray.add(new AttributeDefinition(name, ScalarAttributeType.S));
                else attrarray.add(new AttributeDefinition(name, ScalarAttributeType.N));
            }
            Table table = dynamoDB.createTable(tableName,keyschemaarray,attrarray,
                    new ProvisionedThroughput(Long.parseLong(ReadCapacityUnits),Long.parseLong(WriteCapacityUnits)));

            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void writeToTarget(Dataset dataset) throws ExtensionsProcessingException {
        String endpoint_url = userProperties.getValue("endpoint");
        String ddb_table_name = userProperties.getValue("ddb_table_name");
        String region =  userProperties.getValue("region") == null ? "us-west-2" : userProperties.getValue("region");
        String mode = userProperties.getValue("mode") == null ? "overwrite" : userProperties.getValue("mode");
        String table_exists = userProperties.getValue("table_exists") == null ? "false" : userProperties.getValue("table_exists");
        String ReadCapacityUnits = userProperties.getValue("ReadCapacityUnits") == null ? "10" : userProperties.getValue("ReadCapacityUnits");
        String WriteCapacityUnits = userProperties.getValue("WriteCapacityUnits") == null ? "10" : userProperties.getValue("WriteCapacityUnits");
        String key_schema = userProperties.getValue("key_schema");
        Map<String, String> dfoptions = new HashMap();
        String[] ar = { "region", "roleArn", "writeBatchSize", "targetCapacity", "throughput", "inferSchema","endpoint"};
        for (String i : ar) {
            if (userProperties.getValue(i) != null )
                dfoptions.put(i,userProperties.getValue(i));
        }
        if ("overwrite".equalsIgnoreCase(mode))
        {
            LOGGER.info("Writing to DynamoDB in overwrite mode");
            if ("true".equalsIgnoreCase(table_exists))
            {
                // Make a backup of Table_A, restore to Table_B. Take a backup of Table_B. Drop Table_A. Restore Table_B as Table_A. And drop Table_B.
                truncateTable(endpoint_url, region, ddb_table_name);
            }
            else {
                createTable(endpoint_url, region, ddb_table_name, key_schema, ReadCapacityUnits, WriteCapacityUnits);
            }
            dataset.write().format("dynamodb").mode("append").option("tableName", ddb_table_name).options(dfoptions).save();

        }
        else if ("upsert".equalsIgnoreCase(mode))
        {
            LOGGER.info("Writing to DynamoDB in upsert mode");
            dataset.write().format("dynamodb").mode("append").option("tableName", ddb_table_name).option("update", true).options(dfoptions).save();
        }
        else if ("insert".equalsIgnoreCase(mode))
        {
            LOGGER.info("Writing to DynamoDB in insert only mode");
            dataset.write().format("dynamodb").mode("append").option("tableName", ddb_table_name).options(dfoptions).save();
        }

    }
}

