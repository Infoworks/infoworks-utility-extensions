package io.infoworks;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import org.apache.spark.eventhubs.EventHubsUtils;
import java.time.Duration;
import java.util.Properties;
import java.util.Enumeration;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;

public class AzureEventHubCustomTarget implements SparkCustomTarget {
    SparkSession spark;
    UserProperties properties;
    ProcessingContext processingContext;
    Properties additionalOptions;
    private Logger LOGGER = LoggerFactory.getLogger(AzureEventHubCustomTarget.class);

    public void writeToTarget(Dataset dataset) throws ExtensionsProcessingException {
        java.util.Map<String, String> ehConf=new java.util.HashMap<>();
        String eventHubName = this.properties.getValue("event_name");
        String eventHubNamespace = this.properties.getValue("namespace");
        eventHubNamespace = eventHubNamespace.replace(".servicebus.windows.net", "");
        eventHubNamespace = eventHubNamespace.replace("sb://", "");
        String SharedAccessKeySecretName = this.properties.getValue("shared_access_key_secret");
        String SharedAccessKeyName = this.properties.getValue("shared_access_key_name");
        String SharedAccessKey = this.properties.getValue("shared_access_key");
        String PartitionKey = this.properties.getValue("partition_key");
        String PartitionId = this.properties.getValue("partition_id");
        if (SharedAccessKeySecretName != null && SharedAccessKey == null) {
            SharedAccessKey = this.processingContext.getAdditionalConf().get("event_hub_secret");
        }
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder()
                .setNamespaceName(eventHubNamespace)
                .setEventHubName(eventHubName)
                .setSasKeyName(SharedAccessKeyName)
                .setSasKey(SharedAccessKey);
        String connectionString = connectionStringBuilder.toString();
        connectionString = EventHubsUtils.encrypt(connectionString);
        LOGGER.info(String.format("connectionString : %s",connectionString));
        ehConf.put("eventhubs.connectionString",connectionString);
        LOGGER.info("Started writing data to target");
        String[] columnNames = dataset.columns();
        // Create an array of Column objects
        Column[] columns = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = dataset.col(columnNames[i]);
        }
        // Use struct and to_json to create the new "body" column
        dataset = dataset.withColumn("body", functions.to_json(functions.struct(columns)));
        LOGGER.info("Encapsulated the source table columns into single body column");
        String operationTimeoutDurationSeconds = (this.properties.getValue("operation_timeout") == null) ? "1000" : this.properties.getValue("operation_timeout");
        // Setting the operation timeout to 1000 seconds
        Duration operationTimeoutDuration = Duration.ofSeconds(Integer.parseInt(operationTimeoutDurationSeconds));
        String receiverTimeoutDurationSeconds = (this.properties.getValue("receiver_timeout") == null) ? "1000" : this.properties.getValue("receiver_timeout");
        // Setting the receiver timeout to 1000 seconds
        Duration receiverTimeoutDuration = Duration.ofSeconds(Integer.parseInt(receiverTimeoutDurationSeconds));
        ehConf.put("eventhubs.operationTimeout", formatDuration(operationTimeoutDuration));
        ehConf.put("eventhubs.receiverTimeout", formatDuration(receiverTimeoutDuration));
        this.additionalOptions = this.getAdditionalOptions(this.properties.getValue("additional_options"));
        Enumeration<?> propertyNames = this.additionalOptions.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String key = (String) propertyNames.nextElement();
            String value = this.additionalOptions.getProperty(key);
            ehConf.put(key,value);
        }
        System.out.println("Event Hubs Configuration:");
        LOGGER.info(String.format("Operation Timeout: %s", ehConf.get("eventhubs.operationTimeout")));
        LOGGER.info(String.format("Receiver Timeout: %s" , ehConf.get("eventhubs.receiverTimeout")));
        LOGGER.info(String.format("ehConf:%s", ehConf.toString()));
        System.out.println(String.format("ehConf:%s",ehConf.toString()));
        if (PartitionKey != null){
            String PartitionKeyExpr = String.format("%s as partitionKey",PartitionKey);
            dataset.selectExpr("body",PartitionKeyExpr)
                    .write()
                    .format("eventhubs")
                    .options(ehConf)
                    .option("checkpointLocation", String.format("./checkpoint/%s/", eventHubName))
                    .save();
        }
        else if (PartitionId != null){
            String PartitionIdExpr = String.format("%s as partitionId",PartitionId);
            dataset.selectExpr("body",PartitionIdExpr)
                    .write()
                    .format("eventhubs")
                    .options(ehConf)
                    .option("checkpointLocation", String.format("./checkpoint/%s/", eventHubName))
                    .save();
        }
        else
            {
        dataset.write()
        .format("eventhubs")
        .options(ehConf)
        .option("checkpointLocation", String.format("./checkpoint/%s/", eventHubName))
        .save();
        }
        long rows_written = dataset.count();
        LOGGER.info("Completed writing data to target");
        LOGGER.info(String.format("Completed writing %d rows to Target",rows_written));
    }
    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties,
                                  ProcessingContext processingContext) {
        this.spark = sparkSession;
        this.properties = userProperties;
        this.processingContext = processingContext;
        // In this transformation we don't have any use for the processing context
    }
    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutes() % 60;
        long seconds = duration.getSeconds() % 60;
        return String.format("PT%02dH%02dM%02dS", hours, minutes, seconds);
    }
    private java.util.Properties getAdditionalOptions(String additionalPropertiesStr) {
        java.util.Properties additionalOptions = new java.util.Properties();
        if (additionalPropertiesStr != null) {
            String[] properties = additionalPropertiesStr.split(";");
            String[] var4 = properties;
            int var5 = properties.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                String property = var4[var6];
                String[] keyValuePair = property.split("=");
                if (keyValuePair.length == 2) {
                    additionalOptions.setProperty(keyValuePair[0], keyValuePair[1]);
                }
            }
        }
        return additionalOptions;
    }
}

