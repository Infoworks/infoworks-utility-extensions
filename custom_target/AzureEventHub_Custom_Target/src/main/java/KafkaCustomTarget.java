public class KafkaCustomTarget implements io.infoworks.awb.extensions.api.spark.SparkCustomTarget {
    org.apache.spark.sql.SparkSession spark;
    io.infoworks.awb.extensions.api.spark.UserProperties properties;
    io.infoworks.awb.extensions.api.spark.ProcessingContext processingContext;
    private org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KafkaCustomTarget.class);

    public void writeToTarget(org.apache.spark.sql.Dataset dataset) throws io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException {
        java.util.Map<String, String> ehConf=new java.util.HashMap<>();
        String topicName = this.properties.getValue("topic_name");
        String bootstrapServers = this.properties.getValue("bootstrap_servers");
        String EhSasl = String.format("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://.servicebus.windows.net/;SharedAccessKeyName=;SharedAccessKey=\";'");
        String SharedAccessKeySecretName = this.properties.getValue("shared_access_key_secret");
        String SharedAccessKeyName = this.properties.getValue("shared_access_key_name");
        String SharedAccessKey = this.properties.getValue("shared_access_key");
        if (SharedAccessKeySecretName != null && SharedAccessKey == null){
            SharedAccessKey = this.processingContext.getAdditionalConf().get("event_hub_secret");
        }
        com.microsoft.azure.eventhubs.ConnectionStringBuilder connectionStringBuilder = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
                .setEventHubName(topicName)
                .setSasKeyName(SharedAccessKeyName)
                .setSasKey(SharedAccessKey);
        String connectionString = connectionStringBuilder.toString();
        connectionString = org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString);
        System.out.println(String.format("connectionString : %s",connectionString));
        String batchSize = (this.properties.getValue("batch_size") == null) ? "5000" : this.properties.getValue("batch_size");
        String requestTimeoutMs = (this.properties.getValue("request_timeout_ms") == null) ? "5000" : this.properties.getValue("request_timeout_ms");
        ehConf.put("eventhubs.connectionString",connectionString);
        LOGGER.info("Writing data to target");
        String[] columnNames = dataset.columns();
        // Create an array of Column objects
        org.apache.spark.sql.Column[] columns = new org.apache.spark.sql.Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = dataset.col(columnNames[i]);
        }
        // Use struct and to_json to create the new "body" column
        dataset = dataset.withColumn("body", org.apache.spark.sql.functions.to_json(org.apache.spark.sql.functions.struct(columns)));
        dataset.show();
        String operationTimeoutDurationSeconds = (this.properties.getValue("operation_timeout") == null) ? "1000" : this.properties.getValue("operation_timeout");
        // Setting the operation timeout to 1000 seconds
        java.time.Duration operationTimeoutDuration = java.time.Duration.ofSeconds(Integer.parseInt(operationTimeoutDurationSeconds));
        ehConf.put("eventhubs.operationTimeout", formatDuration(operationTimeoutDuration));
        System.out.println("Event Hubs Configuration:");
        System.out.println("Operation Timeout: " + ehConf.get("eventhubs.operationTimeout"));
        dataset.write()
        .format("eventhubs")
        .options(ehConf)
        .option("checkpointLocation", String.format("./checkpoint/%s/", topicName))
        .save();
        LOGGER.info("Completed writing data to target");
        long rows_written = dataset.count();
        LOGGER.info("Completed writing data to target");
        LOGGER.info(String.format("Completed writing %d rows to Target",rows_written));
    }
    public void initialiseContext(org.apache.spark.sql.SparkSession sparkSession, io.infoworks.awb.extensions.api.spark.UserProperties userProperties,
                                  io.infoworks.awb.extensions.api.spark.ProcessingContext processingContext) {
        this.spark = sparkSession;
        this.properties = userProperties;
        this.processingContext = processingContext;
        // In this transformation we don't have any use for the processing context
    }
    private static String formatDuration(java.time.Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutes() % 60;
        long seconds = duration.getSeconds() % 60;
        return String.format("PT%02dH%02dM%02dS", hours, minutes, seconds);
    }
}

