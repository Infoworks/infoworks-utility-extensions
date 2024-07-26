package io.infoworks;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
public class AzureEventHubCustomTargetTest implements SparkCustomTarget {
    org.apache.spark.sql.SparkSession spark;

    private org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(AzureEventHubCustomTargetTest.class);

    private static SparkSession getSparkSession() {
        return SparkSession.builder().config("spark.master", "local").getOrCreate();
    }

    public void writeToTarget(org.apache.spark.sql.Dataset dataset) throws io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException {
        String TOPIC = "";
        String BOOTSTRAP_SERVERS = ".servicebus.windows.net:9093";
        String EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://.servicebus.windows.net/;SharedAccessKeyName=;SharedAccessKey=\";"; //pragma: allowlist secret
        System.out.println(EH_SASL);
        LOGGER.info("Writing to target");
//        dataset.write()
//        .format("kafka")
//        .option("kafka.sasl.mechanism", "PLAIN")
//        .option("kafka.security.protocol", "SASL_SSL")
//        .option("kafka.sasl.jaas.config", EH_SASL)
//        .option("kafka.batch.size", 5000)
//        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
//        .option("kafka.request.timeout.ms", 120000)
//        .option("topic", TOPIC)
//        .option("checkpointLocation", String.format("./checkpoint/%s/", topic))
//        .save();
//        LOGGER.info("Completed {} to target", mode);
    }

    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties,
                                  ProcessingContext processingContext) {
        this.spark = sparkSession;
        // In this transformation we don't have any use for the processing context
    }


    public static void main(String[] args) {
        java.util.Map<String, String> userProps = new java.util.HashMap<>();

        userProps.put("topic", "employee");
        userProps.put("bootstrap_servers", "ATTEhub.servicebus.windows.net:9093");
        String SharedAccessKeyName = "sendPolicy";
        userProps.put("shared_access_key_name", SharedAccessKeyName);
        String SharedAccessKey = "";
        userProps.put("shared_access_key", SharedAccessKey);
        String EH_SASL = String.format("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://.servicebus.windows.net/;SharedAccessKeyName=%s;SharedAccessKey=%s\";",userProps.get("shared_access_key_name"),userProps.get("shared_access_key")); //pragma: allowlist secret
        System.out.println(EH_SASL);
        userProps.put("eh_sasl", EH_SASL);
        UserProperties userProperties = new UserProperties(userProps);
        SparkSession sparkSession = getSparkSession();
        AzureEventHubCustomTargetTest azureEventHubCustomTargetTest = new AzureEventHubCustomTargetTest();
        azureEventHubCustomTargetTest.initialiseContext(sparkSession, userProperties,null);
//        Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("/Users/nitin.bs/PycharmProjects/infoworks-extensions/utilities/tdbank_poc/sample_data.csv");
//        dataset.show();
        System.out.println(String.format("user_properties: %s",userProps.toString()));
        System.out.println(userProps);
    }
}