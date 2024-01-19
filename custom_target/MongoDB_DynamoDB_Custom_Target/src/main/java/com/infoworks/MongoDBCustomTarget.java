package com.infoworks;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.awb.extensions.exceptions.ExtensionsProcessingException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import io.infoworks.platform.common.encryption.AES256;
import sun.misc.BASE64Decoder;

public class MongoDBCustomTarget implements SparkCustomTarget {
    private SparkSession spark;
    private UserProperties userProperties;
    static org.apache.log4j.Logger LOGGER = Logger.getLogger(MongoDBCustomTarget.class);


    @Override
    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties, ProcessingContext processingContext) {
        this.spark = sparkSession;
        this.userProperties  = userProperties;
    }

    private void truncateTable(String uri, String db_name, String collection_name) {
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(db_name);
            MongoCollection<Document> collection = database.getCollection(collection_name);
            try {
                DeleteResult result = collection.deleteMany(new Document());
                System.out.println("Deleted document count: " + result.getDeletedCount());
                mongoClient.close();
            } catch (MongoException me) {
                System.err.println("Unable to delete due to an error: " + me);
            }

        }

    }

    private void createIndex(String uri, String db_name, String collection_name, String key) {
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(db_name);
            MongoCollection<Document> collection = database.getCollection(collection_name);
            try {
                collection.createIndex(Indexes.ascending(key));
                System.out.println("Index created successfully");
                mongoClient.close();
            } catch (MongoException me) {
                System.err.println("Unable to create Index due to an error: " + me);
            }

        }

    }

    @Override
    public void writeToTarget(Dataset dataset) throws ExtensionsProcessingException {

        String uri = userProperties.getValue("uri");
        String uri_updated = uri;
        String encryptedPassword = userProperties.getValue("encryptedPassword");
        try {
            String password = AES256.decryptData((new BASE64Decoder()).decodeBuffer(encryptedPassword));
            uri_updated=uri.replace("***",password);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(uri_updated);
        String database = userProperties.getValue("database");
        String collection = userProperties.getValue("collection");
        String merge_key = userProperties.getValue("merge_key");
        String mode = userProperties.getValue("mode") == null ? "overwrite" : userProperties.getValue("mode");
        String table_exists = userProperties.getValue("table_exists") == null ? "false" : userProperties.getValue("table_exists");
        String data_has_id = userProperties.getValue("data_has_id") == null ? "false" : userProperties.getValue("data_has_id");
        String index_key = userProperties.getValue("index_key");


        if ("append".equalsIgnoreCase(mode))
        {
            LOGGER.info("Writing to MongoDB in append mode");
            dataset.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", uri_updated).option("database",database).option("collection", collection).save();
        }
        else if ("overwrite".equalsIgnoreCase(mode))
        {
            LOGGER.info("Writing to MongoDB in overwrite mode");
            if ("true".equalsIgnoreCase(table_exists))
            {
                truncateTable(uri_updated, database, collection);
                dataset.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", uri_updated).option("database",database).option("collection", collection).save();

            }
            else dataset.write().format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("uri", uri_updated).option("database",database).option("collection", collection).save();

        }
        else if ("merge".equalsIgnoreCase(mode))
        {
            if ("true".equalsIgnoreCase(data_has_id)) dataset.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", uri_updated).option("database",database).option("collection", collection).save();
            else {
                if (merge_key != null) {
                    Dataset tempDataset = spark.read().format("com.mongodb.spark.sql.DefaultSource").option("uri", uri_updated).option("database",database).option("collection", collection).load();
                    List<String> col_list = Arrays.asList(merge_key.split(","));
                    Seq<String> scalaSeq = JavaConverters.asScalaIteratorConverter(col_list.iterator()).asScala().toSeq();
                    // Updates
                    Dataset updates_joinedds = dataset.alias("inp").join(tempDataset.alias("temp"), scalaSeq, "inner").select("temp._id","inp.*");
                    updates_joinedds.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", uri_updated).option("database",database).option("collection", collection).save();
                    // Inserts
                    Dataset inserts_joinedds = dataset.alias("inp").join(tempDataset.alias("temp"), scalaSeq, "leftanti");
                    inserts_joinedds.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", uri_updated).option("database",database).option("collection", collection).save();

                }
                else throw new java.lang.RuntimeException("Please pass the merge key");
            }
            LOGGER.info("Writing to MongoDB in merge mode");
        }

        if (index_key != null) {
            createIndex(uri_updated, database, collection, index_key);
        }

    }
}

