package com.infoworks;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class SpannerForEachPartition implements Serializable {

    static DatabaseClient dbClient;
    public SpannerForEachPartition(String projectId, String instanceId, String database, String credentialPath, LongAccumulator obj) {
        createSingleClient(projectId,instanceId,database,credentialPath,obj);
    }

    public void createSingleClient(String projectId, String instanceId, String database, String credentialPath, LongAccumulator obj) {
        if (SpannerForEachPartition.dbClient != null) {
            System.out.println("dbclient is not null hence using the existing dbclient");
            return;
        }
        obj.add(1);
        SpannerForEachPartition.dbClient = SpannerDbClient.getDbClient(projectId,instanceId,database,credentialPath);
    }
    static  ForeachPartitionFunction<Row> upsert(String tableName, LinkedHashMap<String, String> sqlColumnsDataMapping, String projectId, String instanceId, String database, String credentialPath, LongAccumulator obj) {
        // SpannerForEachPartition spanner_obj = new SpannerForEachPartition(projectId,instanceId,database,credentialPath,obj);
        return new ForeachPartitionFunction<Row>() {
            @Override
            public void call(java.util.Iterator<Row> iterator) throws Exception {
                // DatabaseClient dbClient = SpannerDbClient.getDbClient(projectId,instanceId,database,credentialPath);
                SpannerForEachPartition spanner_obj = new SpannerForEachPartition(projectId,instanceId,database,credentialPath,obj);
                // spanner_obj.createSingleClient(projectId,instanceId,database,credentialPath,obj);
                List<Mutation> mutations = null;
                mutations = new ArrayList<>();
                Set<String> keys = sqlColumnsDataMapping.keySet();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    Mutation.WriteBuilder mutation_builder = Mutation.newInsertOrUpdateBuilder(tableName);
                    for (String key : keys) {
                        String datatype = sqlColumnsDataMapping.get(key);
                        datatype = datatype.split("[(]")[0];
                        switch (datatype) {
                            case "string":
                                mutation_builder.set(key).to((String) row.getAs(key));
                                break;
                            case "double":
                                mutation_builder.set(key).to((Double) row.getAs(key));
                                break;
                            case "integer":
                                mutation_builder.set(key).to((Integer) row.getAs(key));
                                break;
                            case "float":
                                mutation_builder.set(key).to(Value.float64(Double.valueOf(row.getAs(key).toString())));
                                break;
                            case "long":
                                mutation_builder.set(key).to((Long) row.getAs(key));
                                break;
                            case "short":
                                mutation_builder.set(key).to((Short) row.getAs(key));
                                break;
                            case "byte":
                                mutation_builder.set(key).to((Byte) row.getAs(key));
                                break;
                            case "decimal":
                                mutation_builder.set(key).to((java.math.BigDecimal) row.getAs(key));
                                break;
                            case "boolean":
                                mutation_builder.set(key).to((Boolean) row.getAs(key));
                                break;
                            case "date":
                                mutation_builder.set(key).to(com.google.cloud.Date.fromJavaUtilDate((java.util.Date) row.getAs(key)));
                                break;
                            case "timestamp":
                                String temp_key = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(row.getAs(key));
                                mutation_builder.set(key).to(com.google.cloud.Timestamp.parseTimestamp(temp_key));
                                break;
                        }

                    }
                    mutations.add(mutation_builder.build());
                }
                SpannerForEachPartition.dbClient.write(mutations);
            }
        };
    }
}
