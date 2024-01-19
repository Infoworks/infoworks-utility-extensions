package com.infoworks;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SpannerDbClient {

    static DatabaseClient getDbClient(String projectId, String instance,String database, String credPath) {
        ServiceAccountCredentials sourceCredentials;
        try {
            sourceCredentials = ServiceAccountCredentials
                    .fromStream(Files.newInputStream(Paths.get(credPath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        com.google.api.gax.grpc.InstantiatingGrpcChannelProvider.Builder grpcChannelprovider = InstantiatingGrpcChannelProvider.newBuilder().setMaxInboundMessageSize(1000000000);
        InstantiatingGrpcChannelProvider channelProvider = grpcChannelprovider.build();
        SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(projectId).setCredentials(sourceCredentials).setChannelProvider(channelProvider);
        builder
                .getSpannerStubSettingsBuilder()
                .executeSqlSettings()
                // Configure which errors should be retried.
                .setRetryableCodes(Code.DEADLINE_EXCEEDED, Code.UNAVAILABLE)
                .setRetrySettings(
                        RetrySettings.newBuilder()
                                // Configure retry delay settings.
                                // The initial amount of time to wait before retrying the request.
                                .setInitialRetryDelay(Duration.ofMillis(500))
                                // The maximum amount of time to wait before retrying. I.e. after this value is
                                // reached, the wait time will not increase further by the multiplier.
                                .setMaxRetryDelay(Duration.ofSeconds(64))
                                // The previous wait time is multiplied by this multiplier to come up with the next
                                // wait time, until the max is reached.
                                .setRetryDelayMultiplier(1.5)

                                // Configure RPC and total timeout settings.
                                // Timeout for the first RPC call. Subsequent retries will be based off this value.
                                .setInitialRpcTimeout(Duration.ofSeconds(60))
                                // The max for the per RPC timeout.
                                .setMaxRpcTimeout(Duration.ofSeconds(60))
                                // Controls the change of timeout for each retry.
                                .setRpcTimeoutMultiplier(1.0)
                                // The timeout for all calls (first call + all retries).
                                .setTotalTimeout(Duration.ofSeconds(60))
                                .build());


        SpannerOptions options = builder.build();
//        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId)
//                .setCredentials(sourceCredentials).build();
        Spanner spanner = options.getService();
        System.out.println("Getting the Spanner database connection");
        return spanner.getDatabaseClient(
                DatabaseId.of(projectId, instance, database));
    }

    public static void main(String[] args) {
        SpannerDbClient.getDbClient("gcp-cs-shared-resources","google-spanner-poc","finance-db","/tmp/spanner_svc_account.json");
    }

}
