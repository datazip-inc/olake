package io.debezium.server.iceberg;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

// for custom glue endpoint credentials
public class OlakeAwsClientFactory implements AwsClientFactory {

    private transient AwsClientFactory delegate;
    private transient Map<String, String> props;

    @Override
    public void initialize(Map<String, String> properties) {
        Map<String, String> p = new HashMap<>();
        if (properties != null) {
            for (Map.Entry<String, String> e : properties.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    p.put(e.getKey(), e.getValue());
                }
            }
        }

        this.props = p;
        this.delegate = AwsClientFactories.defaultFactory();
        this.delegate.initialize(this.props);
    }

    @Override
    public S3Client s3() {
        return new DeleteTolerantS3Client(delegate.s3());
    }

    /**
     * Treats NoSuchKey-on-delete as success: AWS returns 204 for deletes of
     * missing keys, GCS's S3-interop returns 404, and Iceberg expects the AWS
     * behavior (e.g. its cleanup of empty writer files that were never uploaded) .
     */
    private static class DeleteTolerantS3Client extends DelegatingS3Client {
        DeleteTolerantS3Client(S3Client delegate) {
            super(delegate);
        }

        @Override
        public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
            try {
                return super.deleteObject(request);
            } catch (S3Exception e) {
                if (isNoSuchKey(e)) {
                    return DeleteObjectResponse.builder().build();
                }
                throw e;
            }
        }

        private static boolean isNoSuchKey(S3Exception e) {
            if (e instanceof NoSuchKeyException) {
                return true;
            }
            return e.statusCode() == 404 && e.awsErrorDetails() != null && "NoSuchKey".equals(e.awsErrorDetails().errorCode());
        }
    }

    @Override
    public GlueClient glue() {
        String glueAccessKey = props.get("glue.access-key-id");
        String glueSecretKey = props.get("glue.secret-access-key");

        // This factory can be registered purely for its S3 client (custom
        // s3.endpoint). Without explicit glue.* overrides, keep Iceberg's default
        // Glue client rather than building one from S3-oriented settings.
        if (isBlank(glueAccessKey) && isBlank(glueSecretKey) && isBlank(props.get("glue.endpoint")) && isBlank(props.get("glue.region"))) {
            return delegate.glue();
        }

        GlueClientBuilder builder = GlueClient.builder();
        if (!isBlank(glueAccessKey) && !isBlank(glueSecretKey)) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(glueAccessKey, glueSecretKey)
                    )
            );
        }

        // prefer glue.region if set, otherwise fall back to s3.region
        String region = props.get("glue.region");
        if (isBlank(region)) {
             region = props.get("s3.region");
        }
        if (!isBlank(region)) {
            builder.region(Region.of(region));
        }

        String endpoint = props.get("glue.endpoint");
        if (!isBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }

        return builder.build();
    }

    @Override
    public KmsClient kms() {
        return delegate.kms();
    }

    @Override
    public DynamoDbClient dynamo() {
        return delegate.dynamo();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
