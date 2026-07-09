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
import software.amazon.awssdk.services.s3.S3Client;

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
        return delegate.s3();
    }

    @Override
    public GlueClient glue() {
        String glueAccessKey = props.get("glue.access-key-id");
        String glueSecretKey = props.get("glue.secret-access-key");

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
