package io.debezium.server.iceberg;

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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom Iceberg AwsClientFactory that keeps S3 FileIO credentials separate from Glue catalog credentials.
 *
 * <p>Behavior:
 * <ul>
 *   <li>S3 client: delegated to Iceberg's default factory (uses standard Iceberg S3 properties like s3.access-key-id)</li>
 *   <li>Glue client: if glue.access-key-id/glue.secret-access-key are provided, use them as static credentials</li>
 *   <li>Glue endpoint: if glue.endpoint is provided, use it as endpointOverride</li>
 * </ul>
 */
public class OlakeAwsClientFactory implements AwsClientFactory {

    private transient AwsClientFactory delegate;
    private transient Map<String, String> props;

    @Override
    public void initialize(Map<String, String> properties) {
        // Iceberg passes a properties map; normalize to String->String
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

        // If no separate Glue creds are provided, fall back to default behavior.
        if (isBlank(glueAccessKey) || isBlank(glueSecretKey)) {
            return delegate.glue();
        }

        GlueClientBuilder builder = GlueClient.builder()
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(glueAccessKey, glueSecretKey)
                        )
                );

        // Region: prefer s3.region (we set it from aws_region in Go)
        String region = props.get("s3.region");
        if (!isBlank(region)) {
            builder.region(Region.of(region));
        }

        // Optional Glue-compatible endpoint override
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

