
package io.debezium.server.iceberg;

import java.util.Map;

import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;

import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

// S3-only client factory, registered via the "s3.client-factory-impl" property.
// Iceberg consults it exclusively for S3FileIO's client, so Glue/KMS/DynamoDB
// client construction is never affected.

// It builds the standard S3 client and wraps it to treat NoSuchKey-on-delete as
// success: AWS reports deletes of missing keys as successful, while GCS's
// S3-interop returns 404 NoSuchKey, and Iceberg expects the AWS behaviour (e.g.
// its cleanup of empty writer files that were never uploaded). Other errors,
// including NoSuchBucket, still propagate.

public class OlakeS3ClientFactory implements S3FileIOAwsClientFactory {

    private AwsClientFactory delegate;

    @Override
    public void initialize(Map<String, String> properties) {
        this.delegate = AwsClientFactories.from(properties);
    }

    @Override
    public S3Client s3() {
        return new DeleteTolerantS3Client(delegate.s3());
    }

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
}
