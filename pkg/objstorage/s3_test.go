package objstorage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// isolateAWSEnv clears AWS environment configuration so factory tests are
// deterministic regardless of the host's AWS setup. Client construction does
// no network I/O (IMDS is disabled explicitly).
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent-aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent-aws-credentials")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestWrapNotFound(t *testing.T) {
	// Typed S3 API error codes map to ErrNotFound, original text preserved
	typed := fmt.Errorf("operation error S3: GetObject, %w",
		&smithy.GenericAPIError{Code: "NoSuchKey", Message: "The specified key does not exist."})
	err := wrapNotFound(typed)
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorContains(t, err, "The specified key does not exist.")

	headStyle := &smithy.GenericAPIError{Code: "NotFound", Message: "Not Found"}
	require.ErrorIs(t, wrapNotFound(headStyle), ErrNotFound)

	// Untyped errors mentioning the S3 codes still classify (S3-compatible fallback)
	require.ErrorIs(t, wrapNotFound(errors.New("api error NoSuchKey: no such object")), ErrNotFound)

	// Everything else passes through unchanged
	denied := &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}
	require.NotErrorIs(t, wrapNotFound(denied), ErrNotFound)
	plain := errors.New("connection reset")
	require.Equal(t, plain, wrapNotFound(plain))
}

func TestNewS3StoreEndpointMode(t *testing.T) {
	isolateAWSEnv(t)

	store, err := NewS3Store(context.Background(), S3Config{
		Bucket:          "test-bucket",
		Endpoint:        "http://localhost:9000",
		AccessKeyID:     "admin",
		SecretAccessKey: "password",
	})
	require.NoError(t, err)

	impl, ok := store.(*s3Store)
	require.True(t, ok)
	require.Equal(t, "test-bucket", impl.bucket)

	opts := impl.client.Options()
	require.True(t, opts.UsePathStyle, "endpoint mode must force path-style addressing")
	require.Equal(t, "http://localhost:9000", aws.ToString(opts.BaseEndpoint))
	require.Equal(t, aws.RequestChecksumCalculationWhenRequired, opts.RequestChecksumCalculation)
	require.Equal(t, aws.ResponseChecksumValidationWhenRequired, opts.ResponseChecksumValidation)
	// With no region configured anywhere, endpoint mode falls back to us-east-1 for signing
	require.Equal(t, "us-east-1", opts.Region)
}

func TestNewS3StoreEndpointModeKeepsExplicitRegion(t *testing.T) {
	isolateAWSEnv(t)

	store, err := NewS3Store(context.Background(), S3Config{
		Bucket:          "test-bucket",
		Region:          "eu-west-1",
		Endpoint:        "http://localhost:9000",
		AccessKeyID:     "admin",
		SecretAccessKey: "password",
	})
	require.NoError(t, err)

	opts := store.(*s3Store).client.Options()
	require.Equal(t, "eu-west-1", opts.Region)
}

func TestNewS3StoreAWSMode(t *testing.T) {
	isolateAWSEnv(t)

	store, err := NewS3Store(context.Background(), S3Config{
		Bucket:          "test-bucket",
		Region:          "ap-south-1",
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
	})
	require.NoError(t, err)

	opts := store.(*s3Store).client.Options()
	require.Equal(t, "ap-south-1", opts.Region)
	require.False(t, opts.UsePathStyle, "AWS mode must keep virtual-hosted addressing")
	require.Nil(t, opts.BaseEndpoint)
	// AWS-path behavior stays byte-identical: no checksum overrides
	require.NotEqual(t, aws.RequestChecksumCalculationWhenRequired, opts.RequestChecksumCalculation)
	require.NotEqual(t, aws.ResponseChecksumValidationWhenRequired, opts.ResponseChecksumValidation)
}
