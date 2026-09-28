// Package s3client builds AWS S3 clients shared by the S3 source driver
// (pkg/objstorage) and S3 job storage (utils/s3). It depends only on the AWS
// SDK so any package can import it without creating an import cycle.
package s3client

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config configures a client for AWS S3 or any S3-compatible service.
type Config struct {
	Region          string // optional; empty defers to the AWS default region resolution
	AccessKeyID     string // optional; must be set together with SecretAccessKey
	SecretAccessKey string
	Endpoint        string // optional; set for S3-compatible services (MinIO, GCS interop, R2, ...)
}

// UsesStaticCredentials reports whether NewS3Client will use the static key pair
// instead of the AWS default credential chain.
func (c Config) UsesStaticCredentials() bool {
	return c.AccessKeyID != "" && c.SecretAccessKey != ""
}

// NewS3Client builds an S3 client. Static credentials are used when both key fields
// are provided; otherwise the AWS default credential chain applies (IAM roles,
// instance profiles, environment variables, shared config).
func NewS3Client(ctx context.Context, cfg Config) (*s3.Client, error) {
	configOpts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		configOpts = append(configOpts, config.WithRegion(cfg.Region))
	}

	if cfg.UsesStaticCredentials() {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.StaticCredentialsProvider{Value: aws.Credentials{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey}},
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if cfg.Endpoint == "" {
		return s3.NewFromConfig(awsCfg), nil
	}

	// SigV4 signing requires a non-empty region; S3-compatible services accept
	// any value. Applied after LoadDefaultConfig so regions resolved from the
	// environment or shared config still win.
	if awsCfg.Region == "" {
		awsCfg.Region = "us-east-1"
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true // Required for MinIO and some S3-compatible services
		// SDK-default CRC32 integrity checksums (service/s3 >= v1.73) are not
		// implemented by several S3-compatible services (R2, older MinIO, GCS interop)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}), nil
}
