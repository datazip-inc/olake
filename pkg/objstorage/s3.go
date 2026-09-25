package objstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/datazip-inc/olake/utils/logger"
)

// S3Config configures a Store backed by AWS S3 or any S3-compatible service.
type S3Config struct {
	Bucket          string
	Region          string
	AccessKeyID     string // optional; must be set together with SecretAccessKey
	SecretAccessKey string
	Endpoint        string // optional; set for S3-compatible services (MinIO, GCS interop, R2, ...)
}

// s3Store implements Store on top of aws-sdk-go-v2.
type s3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store builds a Store for AWS S3 or, when Endpoint is set, an
// S3-compatible endpoint. Static credentials are used when both key fields
// are provided; otherwise the AWS default credential chain applies.
func NewS3Store(ctx context.Context, cfg S3Config) (Store, error) {
	configOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	// Use static credentials if provided, otherwise fall back to default credential chain
	// Default chain includes: IAM roles, instance profiles, environment variables, shared config
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		logger.Info("Using static credentials for S3 authentication")
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				"",
			),
		))
	} else {
		logger.Info("Using default credential chain (IAM role, instance profile, env vars, or shared config)")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var client *s3.Client
	if cfg.Endpoint != "" {
		// SigV4 signing requires a non-empty region; S3-compatible services accept
		// any value. Applied after LoadDefaultConfig so regions resolved from the
		// environment or shared config still win.
		if awsCfg.Region == "" {
			awsCfg.Region = "us-east-1"
		}
		logger.Infof("Connecting to S3-compatible endpoint: %s", cfg.Endpoint)
		client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // Required for MinIO and some S3-compatible services
			// SDK-default CRC32 integrity checksums (service/s3 >= v1.73) are not
			// implemented by several S3-compatible services (R2, older MinIO, GCS interop)
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
	} else {
		logger.Infof("Connecting to AWS S3 in region: %s", cfg.Region)
		client = s3.NewFromConfig(awsCfg)
	}

	return &s3Store{client: client, bucket: cfg.Bucket}, nil
}

// Check verifies the bucket exists and is accessible.
func (s *s3Store) Check(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	return err
}

// List walks all objects under prefix, handling ListObjectsV2 pagination.
func (s *s3Store) List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error {
	var continuationToken *string
	pageCount := 0

	for {
		// Abort promptly between pages if the caller canceled (long listings
		// can span many pages before the SDK call itself observes the context).
		if err := ctx.Err(); err != nil {
			return err
		}

		pageCount++
		result, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return err
		}

		logger.Debugf("Processing S3 list page %d (%d objects in this page)", pageCount, len(result.Contents))

		for _, obj := range result.Contents {
			info := ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
				ETag:         strings.Trim(aws.ToString(obj.ETag), "\""),
			}
			if err := fn(info); err != nil {
				return err
			}
		}

		if !aws.ToBool(result.IsTruncated) {
			logger.Debugf("Completed S3 listing: processed %d pages", pageCount)
			return nil
		}
		continuationToken = result.NextContinuationToken
	}
}

// Open returns the full object body.
func (s *s3Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return result.Body, nil
}

// OpenRange returns object bytes [offset, offset+length) using an S3 ranged GET.
func (s *s3Store) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid range: offset=%d, length=%d", offset, length)
	}
	// S3 range format: "bytes=start-end" (inclusive)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return result.Body, nil
}

// wrapNotFound annotates missing-key errors with ErrNotFound while preserving
// the original SDK error text. Provider knowledge (S3 error codes) stays in
// this layer so callers only ever match errors.Is(err, ErrNotFound).
func wrapNotFound(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" {
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		}
	}
	// Fallback for S3-compatible services whose not-found surfaces without a
	// typed API error code
	if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	return err
}
