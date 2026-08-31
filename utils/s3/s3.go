package s3

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/constants"
)

var (
	s3Client  *awss3.Client
	JobBucket string
	JobPrefix string
)

// IsS3Job reports whether OLAKE_STORAGE_MODE is s3.
func IsS3Job() bool {
	return os.Getenv(constants.EnvStorageMode) == constants.StorageModeS3
}

// Init initializes the shared S3 client when storage mode is S3. No-op for NFS.
func Init(ctx context.Context) error {
	if !IsS3Job() {
		return nil
	}

	configOpts := []func(*config.LoadOptions) error{}
	if region := os.Getenv(constants.EnvS3Region); region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	accessKey := os.Getenv(constants.EnvS3AccessKeyID)
	secretKey := os.Getenv(constants.EnvS3SecretAccessKey)
	if accessKey != "" && secretKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, os.Getenv(constants.EnvS3SessionToken)),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %s", err)
	}

	var s3Opts []func(*awss3.Options)
	if endpoint := os.Getenv(constants.EnvS3Endpoint); endpoint != "" {
		// Path-style is required for MinIO and other S3-compatible endpoints.
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client = awss3.NewFromConfig(awsCfg, s3Opts...)
	return nil
}

func getS3Client() (*awss3.Client, error) {
	if s3Client == nil {
		return nil, fmt.Errorf("s3 storage not initialized")
	}
	return s3Client, nil
}

// GetObject downloads an object and records the job prefix from the first key.
func GetObject(ctx context.Context, bucket, key string) (*awss3.GetObjectOutput, error) {
	if JobBucket == "" {
		JobBucket = bucket
		JobPrefix = path.Dir(key)
	}

	client, err := getS3Client()
	if err != nil {
		return nil, err
	}
	return client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

// UploadFile uploads a local file to bucket/key.
func UploadFileToS3(ctx context.Context, localPath, bucket, key string) error {
	client, err := getS3Client()
	if err != nil {
		return err
	}

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %s", localPath, err)
	}
	defer file.Close()

	// Upload the file to S3.
	_, err = client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload s3://%s/%s: %s", bucket, key, err)
	}

	return nil
}
