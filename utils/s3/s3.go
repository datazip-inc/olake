package s3

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/objstorage/s3client"
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

	client, err := s3client.NewS3Client(ctx, s3client.Config{
		Region:          os.Getenv(constants.EnvS3Region),
		AccessKeyID:     os.Getenv(constants.EnvS3AccessKeyID),
		SecretAccessKey: os.Getenv(constants.EnvS3SecretAccessKey),
		SessionToken:    os.Getenv(constants.EnvS3SessionToken),
		Endpoint:        os.Getenv(constants.EnvS3Endpoint),
	})
	if err != nil {
		return err
	}

	s3Client = client
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
