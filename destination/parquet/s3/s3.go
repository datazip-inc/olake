package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Bucket     string
	Region     string
	AccessKey  string
	SecretKey  string
	Prefix     string
	S3Endpoint string
}

// Store is the implementation of the ObjectStore interface for S3.
type Store struct {
	client   s3iface.S3API
	uploader s3manageriface.UploaderAPI
	bucket   string
	prefix   string
	gcs      bool
}

// New creates a new AWS S3 client with shared key credential and returns a new Store.
func New(cfg Config) (*Store, error) {
	awsCfg := aws.Config{Region: aws.String(cfg.Region)}
	if cfg.S3Endpoint != "" {
		awsCfg.Endpoint = aws.String(cfg.S3Endpoint)
		awsCfg.S3ForcePathStyle = aws.Bool(true)
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, "")
	}
	sess, err := session.NewSession(&awsCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}
	return &Store{
		client:   awss3.New(sess),
		uploader: s3manager.NewUploader(sess),
		bucket:   cfg.Bucket,
		prefix:   strings.Trim(cfg.Prefix, "/"),
		gcs:      strings.Contains(cfg.S3Endpoint, "googleapis.com"),
	}, nil
}

func NewWithClient(client s3iface.S3API, uploader s3manageriface.UploaderAPI, bucket, prefix string) *Store {
	return &Store{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
		prefix:   strings.Trim(prefix, "/"),
	}
}

func (s *Store) SetPrefix(prefix string) {
	s.prefix = strings.TrimSuffix(prefix, "/")
}

func (s *Store) Kind() string {
	return "s3"
}

func (s *Store) ObjectKey(relativePath string) string {
	return path.Join(s.prefix, relativePath)
}

func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObjectWithContext(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *Store) UploadFile(ctx context.Context, key string, file *os.File) error {
	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	return err
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	if s.gcs {
		return s.deletePrefixIndividually(ctx, prefix)
	}
	keys, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// GCP: no DeleteObjects API. Concurrent single deletes; GCS throttles at HTTP 429
// (~5000 mutations/sec/bucket). Same behavior as the old clearS3Files googleapis branch.
func (s *Store) deletePrefixIndividually(ctx context.Context, prefix string) error {
	var pageErr error
	listErr := utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(_ context.Context) error {
		pageErr = nil
		return s.client.ListObjectsPagesWithContext(ctx, &awss3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		}, func(page *awss3.ListObjectsOutput, _ bool) bool {
			pageKeys := make([]string, 0, len(page.Contents))
			for _, obj := range page.Contents {
				pageKeys = append(pageKeys, aws.StringValue(obj.Key))
			}
			if len(pageKeys) == 0 {
				return true
			}
			concurrency := min(runtime.GOMAXPROCS(0)*4, len(pageKeys))
			if pageErr = utils.Concurrent(ctx, pageKeys, concurrency, func(_ context.Context, key string, _ int) error {
				return utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(_ context.Context) error {
					return s.Delete(ctx, key)
				})
			}); pageErr != nil {
				return false
			}
			return true
		})
	})
	if listErr != nil {
		return fmt.Errorf("failed to list objects for prefix %s: %w", prefix, listErr)
	}
	return pageErr
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObjectWithContext(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	err := s.client.ListObjectsPagesWithContext(ctx, &awss3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}, func(page *awss3.ListObjectsOutput, _ bool) bool {
		for _, obj := range page.Contents {
			keys = append(keys, aws.StringValue(obj.Key))
		}
		return true
	})
	return keys, err
}

func (s *Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	escaped := strings.ReplaceAll(url.PathEscape(srcKey), "%2F", "/")
	_, err := s.client.CopyObjectWithContext(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(s.bucket + "/" + escaped),
	})
	return err
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObjectWithContext(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *Store) IsNotFound(err error) bool {
	var awsErr awserr.Error
	return errors.As(err, &awsErr) && (awsErr.Code() == awss3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound")
}

// IsRateLimitError is used to check if an error is a throttle response
// AWS S3 returns HTTP 503 for throttling (SlowDown / ServiceUnavailable).
// GCP Cloud Storage returns HTTP 429 (Too Many Requests).
//
// For batch delete operations, errors are wrapped in s3manager.BatchError which does NOT
// implement awserr.RequestFailure directly. The actual RequestFailure is nested inside
// BatchError.Errors[].OrigErr, so we must inspect those inner errors as well.
func (s *Store) IsRateLimitError(err error) bool {
	isThrottled := func(target error) bool {
		var rf awserr.RequestFailure
		return errors.As(target, &rf) && (rf.StatusCode() == 429 || rf.StatusCode() == 503)
	}
	if isThrottled(err) {
		return true
	}
	var batchErr awserr.Error
	if errors.As(err, &batchErr) {
		return isThrottled(batchErr.OrigErr())
	}
	return false
}
