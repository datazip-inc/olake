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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/datazip-inc/olake/pkg/objstorage/s3client"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// clientAPI is the slice of S3 the store uses. *awss3.Client implements it.
type clientAPI interface {
	PutObject(ctx context.Context, params *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *awss3.DeleteObjectInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *awss3.DeleteObjectsInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error)
	CopyObject(ctx context.Context, params *awss3.CopyObjectInput, optFns ...func(*awss3.Options)) (*awss3.CopyObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

// uploaderAPI uploads an object, using multipart when the body is large.
type uploaderAPI interface {
	Upload(ctx context.Context, input *awss3.PutObjectInput) error
}

type sdkUploader struct {
	inner *manager.Uploader
}

func (u sdkUploader) Upload(ctx context.Context, input *awss3.PutObjectInput) error {
	_, err := u.inner.Upload(ctx, input)
	return err
}

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
	client   clientAPI
	uploader uploaderAPI
	bucket   string
	prefix   string
	gcs      bool
	Kind     string
}

// New creates a new AWS S3 client.
func New(cfg Config) (*Store, error) {
	client, err := s3client.NewS3Client(context.Background(), s3client.Config{
		Region:          cfg.Region,
		AccessKeyID:     cfg.AccessKey,
		SecretAccessKey: cfg.SecretKey,
		Endpoint:        cfg.S3Endpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS S3 client: %w", err)
	}
	return &Store{
		client:   client,
		uploader: sdkUploader{inner: manager.NewUploader(client)},
		bucket:   cfg.Bucket,
		prefix:   strings.Trim(cfg.Prefix, "/"),
		gcs:      strings.Contains(cfg.S3Endpoint, "googleapis.com"),
		Kind:     "s3",
	}, nil
}

func NewWithClient(client clientAPI, uploader uploaderAPI, bucket, prefix string) *Store {
	return &Store{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
		prefix:   strings.Trim(prefix, "/"),
		Kind:     "s3",
	}
}

// WithPrefix returns a store that shares this store's clients and bucket and applies prefix
// with the same trim as New. gcs is copied so a GCS store stays on individual deletes.
func (s *Store) WithPrefix(prefix string) *Store {
	next := NewWithClient(s.client, s.uploader, s.bucket, prefix)
	next.gcs = s.gcs
	next.Kind = s.Kind
	return next
}

func (s *Store) ObjectKey(relativePath string) string {
	return path.Join(s.prefix, relativePath)
}

func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *Store) UploadFile(ctx context.Context, key string, file *os.File) error {
	return s.uploader.Upload(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   file,
	})
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	if s.gcs {
		return s.deletePrefixIndividually(ctx, prefix)
	}
	err := utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(ctx context.Context) error {
		return s.deletePrefixBatch(ctx, prefix)
	})
	if err != nil {
		logger.Warnf("batch delete failed for prefix %s, falling back to individual deletes: %v", prefix, err)
		if fallbackErr := s.deletePrefixIndividually(ctx, prefix); fallbackErr != nil {
			return fmt.Errorf("batch delete failed: %v, fallback individual delete also failed: %w", err, fallbackErr)
		}
	}
	return nil
}

// deletePrefixBatch removes every object under prefix with DeleteObjects. A page is at most
// 1000 keys, which is also the DeleteObjects limit, so each page is one request.
func (s *Store) deletePrefixBatch(ctx context.Context, prefix string) error {
	return s.forEachPage(ctx, prefix, func(keys []string) error {
		objects := make([]types.ObjectIdentifier, len(keys))
		for i, key := range keys {
			objects[i] = types.ObjectIdentifier{Key: aws.String(key)}
		}
		out, err := s.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
		if len(out.Errors) > 0 {
			return fmt.Errorf("s3 delete objects: %s", aws.ToString(out.Errors[0].Message))
		}
		return nil
	})
}

// errStopPage ends a list without treating the page's delete failure as a list error.
// The list retry must not run again, or it would delete keys the page already removed.
var errStopPage = errors.New("stop object page")

// GCP: no DeleteObjects API. Concurrent single deletes; GCS throttles at HTTP 429
// (~5000 mutations/sec/bucket). Same behavior as the old clearS3Files googleapis branch.
func (s *Store) deletePrefixIndividually(ctx context.Context, prefix string) error {
	var pageErr error
	listErr := utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(ctx context.Context) error {
		pageErr = nil
		return s.forEachPage(ctx, prefix, func(keys []string) error {
			logger.Debugf("individual delete: found %d objects under %s, deleting", len(keys), prefix)
			concurrency := min(runtime.GOMAXPROCS(0)*4, len(keys))
			pageErr = utils.Concurrent(ctx, keys, concurrency, func(_ context.Context, key string, _ int) error {
				return utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(_ context.Context) error {
					return s.Delete(ctx, key)
				})
			})
			if pageErr != nil {
				return errStopPage
			}
			return nil
		})
	})
	if errors.Is(listErr, errStopPage) {
		return pageErr
	}
	if listErr != nil {
		return fmt.Errorf("failed to list objects for prefix %s: %w", prefix, listErr)
	}
	return nil
}

func (s *Store) forEachPage(ctx context.Context, prefix string, fn func(keys []string) error) error {
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(page.Contents))
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if len(keys) > 0 {
			if err := fn(keys); err != nil {
				return err
			}
		}
		next := page.NextContinuationToken
		if !aws.ToBool(page.IsTruncated) || next == nil || (token != nil && aws.ToString(token) == aws.ToString(next)) {
			return nil
		}
		token = next
	}
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = out.Body.Close() }()
	return io.ReadAll(out.Body)
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	err := s.forEachPage(ctx, prefix, func(pageKeys []string) error {
		keys = append(keys, pageKeys...)
		return nil
	})
	return keys, err
}

func (s *Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	escaped := strings.ReplaceAll(url.PathEscape(srcKey), "%2F", "/")
	_, err := s.client.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(s.bucket + "/" + escaped),
	})
	return err
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *Store) IsNotFound(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound")
}

// IsRateLimitError reports a throttle response. AWS S3 returns HTTP 503
// (SlowDown / ServiceUnavailable). GCP Cloud Storage returns HTTP 429.
func (s *Store) IsRateLimitError(err error) bool {
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && (respErr.HTTPStatusCode() == 429 || respErr.HTTPStatusCode() == 503)
}
