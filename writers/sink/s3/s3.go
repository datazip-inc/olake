package s3

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/sink"
)

type Config struct {
	Path      string `json:"local_path,omitempty"` // Local file path (for local file system usage)
	Bucket    string `json:"s3_bucket,omitempty"`
	Region    string `json:"s3_region,omitempty"`
	AccessKey string `json:"s3_access_key,omitempty"`
	SecretKey string `json:"s3_secret_key,omitempty"`
	Prefix    string `json:"s3_path,omitempty"`
	// S3 endpoint for custom S3-compatible services (like MinIO)
	S3Endpoint string `json:"s3_endpoint,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}

// S3Sink is the transactional Sink for object storage (S3, GCS,
// MinIO …). Each rolled file is staged to a local temp file as it is written and
// recorded on Stage; nothing is uploaded until Commit, and Abort discards the
// staged files. This keeps output transactional — a failed sync uploads nothing —
// and keeps peak memory at roughly one row group rather than a full file in RAM.
type S3Sink struct {
	config   Config
	client   *s3.S3
	uploader *s3manager.Uploader
	stageDir string
	key      sink.FilePathFunc
	index    int
	staged   []s3Staged
	// uploadFn publishes one finished object. NewS3Sink wires it to the
	// s3manager-backed upload; tests override it to capture uploads in memory
	// instead of reaching S3.
	uploadFn func(ctx context.Context, key string, body io.Reader) error
}

type s3Staged struct {
	tmpPath string
	key     string
}

// NewS3Sink builds an S3 Sink.
//   - uploader: persists finished objects (backend-supplied).
//   - stageDir: local directory for staging files before upload (e.g. os.TempDir()).
//   - key:      returns the object key for the i-th rolled file (must be unique
//     per index, e.g. "<partition>/<timestamp>.parquet").
func NewS3Sink(config Config, key sink.FilePathFunc) (*S3Sink, error) {
	s3Config := aws.Config{
		Region: aws.String(config.Region),
	}
	if config.S3Endpoint != "" {
		s3Config.Endpoint = aws.String(config.S3Endpoint)
		// Force path-style URLs (e.g., http://minio:9000/bucket/key) to support MinIO and avoid bucket-based DNS resolution
		s3Config.S3ForcePathStyle = aws.Bool(true)
	}
	if config.AccessKey != "" && config.SecretKey != "" {
		s3Config.Credentials = credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")
	}
	sess, err := session.NewSession(&s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %s", err)
	}
	s := &S3Sink{config: config, client: s3.New(sess), uploader: s3manager.NewUploader(sess), stageDir: config.Path, key: key}
	s.uploadFn = s.upload
	return s, nil
}

type s3FileHandle struct {
	file    *os.File
	tmpPath string
	key     string
}

func (s *S3Sink) Open(_ context.Context) (io.Writer, sink.FileHandle, error) {
	key := s.key(s.index)
	s.index++

	if err := os.MkdirAll(s.stageDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("failed to create stage dir %q: %w", s.stageDir, err)
	}
	f, err := os.CreateTemp(s.stageDir, "olake-*."+parquetExt)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create stage file: %w", err)
	}
	return f, &s3FileHandle{file: f, tmpPath: f.Name(), key: key}, nil
}

func (s *S3Sink) GetClient() *s3.S3 {
	return s.client
}

func (s *S3Sink) Stage(_ context.Context, handle sink.FileHandle, _ int64) error {
	h, ok := handle.(*s3FileHandle)
	if !ok {
		return fmt.Errorf("unexpected handle type %T for S3Sink", handle)
	}
	// The encoder already wrote the parquet footer; flush the descriptor so the
	// staged bytes are complete, then record it for Commit (no upload yet).
	if err := h.file.Close(); err != nil {
		return fmt.Errorf("failed to close stage file %q: %w", h.tmpPath, err)
	}
	s.staged = append(s.staged, s3Staged{tmpPath: h.tmpPath, key: h.key})
	return nil
}

func (s *S3Sink) Commit(ctx context.Context) error {
	for i, f := range s.staged {
		if err := s.uploadOne(ctx, f); err != nil {
			// Keep the unpublished entries (including the failed one, whose temp
			// uploadOne left in place) so a follow-up Abort can remove their staged
			// temp files instead of leaking them.
			s.staged = s.staged[i:]
			return err
		}
	}
	s.staged = nil
	return nil
}

func (s *S3Sink) Abort(_ context.Context) error {
	defer func() { s.staged = nil }()
	for _, f := range s.staged {
		_ = os.Remove(f.tmpPath)
	}
	return nil
}

func (s *S3Sink) PutObject(key string, body io.ReadSeeker) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

// ListObjectsPages lists every object under prefix in the sink's bucket, invoking
// fn for each page (same contract as the AWS SDK pager: return false to stop).
func (s *S3Sink) ListObjectsPages(ctx context.Context, prefix string, fn func(*s3.ListObjectsOutput, bool) bool) error {
	return s.client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	}, fn)
}

// DeleteObject deletes a single object by key from the sink's bucket.
func (s *S3Sink) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *S3Sink) upload(ctx context.Context, key string, body io.Reader) error {
	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

func (s *S3Sink) uploadOne(ctx context.Context, f s3Staged) error {
	file, err := os.Open(f.tmpPath)
	if err != nil {
		return fmt.Errorf("failed to reopen stage file %q: %w", f.tmpPath, err)
	}
	err = s.uploadFn(ctx, f.key, file)
	_ = file.Close()
	if err != nil {
		return fmt.Errorf("failed to upload %q: %w", f.key, err)
	}
	_ = os.Remove(f.tmpPath)
	return nil
}

const parquetExt = "parquet"
