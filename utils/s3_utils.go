package utils

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3URIPrefix = "s3://"

// S3PathMapping tracks an S3 URI and its downloaded local copy.
type S3PathMapping struct {
	OriginalPath string
	LocalPath    string
	IsS3         bool
	Bucket       string
	Key          string
}

// newS3Client creates a new S3 client.
func newS3Client(ctx context.Context) (*s3.Client, error) {
	// Load the AWS config.
	configOpts := []func(*config.LoadOptions) error{}

	if region := envFirst("OLAKE_S3_REGION", "AWS_REGION"); region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	accessKey := envFirst("OLAKE_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
	secretKey := envFirst("OLAKE_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		sessionToken := envFirst("OLAKE_S3_SESSION_TOKEN", "AWS_SESSION_TOKEN")
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %s", err)
	}

	opts := []func(*s3.Options){}
	if endpoint := envFirst("OLAKE_S3_ENDPOINT", "AWS_ENDPOINT_URL"); endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(cfg, opts...), nil
}

// envFirst returns the first non-empty environment variable value from the given keys.
func envFirst(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}

// ResolveS3Path downloads an s3:// path to a local temp file. Local paths are returned unchanged.
func ResolveS3Path(ctx context.Context, path string) (S3PathMapping, error) {
	if !IsS3Path(path) {
		return S3PathMapping{OriginalPath: path, LocalPath: path}, nil
	}

	bucket, key, err := ParseS3URI(path)
	if err != nil {
		return S3PathMapping{}, err
	}

	client, err := newS3Client(ctx)
	if err != nil {
		return S3PathMapping{}, err
	}

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to download %s: %s", path, err)
	}
	defer resp.Body.Close()

	localPath := localPathForS3URI(bucket, key)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to create temp dir for %s: %s", path, err)
	}

	file, err := os.Create(localPath)
	if err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to create local file for %s: %s", path, err)
	}

	if _, err = io.Copy(file, resp.Body); err != nil {
		file.Close()
		return S3PathMapping{}, fmt.Errorf("failed to write local file for %s: %s", path, err)
	}
	if err := file.Close(); err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to close local file for %s: %s", path, err)
	}

	return S3PathMapping{
		OriginalPath: path,
		LocalPath:    localPath,
		IsS3:         true,
		Bucket:       bucket,
		Key:          key,
	}, nil
}

// ParseS3URI parses an s3:// URI into a bucket and key.
// Caller must ensure uri is an s3:// path.
func ParseS3URI(uri string) (bucket, key string, err error) {
	rest := strings.TrimPrefix(uri, s3URIPrefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid s3 uri: %s", uri)
	}

	return parts[0], parts[1], nil
}

// localPathForS3URI returns the local path for an S3 URI.
func localPathForS3URI(bucket, key string) string {
	return filepath.Join(os.TempDir(), "olake", "s3", bucket, key)
}

// IsS3Path returns true if the path is an S3 URI.
func IsS3Path(path string) bool {
	return strings.HasPrefix(path, s3URIPrefix)
}

// UploadFileToS3 uploads the local copy back to S3 when the original path was s3://.
// Caller must ensure LocalPath exists.
func UploadFileToS3(ctx context.Context, s3PathMap S3PathMapping) error {
	if !s3PathMap.IsS3 {
		return nil
	}

	// Create a new S3 client.
	client, err := newS3Client(ctx)
	if err != nil {
		return err
	}

	// Open the local file.
	file, err := os.Open(s3PathMap.LocalPath)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %s", s3PathMap.LocalPath, err)
	}
	defer file.Close()

	// Upload the file to S3.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s3PathMap.Bucket),
		Key:    aws.String(s3PathMap.Key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload %s: %s", s3PathMap.OriginalPath, err)
	}

	return nil
}
