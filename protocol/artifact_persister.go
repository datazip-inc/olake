package protocol

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"errors"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/viper"
)

const artifactSubDir = "_olake_runtime" // Directory within the base path for artifacts

// ErrArtifactStorageNotConfigured is a sentinel error for when artifact storage is not configured
var ErrArtifactStorageNotConfigured = errors.New("artifact storage not configured: --artifact-storage flag not provided or path is empty")

// ArtifactPersister handles uploading runtime artifacts (state, logs, etc.) to S3.
type ArtifactPersister struct {
	s3Client     *s3.S3
	s3Uploader   *s3manager.Uploader
	bucket       string
	fullBasePath string // Combined BasePath + artifactSubDir
}

// NewArtifactPersister creates and initializes an ArtifactPersister
// It will only return a valid persister or an error, never (nil, error)
func NewArtifactPersister(cfg types.ArtifactStorageConfig) (*ArtifactPersister, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket name cannot be empty for ArtifactPersister")
	}

	awsCfg := aws.NewConfig()
	if cfg.Region != "" {
		awsCfg.WithRegion(cfg.Region)
	} else if cfg.Endpoint == "" {
		logger.Warn("S3 region not explicitly provided for artifact persistence, attempting to use default AWS credential chain resolution")
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		logger.Info("Using explicit S3 credentials for artifact persistence")
		awsCfg.WithCredentials(credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken))
	} else {
		logger.Info("Explicit S3 credentials not provided for artifact persistence, using default AWS credential chain")
	}
	if cfg.Endpoint != "" {
		logger.Infof("Using custom S3 endpoint for artifact persistence: %s", cfg.Endpoint)
		awsCfg.WithEndpoint(cfg.Endpoint)
		awsCfg.WithS3ForcePathStyle(cfg.PathStyle)
		if !cfg.UseSSL {
			awsCfg.WithDisableSSL(true)
		}
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		Config:            *awsCfg,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		logger.Errorf("Failed to create AWS session: %v - artifact persistence disabled", err)
		return nil, err
	}
	if _, err := sess.Config.Credentials.Get(); err != nil {
		logger.Errorf("Failed to get AWS credentials: %v - artifact persistence disabled", err)
		return nil, err
	}

	s3Client := s3.New(sess)
	s3Uploader := s3manager.NewUploader(sess)

	trimmedBasePath := strings.Trim(cfg.BasePath, "/")
	fullBasePath := cfg.GetFullBasePath(artifactSubDir)
	if trimmedBasePath != "" {
		fullBasePath = filepath.Join(trimmedBasePath, artifactSubDir)
	}
	fullBasePath = filepath.ToSlash(fullBasePath) // Ensure forward slashes for S3

	persister := &ArtifactPersister{
		s3Client:     s3Client,
		s3Uploader:   s3Uploader,
		bucket:       cfg.Bucket,
		fullBasePath: fullBasePath,
	}

	logger.Infof("ArtifactPersister initialized. Target: s3://%s/%s/", cfg.Bucket, fullBasePath)
	return persister, nil
}

// testS3Connection performs a simple write/delete check to validate S3 access
// Returns nil if successful, error otherwise
func testS3Connection(s3Client *s3.S3, bucket, basePath string) error {
	testKey := filepath.Join(basePath, ".olake_write_test")
	testKey = filepath.ToSlash(testKey)

	// Try to write test object
	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("Olake artifact persister write test"),
	})
	if err != nil {
		return err
	}

	// Clean up (ignore errors here)
	_, _ = s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
	})

	return nil
}

// InitializePersister loads config and creates a persister if the flag is provided
func InitializePersister(ctx context.Context) (*ArtifactPersister, error) {
	// If flag not provided, artifact persistence is not requested
	if artifactStoragePath == "" {
		return nil, ErrArtifactStorageNotConfigured
	}

	// Load and validate config
	var cfg types.ArtifactStorageConfig
	if err := utils.UnmarshalFile(artifactStoragePath, &cfg); err != nil {
		return nil, fmt.Errorf("failed to load artifact storage config: %w", err)
	}

	// Extract bucket name from s3:// format
	if cfg.Bucket != "" {
		bucketStr := strings.TrimPrefix(cfg.Bucket, "s3://")
		bucketStr = strings.TrimPrefix(bucketStr, "s3a://")

		if parts := strings.SplitN(bucketStr, "/", 2); len(parts) > 0 {
			cfg.Bucket = parts[0]

			// If path part exists and base path not already set, use it as base path
			if len(parts) > 1 && cfg.BasePath == "" {
				cfg.BasePath = parts[1]
				cfg.BasePath = strings.Trim(cfg.BasePath, "/")
			}
		}
		logger.Infof("Using S3 bucket: %s and base path: '%s'", cfg.Bucket, cfg.BasePath)
	}

	// Validate bucket is set (this uses the already parsed cfg.Bucket)
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("S3 bucket name cannot be determined or is empty in artifact storage config (from %s)", artifactStoragePath)
	}

	// Create persister - this will error if config is invalid
	persister, err := NewArtifactPersister(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifact persister initialization failed: %w", err)
	}

	// Test connection
	if err := testS3Connection(persister.s3Client, persister.bucket, persister.fullBasePath); err != nil {
		return nil, fmt.Errorf("S3 connection test failed: %w", err)
	}

	// Start periodic uploader
	interval := 5 * time.Minute
	if cfg.UploadInterval != "" {
		if d, err := time.ParseDuration(cfg.UploadInterval); err == nil && d > 0 {
			interval = d
		} else {
			logger.Warnf("Invalid upload interval format in config: %s (using default: 5m)", cfg.UploadInterval)
		}
	}

	go RunPeriodicStateUploader(ctx, persister, interval)
	logger.Infof("S3 artifact persistence enabled. Uploading state.json every %v", interval)
	return persister, nil
}

// UploadFile uploads a single local file to the configured S3 path
func (ap *ArtifactPersister) UploadFile(ctx context.Context, localPath string, s3KeySuffix string) error {
	if ap == nil || ap.s3Uploader == nil {
		return fmt.Errorf("ArtifactPersister not initialized")
	}

	file, err := os.Open(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warnf("Local artifact file not found for upload: %s", localPath)
			return nil // Don't fail hard
		}
		return fmt.Errorf("failed to open local artifact file %s: %w", localPath, err)
	}
	defer file.Close()

	s3Key := filepath.Join(ap.fullBasePath, s3KeySuffix)
	s3Key = filepath.ToSlash(s3Key) // Ensure forward slashes

	logger.Debugf("Uploading artifact %s to s3://%s/%s", localPath, ap.bucket, s3Key)

	_, err = ap.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(ap.bucket),
		Key:    aws.String(s3Key),
		Body:   file,
	})

	if err != nil {
		logger.Warnf("Failed to upload artifact %s to S3: %v", localPath, err)
		return fmt.Errorf("failed to upload artifact %s to s3://%s/%s: %w", localPath, ap.bucket, s3Key, err)
	}

	logger.Debugf("Successfully uploaded artifact %s to s3://%s/%s", localPath, ap.bucket, s3Key)
	return nil
}

// RunPeriodicStateUploader periodically uploads the state.json file
func RunPeriodicStateUploader(ctx context.Context, persister *ArtifactPersister, interval time.Duration) {
	if persister == nil {
		return
	}

	configDir := viper.GetString("CONFIG_FOLDER")
	if configDir == "" {
		logger.Error("CONFIG_FOLDER not set, cannot determine path for state.json periodic upload.")
		return
	}
	localStatePath := filepath.Join(configDir, "state.json")
	s3KeySuffix := "state.json"

	logger.Infof("Starting periodic state uploader. Interval: %v. Target: s3://%s/%s",
		interval,
		persister.bucket,
		filepath.ToSlash(filepath.Join(persister.fullBasePath, s3KeySuffix)))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping periodic state uploader due to context cancellation.")
			// Final upload attempt on cancellation
			finalUploadCtx, finalCancel := context.WithTimeout(context.Background(), 30*time.Second)
			logger.Infof("Attempting final state upload to S3 before shutdown...")
			err := persister.UploadFile(finalUploadCtx, localStatePath, s3KeySuffix)
			finalCancel()
			if err != nil {
				logger.Errorf("Final state upload to S3 failed: %v", err)
			} else {
				logger.Info("Final state upload to S3 successful.")
			}
			return
		case <-ticker.C:
			uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_ = persister.UploadFile(uploadCtx, localStatePath, s3KeySuffix)
			cancel()
			logger.Debugf("Periodic state upload successful for %s", localStatePath)
		}
	}
}

// UploadFinalState performs a final upload of the state file before exit
func (ap *ArtifactPersister) UploadFinalState(ctx context.Context) error {
	if ap == nil {
		return nil
	}

	configDir := viper.GetString("CONFIG_FOLDER")
	if configDir == "" {
		return fmt.Errorf("CONFIG_FOLDER not set, cannot determine path for state.json")
	}

	localStatePath := filepath.Join(configDir, "state.json")
	logger.Info("Performing final state upload before exit...")

	err := ap.UploadFile(ctx, localStatePath, "state.json")
	if err != nil {
		logger.Errorf("Final state upload failed: %v", err)
		return err
	}

	logger.Info("Final state upload completed successfully")
	return nil
}
