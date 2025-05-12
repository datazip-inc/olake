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

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/viper"
)

const artifactSubDir = "_olake_runtime" // Directory within the base path for artifacts

// ArtifactPersister handles uploading runtime artifacts (state, logs, etc.) to S3.
type ArtifactPersister struct {
	s3Client     *s3.S3
	s3Uploader   *s3manager.Uploader
	bucket       string
	fullBasePath string // Combined BasePath + artifactSubDir
}

// NewArtifactPersister creates and initializes an ArtifactPersister
func NewArtifactPersister(cfg types.ArtifactStorageConfig, isActive bool) (*ArtifactPersister, error) {
	if !isActive {
		return nil, nil
	}
	if cfg.Bucket == "" {
		logger.Error("S3 bucket name cannot be empty for ArtifactPersister - persistence disabled")
		return nil, nil
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
		return nil, nil
	}
	if _, err := sess.Config.Credentials.Get(); err != nil {
		logger.Errorf("Failed to get AWS credentials: %v - artifact persistence disabled", err)
		return nil, nil
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

// InitializePersister loads config and creates a persister
func InitializePersister(ctx context.Context) (*ArtifactPersister, error) {
	// Check if artifact storage config path is provided
	if artifactStoragePath == "" {
		return nil, nil // No persistence requested
	}

	// Load config from JSON file as raw map first to handle multiple formats
	var rawConfig map[string]interface{}
	if err := utils.UnmarshalFile(artifactStoragePath, &rawConfig); err != nil {
		logger.Errorf("Failed to load artifact storage config from %s: %v", artifactStoragePath, err)
		return nil, fmt.Errorf("failed to load artifact storage config: %w", err)
	}

	// Convert to our config type
	var cfg types.ArtifactStorageConfig

	// First load standard config
	if err := utils.UnmarshalFile(artifactStoragePath, &cfg); err != nil {
		logger.Errorf("Failed to parse artifact storage config from %s: %v", artifactStoragePath, err)
		return nil, fmt.Errorf("failed to parse artifact storage config: %w", err)
	}

	// If bucket not explicitly set, try to extract from iceberg_s3_path
	if cfg.Bucket == "" {
		if icebergS3Path, ok := rawConfig["iceberg_s3_path"].(string); ok && icebergS3Path != "" {
			// Extract bucket from s3://bucket-name/path format
			icebergS3Path = strings.TrimPrefix(icebergS3Path, "s3://")
			icebergS3Path = strings.TrimPrefix(icebergS3Path, "s3a://")

			if parts := strings.SplitN(icebergS3Path, "/", 2); len(parts) > 0 {
				cfg.Bucket = parts[0] // First part is the bucket name

				// If path part exists, use it as base path
				if len(parts) > 1 && cfg.BasePath == "" {
					cfg.BasePath = parts[1]
				}
			}

			logger.Infof("Using bucket '%s' extracted from iceberg_s3_path", cfg.Bucket)
		}
	}

	// If region not set, try AWS region keys
	if cfg.Region == "" {
		if awsRegion, ok := rawConfig["aws_region"].(string); ok && awsRegion != "" {
			cfg.Region = awsRegion
			logger.Infof("Using region '%s' from aws_region", cfg.Region)
		} else if region, ok := rawConfig["region"].(string); ok && region != "" {
			cfg.Region = region
		}
	}

	// If credentials not set, try AWS credential keys
	if cfg.AccessKey == "" {
		if awsAccessKey, ok := rawConfig["aws_access_key"].(string); ok && awsAccessKey != "" {
			cfg.AccessKey = awsAccessKey
		}
	}

	if cfg.SecretKey == "" {
		if awsSecretKey, ok := rawConfig["aws_secret_key"].(string); ok && awsSecretKey != "" {
			cfg.SecretKey = awsSecretKey
		}
	}

	// Validate bucket is set after all extraction attempts
	if cfg.Bucket == "" {
		logger.Error("S3 bucket name cannot be determined from artifact storage config")
		return nil, fmt.Errorf("S3 bucket name cannot be determined from artifact storage config")
	}

	// Force UseSSL to true for security
	cfg.UseSSL = true

	persister, err := NewArtifactPersister(cfg, true)
	if err != nil {
		logger.Errorf("Failed to initialize artifact persister: %v", err)
		return nil, fmt.Errorf("artifact persister initialization failed: %w", err)
	}
	if persister == nil {
		logger.Error("Failed to initialize artifact persister: returned nil")
		return nil, fmt.Errorf("artifact persister initialization failed with nil result")
	}

	// Test the connection after initialization
	if err := testS3Connection(persister.s3Client, persister.bucket, persister.fullBasePath); err != nil {
		logger.Errorf("S3 connection test failed: %v", err)
		return nil, fmt.Errorf("S3 connection test failed: %w", err)
	}

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
			err := persister.UploadFile(uploadCtx, localStatePath, s3KeySuffix)
			cancel()
			if err != nil {
				// Already logged within UploadFile
			} else {
				logger.Debugf("Periodic state upload successful for %s", localStatePath)
			}
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
