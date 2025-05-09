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
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/viper"
)

const artifactSubDir = "_olake_runtime" // Directory within the base path for artifacts

// PersistenceConfig holds configuration for artifact persistence
type PersistenceConfig struct {
	Type         string `json:"type"`          // "s3" (only option currently)
	Bucket       string `json:"bucket"`        // S3 bucket name
	Region       string `json:"region"`        // AWS region
	BasePath     string `json:"base_path"`     // Base path in bucket
	AccessKey    string `json:"access_key"`    // AWS access key (optional)
	SecretKey    string `json:"secret_key"`    // AWS secret key (optional)
	SessionToken string `json:"session_token"` // AWS session token (optional)
	Endpoint     string `json:"endpoint"`      // Custom S3 endpoint (optional)
	UseSSL       bool   `json:"use_ssl"`       // Use SSL for S3 (default: true)
	PathStyle    bool   `json:"path_style"`    // Use path-style addressing (optional)
	Interval     string `json:"interval"`      // Upload interval (e.g. "5m", default: "5m")
}

// S3ArtifactConfig holds the necessary S3 configuration parameters.
type S3ArtifactConfig struct {
	Bucket       string
	Region       string
	BasePath     string // The base S3 path (e.g., prefix or parsed path)
	AccessKey    string
	SecretKey    string
	SessionToken string
	Endpoint     string
	UseSSL       bool
	PathStyle    bool
}

// ArtifactPersister handles uploading runtime artifacts (state, logs, etc.) to S3.
type ArtifactPersister struct {
	s3Client     *s3.S3
	s3Uploader   *s3manager.Uploader
	bucket       string
	fullBasePath string // Combined BasePath + artifactSubDir
}

// NewArtifactPersister creates and initializes an ArtifactPersister
func NewArtifactPersister(cfg S3ArtifactConfig, isActive bool) (*ArtifactPersister, error) {
	// Early return if inactive
	if !isActive {
		return nil, nil
	}

	// Basic validation - log and return inactive for missing bucket
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

	// Verify credentials - log and return inactive if failed
	if _, err := sess.Config.Credentials.Get(); err != nil {
		logger.Errorf("Failed to get AWS credentials: %v - artifact persistence disabled", err)
		return nil, nil
	}

	s3Client := s3.New(sess)
	s3Uploader := s3manager.NewUploader(sess)

	// Prepare paths
	trimmedBasePath := strings.Trim(cfg.BasePath, "/")
	fullBasePath := artifactSubDir
	if trimmedBasePath != "" {
		fullBasePath = filepath.Join(trimmedBasePath, artifactSubDir)
	}
	fullBasePath = filepath.ToSlash(fullBasePath) // Ensure forward slashes for S3

	// S3 write check - log and return inactive if failed
	testKey := strings.Join([]string{fullBasePath, ".olake_write_test"}, "/")
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("Olake artifact persister write test"),
	})
	if err != nil {
		logger.Errorf("S3 write check failed: %v - artifact persistence disabled", err)
		return nil, nil
	} else {
		// Clean up test object
		_, _ = s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(testKey),
		})
	}

	persister := &ArtifactPersister{
		s3Client:     s3Client,
		s3Uploader:   s3Uploader,
		bucket:       cfg.Bucket,
		fullBasePath: fullBasePath,
	}

	logger.Infof("ArtifactPersister initialized. Target: s3://%s/%s/", cfg.Bucket, fullBasePath)
	return persister, nil
}

// InitializePersister loads config and creates a persister
func InitializePersister(ctx context.Context) (*ArtifactPersister, error) {
	// Check if persistence is enabled
	if !viper.GetBool("PERSISTENCE_ENABLED") {
		return nil, nil
	}

	// Get config file path
	configPath := viper.GetString("PERSISTENCE_CONFIG")
	if configPath == "" {
		msg := "Artifact persistence is enabled but no config file specified (use --persistence-config)"
		if viper.GetBool("PERSISTENCE_REQUIRED") {
			return nil, fmt.Errorf(msg)
		}
		logger.Error(msg)
		return nil, nil
	}

	// Load config file
	var config PersistenceConfig
	if err := utils.UnmarshalFile(configPath, &config); err != nil {
		msg := fmt.Sprintf("Failed to load persistence config file: %v", err)
		if viper.GetBool("PERSISTENCE_REQUIRED") {
			return nil, fmt.Errorf(msg)
		}
		logger.Error(msg)
		return nil, nil
	}

	// Validate config
	if config.Type != "s3" {
		logger.Errorf("Unsupported persistence type: %s (only s3 supported)", config.Type)
		return nil, nil
	}

	if config.Bucket == "" {
		msg := "S3 bucket name cannot be empty in persistence config"
		if viper.GetBool("PERSISTENCE_REQUIRED") {
			return nil, fmt.Errorf(msg)
		}
		logger.Error(msg)
		return nil, nil
	}

	// Convert to S3ArtifactConfig
	s3Config := S3ArtifactConfig{
		Bucket:       config.Bucket,
		Region:       config.Region,
		BasePath:     config.BasePath,
		AccessKey:    config.AccessKey,
		SecretKey:    config.SecretKey,
		SessionToken: config.SessionToken,
		Endpoint:     config.Endpoint,
		UseSSL:       config.UseSSL,
		PathStyle:    config.PathStyle,
	}

	// Create persister
	persister, err := NewArtifactPersister(s3Config, true)
	if err != nil || persister == nil {
		msg := fmt.Sprintf("Failed to initialize artifact persister: %v", err)
		if viper.GetBool("PERSISTENCE_REQUIRED") {
			return nil, fmt.Errorf(msg)
		}
		logger.Error(msg)
		return nil, nil
	}

	// Parse interval
	interval := 5 * time.Minute // default
	if config.Interval != "" {
		customInterval, err := time.ParseDuration(config.Interval)
		if err == nil && customInterval > 0 {
			interval = customInterval
		} else {
			logger.Warnf("Invalid interval format in config (%s), using default (5m)", config.Interval)
		}
	}

	// Start periodic uploader
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
