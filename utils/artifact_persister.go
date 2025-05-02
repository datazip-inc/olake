package utils

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager" // Using s3manager for potential uploads
	"github.com/datazip-inc/olake/logger"
)

const artifactSubDir = "_olake_runtime" // Directory within the base path for artifacts

// S3ArtifactConfig holds the necessary configuration parameters for the ArtifactPersister.
type S3ArtifactConfig struct {
	Bucket       string // Target S3 bucket name
	Region       string // AWS Region for the bucket
	BasePath     string // Base path prefix within the bucket (derived from writer config)
	AccessKey    string // Optional: Explicit AWS Access Key
	SecretKey    string // Optional: Explicit AWS Secret Key
	SessionToken string // Optional: AWS Session Token
	Endpoint     string // Optional: Custom S3 endpoint (for S3-compatible storage)
	UseSSL       bool   // Optional: Use SSL for custom endpoint
	PathStyle    bool   // Optional: Use path-style access for custom endpoint
}

// ArtifactPersister handles uploading runtime artifacts (state, logs, etc.) to S3.
type ArtifactPersister struct {
	s3Client         *s3.S3
	s3Uploader       *s3manager.Uploader // Useful for efficient uploads
	bucket           string
	fullBasePath     string // Combined BasePath + artifactSubDir
	isExternalConfig bool   // Flag if external S3 config is used (future use maybe)
}

// NewArtifactPersister creates and initializes a new ArtifactPersister.
// It configures the AWS session and S3 client based on the provided config.
// It prioritizes explicit credentials, then falls back to the default AWS credential chain.
func NewArtifactPersister(cfg S3ArtifactConfig) (*ArtifactPersister, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket name is required for ArtifactPersister")
	}

	// --- AWS Session Configuration ---
	awsCfg := aws.Config{}

	// Region
	if cfg.Region != "" {
		awsCfg.Region = aws.String(cfg.Region)
	} else if cfg.Endpoint == "" {
		// Only warn if using AWS S3 (no custom endpoint) and region isn't explicit
		// Region might still be picked up from env (AWS_REGION) or shared config
		logger.Warn("S3 region not explicitly provided for artifact persistence, attempting to use default AWS credential chain resolution")
	}

	// Credentials - Prioritize explicit keys
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		logger.Info("Using explicit S3 credentials for artifact persistence")
		awsCfg.Credentials = credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)
	} else {
		logger.Info("Explicit S3 credentials not provided for artifact persistence, using default AWS credential chain (Env Vars, Shared Config/Credentials, IAM Role/Instance Profile)")
		// If keys aren't provided, the SDK will automatically use the default chain
	}

	// Custom Endpoint (for S3-compatible storage like MinIO)
	if cfg.Endpoint != "" {
		logger.Infof("Using custom S3 endpoint for artifact persistence: %s", cfg.Endpoint)
		awsCfg.Endpoint = aws.String(cfg.Endpoint)
		awsCfg.S3ForcePathStyle = aws.Bool(cfg.PathStyle) // Often needed for custom endpoints
		if !cfg.UseSSL {
			awsCfg.DisableSSL = aws.Bool(true)
		}
	}
	// --- End AWS Session Configuration ---

	// Create AWS Session
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:            awsCfg,
		SharedConfigState: session.SharedConfigEnable, // Enable loading from ~/.aws/config
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session for artifact persistence: %w", err)
	}

	// Verify credentials are loaded if not explicitly provided
	// This helps fail fast if the default chain doesn't resolve anything
	if cfg.AccessKey == "" && cfg.SecretKey == "" {
		if _, err := sess.Config.Credentials.Get(); err != nil {
			return nil, fmt.Errorf("failed to get AWS credentials using default chain for artifact persistence: %w."+
				" Ensure credentials are configured via ENV vars, shared files, or IAM role/profile", err)
		}
	}

	s3Client := s3.New(sess)
	s3Uploader := s3manager.NewUploaderWithClient(s3Client)

	// Construct the full path including the artifact subdirectory
	// Trim leading/trailing slashes from base path for clean joining
	trimmedBasePath := strings.Trim(cfg.BasePath, "/")
	fullBasePath := artifactSubDir // Start with the artifact dir
	if trimmedBasePath != "" {
		fullBasePath = strings.Join([]string{trimmedBasePath, artifactSubDir}, "/")
	}

	// Simple S3 write check (optional but recommended)
	testKey := strings.Join([]string{fullBasePath, ".olake_write_test"}, "/")
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("Olake artifact persister write test"),
	})
	if err != nil {
		return nil, fmt.Errorf("S3 write check failed for artifact persister (bucket: %s, key: %s): %w. Check bucket/path existence and permissions", cfg.Bucket, testKey, err)
	} else {
		// Clean up test file (best effort)
		_, _ = s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(testKey),
		})
	}

	return &ArtifactPersister{
		s3Client:     s3Client,
		s3Uploader:   s3Uploader,
		bucket:       cfg.Bucket,
		fullBasePath: fullBasePath,
		// isExternalConfig: false, // Set appropriately if separate config introduced
	}, nil
}

// --- Placeholder for Upload methods ---
// func (ap *ArtifactPersister) UploadFile(...) error { ... }
// func (ap *ArtifactPersister) UploadLogDirectory(...) error { ... }
