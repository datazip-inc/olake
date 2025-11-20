package driver

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// S3 represents the S3 source driver
type S3 struct {
	client          *s3.Client
	config          *Config
	state           *types.State
	filePattern     *regexp.Regexp
	discoveredFiles map[string][]FileObject // map[streamName][]files
}

// GetConfigRef returns a reference to the config struct
func (s *S3) GetConfigRef() abstract.Config {
	s.config = &Config{}
	return s.config
}

// Spec returns the configuration specification
func (s *S3) Spec() any {
	return Config{}
}

// Type returns the driver type identifier
func (s *S3) Type() string {
	return "s3"
}

// Setup initializes the S3 client and validates the configuration
func (s *S3) Setup(ctx context.Context) error {
	// Validate configuration
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}

	// Compile file pattern regex if provided
	if s.config.FilePattern != "" {
		pattern, err := regexp.Compile(s.config.FilePattern)
		if err != nil {
			return fmt.Errorf("failed to compile file_pattern regex: %w", err)
		}
		s.filePattern = pattern
		logger.Infof("Using file pattern filter: %s", s.config.FilePattern)
	}

	// Configure AWS SDK - supports both static credentials and default credential chain
	var cfg aws.Config
	var err error

	// Build config options
	configOpts := []func(*config.LoadOptions) error{
		config.WithRegion(s.config.Region),
	}

	// Use static credentials if provided, otherwise fall back to default credential chain
	// Default chain includes: IAM roles, instance profiles, environment variables, shared config
	if s.config.AccessKeyID != "" && s.config.SecretAccessKey != "" {
		logger.Info("Using static credentials for S3 authentication")
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				s.config.AccessKeyID,
				s.config.SecretAccessKey,
				"",
			),
		))
	} else {
		logger.Info("Using default credential chain (IAM role, instance profile, env vars, or shared config)")
	}

	// Load configuration
	if s.config.Endpoint != "" {
		logger.Infof("Connecting to S3-compatible endpoint: %s", s.config.Endpoint)
	} else {
		logger.Infof("Connecting to AWS S3 in region: %s", s.config.Region)
	}

	cfg, err = config.LoadDefaultConfig(ctx, configOpts...)

	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	if s.config.Endpoint != "" {
		s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s.config.Endpoint)
			o.UsePathStyle = true // Required for MinIO and some S3-compatible services
		})
	} else {
		s.client = s3.NewFromConfig(cfg)
	}

	// Test connection by checking if bucket exists and is accessible
	logger.Infof("Testing connection to bucket: %s", s.config.BucketName)
	_, err = s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.BucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to access bucket %s: %w", s.config.BucketName, err)
	}

	logger.Info("Successfully connected to S3")
	return nil
}

// SetupState sets the state reference for tracking sync progress
func (s *S3) SetupState(state *types.State) {
	s.state = state
}

// StateType returns the type of state management this driver uses
func (s *S3) StateType() types.StateType {
	return types.StreamType
}

// MaxConnections returns the maximum number of concurrent connections
func (s *S3) MaxConnections() int {
	return s.config.MaxThreads
}

// MaxRetries returns the maximum number of retry attempts
func (s *S3) MaxRetries() int {
	return s.config.RetryCount
}

// GetStreamNames discovers all files in the S3 bucket matching the configuration
func (s *S3) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Discovering files in bucket: %s with prefix: %s", s.config.BucketName, s.config.PathPrefix)

	// Initialize the map for grouped files
	filesByStream := make(map[string][]FileObject)
	var continuationToken *string
	pageCount := 0
	totalDiscovered := 0

	// List all objects with the given prefix (paginated)
	// Note: We accumulate all file metadata before processing because:
	// 1. File metadata is small (~200 bytes per file, 1M files = ~200MB)
	// 2. Chunking requires full file list to group files into ~2GB chunks
	// 3. Incremental sync needs to filter across all files by LastModified
	for {
		pageCount++
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.BucketName),
			Prefix:            aws.String(s.config.PathPrefix),
			ContinuationToken: continuationToken,
		}

		result, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket: %w", err)
		}

		logger.Debugf("Processing S3 list page %d (%d objects in this page)", pageCount, len(result.Contents))

		// Filter and collect matching files
		for _, obj := range result.Contents {
			key := aws.ToString(obj.Key)

			// Skip directories (keys ending with /)
			if strings.HasSuffix(key, "/") {
				continue
			}

			// Apply file pattern filter if configured
			if s.filePattern != nil && !s.filePattern.MatchString(key) {
				logger.Debugf("Skipping file %s (does not match pattern)", key)
				continue
			}

			// Filter by file extension based on format
			if !s.matchesFileFormat(key) {
				logger.Debugf("Skipping file %s (does not match format)", key)
				continue
			}

			fileObj := FileObject{
				FileKey:      key,
				Size:         aws.ToInt64(obj.Size),
				LastModified: obj.LastModified.Format("2006-01-02T15:04:05Z"),
				ETag:         strings.Trim(aws.ToString(obj.ETag), "\""),
			}

			// Group files by stream name (folder or individual file)
			streamName := s.extractStreamName(key)
			filesByStream[streamName] = append(filesByStream[streamName], fileObj)
			totalDiscovered++
		}

		// Check if there are more results
		if !aws.ToBool(result.IsTruncated) {
			logger.Infof("Completed S3 discovery: processed %d pages, discovered %d files", pageCount, totalDiscovered)
			break
		}
		continuationToken = result.NextContinuationToken
	}

	// Store grouped files
	s.discoveredFiles = filesByStream

	// Extract stream names
	streamNames := make([]string, 0, len(filesByStream))
	totalFiles := 0
	for streamName, files := range filesByStream {
		streamNames = append(streamNames, streamName)
		totalFiles += len(files)
	}

	logger.Infof("Discovered %d files in %d streams (after filtering)", totalFiles, len(streamNames))
	if s.config.StreamGroupingEnabled {
		logger.Infof("Stream grouping enabled at level %d", s.config.StreamGroupingLevel)
	}

	return streamNames, nil
}

// extractStreamName extracts the stream name from a file key based on grouping configuration
func (s *S3) extractStreamName(key string) string {
	if !s.config.StreamGroupingEnabled {
		// No grouping - each file is its own stream
		return key
	}

	// Remove path_prefix from the key to get relative path
	relativePath := key
	if s.config.PathPrefix != "" {
		relativePath = strings.TrimPrefix(key, s.config.PathPrefix)
		relativePath = strings.TrimPrefix(relativePath, "/")
	}

	// Handle edge case: empty relative path after prefix removal
	if relativePath == "" {
		logger.Warnf("File %s has empty relative path after prefix removal, using full key", key)
		return key
	}

	// Split by / and take first N levels based on StreamGroupingLevel
	parts := strings.Split(relativePath, "/")
	if len(parts) == 0 {
		logger.Warnf("File %s produced no path parts, using full key", key)
		return key
	}

	// If file is at or below grouping level, use the path up to that level
	if len(parts) <= s.config.StreamGroupingLevel {
		// If only one part (no folders), use the first part
		if len(parts) == 1 {
			return parts[0]
		}
		// Use all parts except the filename
		return strings.Join(parts[:len(parts)-1], "/")
	}

	// Take the first N levels as stream name
	return strings.Join(parts[:s.config.StreamGroupingLevel], "/")
}

// matchesFileFormat checks if a file key matches the configured file format
func (s *S3) matchesFileFormat(key string) bool {
	lowerKey := strings.ToLower(key)

	switch s.config.FileFormat {
	case FormatCSV:
		return strings.HasSuffix(lowerKey, ".csv") ||
			(s.config.Compression == CompressionGzip && strings.HasSuffix(lowerKey, ".csv.gz"))
	case FormatJSON:
		return strings.HasSuffix(lowerKey, ".json") ||
			strings.HasSuffix(lowerKey, ".jsonl") ||
			(s.config.Compression == CompressionGzip && (strings.HasSuffix(lowerKey, ".json.gz") || strings.HasSuffix(lowerKey, ".jsonl.gz")))
	case FormatParquet:
		return strings.HasSuffix(lowerKey, ".parquet")
	default:
		return false
	}
}

// ProduceSchema generates schema for a given stream (folder or file)
func (s *S3) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("Producing schema for stream: %s", streamName)

	// Get files for this stream
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		return nil, fmt.Errorf("no files found for stream: %s", streamName)
	}

	// Create stream
	stream := types.NewStream(streamName, "s3", &s.config.BucketName)

	// Infer schema from the first file in the stream
	firstFile := files[0]
	logger.Infof("Inferring schema from file: %s (%d files in stream)", firstFile.FileKey, len(files))

	var inferredStream *types.Stream
	var err error

	// Infer schema based on file format
	switch s.config.FileFormat {
	case FormatCSV:
		inferredStream, err = s.inferCSVSchema(ctx, stream, firstFile.FileKey)
	case FormatJSON:
		inferredStream, err = s.inferJSONSchema(ctx, stream, firstFile.FileKey)
	case FormatParquet:
		inferredStream, err = s.inferParquetSchema(ctx, stream, firstFile.FileKey)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}

	if err != nil {
		return nil, err
	}

	return inferredStream, nil
}

// CDCSupported returns false as S3 does not support CDC
func (s *S3) CDCSupported() bool {
	return false
}

// PreCDC is not supported for S3
func (s *S3) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// StreamChanges is not supported for S3
func (s *S3) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// PostCDC is not supported for S3
func (s *S3) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// CloseConnection closes the S3 client connection
func (s *S3) CloseConnection() {
	logger.Info("Closing S3 connection")
	// S3 client doesn't require explicit cleanup
}

// getFileSize retrieves the file size from discovered files
// Returns 0 if file not found (fallback to non-streaming mode)
func (s *S3) getFileSize(streamName, fileKey string) int64 {
	files, exists := s.discoveredFiles[streamName]
	if !exists {
		return 0
	}

	for _, file := range files {
		if file.FileKey == fileKey {
			return file.Size
		}
	}

	return 0
}
