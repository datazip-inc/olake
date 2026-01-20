package driver

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/parser"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	// lastModifiedField is the S3 metadata field name used for tracking file modification timestamps for incremental sync
	lastModifiedField = "_last_modified_time"
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
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// Compile file pattern regex if provided
	if s.config.FilePattern != "" {
		pattern, err := regexp.Compile(s.config.FilePattern)
		if err != nil {
			return fmt.Errorf("failed to compile file_pattern regex: %s", err)
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
	cfg, err = config.LoadDefaultConfig(ctx, configOpts...)

	if err != nil {
		return fmt.Errorf("failed to load AWS config: %s", err)
	}

	// Create S3 client
	if s.config.Endpoint != "" {
		logger.Infof("Connecting to S3-compatible endpoint: %s", s.config.Endpoint)
		s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s.config.Endpoint)
			o.UsePathStyle = true // Required for MinIO and some S3-compatible services
		})
	} else {
		logger.Infof("Connecting to AWS S3 in region: %s", s.config.Region)
		s.client = s3.NewFromConfig(cfg)
	}

	// Test connection by checking if bucket exists and is accessible
	logger.Infof("Testing connection to bucket: %s", s.config.BucketName)
	_, err = s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.BucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to access bucket %s: %s", s.config.BucketName, err)
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
			return nil, fmt.Errorf("failed to list objects in bucket: %s", err)
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
	logger.Infof("Stream grouping enabled at level 1 (first folder after path_prefix)")

	return streamNames, nil
}

// extractStreamName extracts the stream name from a file key
// Stream grouping is always enabled at level 1 (first folder after path_prefix)
func (s *S3) extractStreamName(key string) string {
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

	// Split by / and extract first folder level
	parts := strings.Split(relativePath, "/")
	if len(parts) == 0 {
		logger.Warnf("File %s produced no path parts, using full key", key)
		return key
	}

	// Return first folder level as stream name
	// For files at root (no folders), use the filename itself
	return parts[0]
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

	// Create appropriate parser and infer schema (format-specific logic from parser package)
	switch s.config.FileFormat {
	case FormatCSV:
		inferredStream, err = s.inferSchemaForCSV(ctx, firstFile, stream)
	case FormatJSON:
		inferredStream, err = s.inferSchemaForJSON(ctx, firstFile, stream)
	case FormatParquet:
		inferredStream, err = s.inferSchemaForParquet(ctx, firstFile, stream)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to infer schema: %s", err)
	}

	// Add _last_modified_time as a cursor field for incremental sync
	inferredStream.UpsertField(lastModifiedField, types.String, false)
	inferredStream.WithCursorField(lastModifiedField)

	return inferredStream, nil
}

// withFileReader is a helper that manages file reader lifecycle for CSV/JSON formats
// It acquires a reader, ensures cleanup, and executes the provided callback
func (s *S3) withFileReader(ctx context.Context, fileKey string, callback func(io.Reader) (*types.Stream, error)) (*types.Stream, error) {
	reader, _, err := s.getFileReader(ctx, fileKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get file reader: %s", err)
	}
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	return callback(reader)
}

// withParquetReader is a helper that manages Parquet reader lifecycle
// It acquires a ReaderAt, ensures cleanup, and executes the provided callback
func (s *S3) withParquetReader(ctx context.Context, fileKey string, fileSize int64, callback func(io.Reader) (*types.Stream, error)) (*types.Stream, error) {
	parquetReader, parquetSize, err := s.getParquetReaderAt(ctx, fileKey, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get Parquet reader: %s", err)
	}
	defer func() {
		if closer, ok := parquetReader.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Create a wrapper that implements both io.ReaderAt and provides size info
	wrapper := parser.NewParquetReaderWrapper(parquetReader, parquetSize)
	return callback(wrapper)
}

// inferSchemaForCSV infers schema from a CSV file
func (s *S3) inferSchemaForCSV(ctx context.Context, file FileObject, stream *types.Stream) (*types.Stream, error) {
	return s.withFileReader(ctx, file.FileKey, func(reader io.Reader) (*types.Stream, error) {
		csvParser := parser.NewCSVParser(*s.config.GetCSVConfig(), stream)
		return csvParser.InferSchema(ctx, reader)
	})
}

// inferSchemaForJSON infers schema from a JSON file
func (s *S3) inferSchemaForJSON(ctx context.Context, file FileObject, stream *types.Stream) (*types.Stream, error) {
	return s.withFileReader(ctx, file.FileKey, func(reader io.Reader) (*types.Stream, error) {
		jsonParser := parser.NewJSONParser(*s.config.GetJSONConfig(), stream)
		return jsonParser.InferSchema(ctx, reader)
	})
}

// inferSchemaForParquet infers schema from a Parquet file
func (s *S3) inferSchemaForParquet(ctx context.Context, file FileObject, stream *types.Stream) (*types.Stream, error) {
	return s.withParquetReader(ctx, file.FileKey, file.Size, func(reader io.Reader) (*types.Stream, error) {
		parquetParser := parser.NewParquetParser(*s.config.GetParquetConfig(), stream)
		return parquetParser.InferSchema(ctx, reader)
	})
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
func (s *S3) StreamChanges(ctx context.Context, streamIndex int, processFn abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// PostCDC is not supported for S3
func (s *S3) PostCDC(ctx context.Context, streamIndex int) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// ChangeStreamConfig returns the change stream configuration for S3
func (s *S3) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}

func (s *S3) GetCDCPosition() string {
	// S3 doesn't support CDC
	return ""
}

func (s *S3) GetCDCStartPosition() string {
	// S3 doesn't support CDC
	return ""
}
func (s *S3) SetNextCDCPosition(position string) {
	// S3 doesn't support CDC
}

func (s *S3) GetNextCDCPosition() string {
	// S3 doesn't support CDC
	return ""
}

func (s *S3) SetCurrentCDCPosition(position string) {
	// S3 doesn't support CDC
}

func (s *S3) SetProcessingStreams(streamIDs []string) {
	// S3 doesn't support CDC
}

func (s *S3) RemoveProcessingStream(streamID string) {
	// S3 doesn't support CDC
}

func (s *S3) GetProcessingStreams() []string {
	// S3 doesn't support CDC
	return nil
}

func (s *S3) CheckCDCRecovery(ctx context.Context, pool *destination.WriterPool, streams []types.StreamInterface) error {
	// S3 doesn't support CDC
	return nil
}

func (s *S3) SetTargetCDCPosition(position string) {
	// S3 doesn't support CDC
}

func (s *S3) GetTargetCDCPosition() string {
	// S3 doesn't support CDC
	return ""
}

// SaveNextCDCPositionForStream - no-op for S3
func (s *S3) SaveNextCDCPositionForStream(streamID string) {}

// CommitCDCPositionForStream - no-op for S3
func (s *S3) CommitCDCPositionForStream(streamID string) {}

// CheckPerStreamRecovery - no-op for S3
func (s *S3) CheckPerStreamRecovery(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error {
	return nil
}

// AcknowledgeCDCPosition - no-op for S3
func (s *S3) AcknowledgeCDCPosition(ctx context.Context, position string) error {
	return nil
}

// getFileReader returns a reader for an S3 file with decompression applied (S3-specific logic)
// Returns (reader, fileSize, error)
func (s *S3) getFileReader(ctx context.Context, key string) (io.Reader, int64, error) {
	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get object from S3: %s", err)
	}

	// Get file size
	fileSize := int64(0)
	if result.ContentLength != nil {
		fileSize = *result.ContentLength
	}

	// Apply decompression if needed (auto-detect from file extension)
	reader, err := getDecompressedReader(result.Body, key)
	if err != nil {
		result.Body.Close()
		return nil, 0, fmt.Errorf("failed to create decompressed reader: %s", err)
	}

	return reader, fileSize, nil
}

// getParquetReaderAt returns a reader suitable for Parquet files (io.ReaderAt)
// Uses S3 range reader for streaming or loads into memory based on config
// Returns (readerAt, fileSize, error)
func (s *S3) getParquetReaderAt(ctx context.Context, key string, fileSize int64) (io.ReaderAt, int64, error) {
	parquetConfig := s.config.GetParquetConfig()

	if parquetConfig.StreamingEnabled && fileSize > 0 {
		// Use S3 range requests for streaming (memory-efficient)
		logger.Debugf("Using S3 range requests for Parquet file: %s", key)
		rangeReader := NewS3RangeReader(ctx, s.client, s.config.BucketName, key, fileSize)
		return rangeReader, fileSize, nil
	}

	// Fallback: Load entire file into memory
	logger.Debugf("Loading Parquet file into memory: %s", key)
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get object from S3: %s", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read Parquet file: %s", err)
	}

	return bytes.NewReader(data), int64(len(data)), nil
}

// getDecompressedReader returns an appropriate reader based on file extension
// Auto-detects compression from file extension (.gz)
func getDecompressedReader(body io.Reader, key string) (io.Reader, error) {
	lowerKey := strings.ToLower(key)

	// Check if file has gzip extension
	if strings.HasSuffix(lowerKey, ".gz") {
		gzipReader, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %s", err)
		}
		logger.Debugf("Using gzip decompression for file: %s", key)
		return gzipReader, nil
	}

	// No compression detected, return body as-is
	return body, nil
}

// CloseConnection closes the S3 client connection
func (s *S3) CloseConnection() {
	logger.Info("Closing S3 connection")
	// S3 client doesn't require explicit cleanup
}
