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
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/parser"
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

	// Create appropriate parser and infer schema (format-specific logic from parser package)
	switch s.config.FileFormat {
	case FormatCSV:
		// Get reader for CSV file (S3-specific: handles S3 API, decompression)
		var reader io.Reader
		var csvErr error
		reader, _, csvErr = s.getFileReader(ctx, firstFile.FileKey)
		if csvErr != nil {
			return nil, fmt.Errorf("failed to get file reader: %w", csvErr)
		}
		defer func() {
			if closer, ok := reader.(io.Closer); ok {
				closer.Close()
			}
		}()
		
		csvParser := parser.NewCSVParser(*s.config.GetCSVConfig(), stream)
		inferredStream, err = csvParser.InferSchema(ctx, reader)
	case FormatJSON:
		// Get reader for JSON file (S3-specific: handles S3 API, decompression)
		var reader io.Reader
		var jsonErr error
		reader, _, jsonErr = s.getFileReader(ctx, firstFile.FileKey)
		if jsonErr != nil {
			return nil, fmt.Errorf("failed to get file reader: %w", jsonErr)
		}
		defer func() {
			if closer, ok := reader.(io.Closer); ok {
				closer.Close()
			}
		}()
		
		jsonParser := parser.NewJSONParser(*s.config.GetJSONConfig(), stream)
		inferredStream, err = jsonParser.InferSchema(ctx, reader)
	case FormatParquet:
		// For Parquet, we need a special reader that implements io.ReaderAt
		// Use the firstFile.Size that we got from S3 discovery
		var parquetReader io.ReaderAt
		var parquetSize int64
		var parquetErr error
		parquetReader, parquetSize, parquetErr = s.getParquetReaderAt(ctx, firstFile.FileKey, firstFile.Size)
		if parquetErr != nil {
			return nil, fmt.Errorf("failed to get Parquet reader: %w", parquetErr)
		}
		defer func() {
			if closer, ok := parquetReader.(io.Closer); ok {
				closer.Close()
			}
		}()
		
		// Create a wrapper that implements both io.ReaderAt and provides size info
		wrapper := &parquetReaderWrapper{
			readerAt: parquetReader,
			size:     parquetSize,
		}
		
		parquetParser := parser.NewParquetParser(*s.config.GetParquetConfig(), stream)
		inferredStream, err = parquetParser.InferSchema(ctx, wrapper)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to infer schema: %w", err)
	}

	// Add _olake_last_modified as a cursor field for incremental sync
	inferredStream.UpsertField("_olake_last_modified", types.String, false)
	inferredStream.WithCursorField("_olake_last_modified")

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

// getFileReader returns a reader for an S3 file with decompression applied (S3-specific logic)
// Returns (reader, fileSize, error)
func (s *S3) getFileReader(ctx context.Context, key string) (io.Reader, int64, error) {
	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get object from S3: %w", err)
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
		return nil, 0, fmt.Errorf("failed to create decompressed reader: %w", err)
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
		return nil, 0, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read Parquet file: %w", err)
	}

	return bytes.NewReader(data), int64(len(data)), nil
}

// parquetReaderWrapper wraps an io.ReaderAt with size info and implements io.Seeker
// This allows the Parquet parser to determine file size via Seek
type parquetReaderWrapper struct {
	readerAt io.ReaderAt
	size     int64
	offset   int64
}

func (w *parquetReaderWrapper) ReadAt(p []byte, off int64) (n int, err error) {
	return w.readerAt.ReadAt(p, off)
}

func (w *parquetReaderWrapper) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		w.offset = offset
	case io.SeekCurrent:
		w.offset += offset
	case io.SeekEnd:
		w.offset = w.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if w.offset < 0 {
		w.offset = 0
	}
	if w.offset > w.size {
		w.offset = w.size
	}

	return w.offset, nil
}

func (w *parquetReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = w.readerAt.ReadAt(p, w.offset)
	w.offset += int64(n)
	return n, err
}

// getReader returns an appropriate reader based on file extension (S3 method wrapper)
// Auto-detects compression from file extension (.gz)
func (s *S3) getReader(body io.Reader, key string) (io.Reader, error) {
	return getDecompressedReader(body, key)
}

// getDecompressedReader returns an appropriate reader based on file extension
// Auto-detects compression from file extension (.gz)
func getDecompressedReader(body io.Reader, key string) (io.Reader, error) {
	lowerKey := strings.ToLower(key)

	// Check if file has gzip extension
	if strings.HasSuffix(lowerKey, ".gz") {
		gzipReader, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
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
