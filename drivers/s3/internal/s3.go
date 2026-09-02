package driver

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/s3/internal/pkg/parser"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	// lastModifiedField is the S3 metadata field name used for tracking file modification timestamps for incremental sync
	lastModifiedField = "_last_modified_time"
)

// S3 represents the S3 source driver
type S3 struct {
	store           objstorage.Store
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

	// Build the storage client (AWS S3 or any S3-compatible endpoint)
	store, err := objstorage.NewS3Store(ctx, objstorage.S3Config{
		Bucket:          s.config.BucketName,
		Region:          s.config.Region,
		AccessKeyID:     s.config.AccessKeyID,
		SecretAccessKey: s.config.SecretAccessKey,
		Endpoint:        s.config.Endpoint,
	})
	if err != nil {
		return err
	}
	s.store = store

	// Test connection by checking if bucket exists and is accessible
	logger.Infof("Testing connection to bucket: %s", s.config.BucketName)
	if err := s.store.Check(ctx); err != nil {
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
func (s *S3) GetStreamNames(ctx context.Context) ([]types.StreamID, error) {
	logger.Infof("Discovering files in bucket: %s with prefix: %s", s.config.BucketName, s.config.PathPrefix)

	// Initialize the map for grouped files
	filesByStream := make(map[string][]FileObject)
	totalDiscovered := 0

	// List all objects with the given prefix (pagination handled by the store)
	// Note: We accumulate all file metadata before processing because:
	// 1. File metadata is small (~200 bytes per file, 1M files = ~200MB)
	// 2. Chunking requires full file list to group files into ~2GB chunks
	// 3. Incremental sync needs to filter across all files by LastModified
	err := s.store.List(ctx, s.config.PathPrefix, func(obj objstorage.ObjectInfo) error {
		// Skip directories (keys ending with /)
		if strings.HasSuffix(obj.Key, "/") {
			return nil
		}

		// Apply file pattern filter if configured
		if s.filePattern != nil && !s.filePattern.MatchString(obj.Key) {
			logger.Debugf("Skipping file %s (does not match pattern)", obj.Key)
			return nil
		}

		// Filter by file extension based on format
		if !s.matchesFileFormat(obj.Key) {
			logger.Debugf("Skipping file %s (does not match format)", obj.Key)
			return nil
		}

		fileObj := FileObject{
			FileKey:      obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified.UTC().Format("2006-01-02T15:04:05Z"),
			ETag:         obj.ETag,
		}

		// Group files by stream name (folder or individual file)
		streamName := s.extractStreamName(obj.Key)
		filesByStream[streamName] = append(filesByStream[streamName], fileObj)
		totalDiscovered++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in bucket: %w", err)
	}

	logger.Infof("Completed S3 discovery: discovered %d files", totalDiscovered)

	// Store grouped files
	s.discoveredFiles = filesByStream

	// Extract stream names
	streamNames := make([]types.StreamID, 0, len(filesByStream))
	totalFiles := 0
	for streamName, files := range filesByStream {
		streamNames = append(streamNames, types.StreamID{Namespace: "s3", Name: streamName})
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

	extensions, ok := formatExtensions[s.config.FileFormat]
	if !ok {
		return false
	}
	for _, extension := range extensions {
		if strings.HasSuffix(lowerKey, extension) {
			return true
		}
		if s.config.FileFormat != FormatParquet &&
			s.config.Compression == CompressionGzip &&
			strings.HasSuffix(lowerKey, extension+".gz") {
			return true
		}
	}
	return false
}

// ProduceSchema generates schema for a given stream (folder or file)
func (s *S3) ProduceSchema(ctx context.Context, streamID types.StreamID) (*types.Stream, error) {
	logger.Infof("Producing schema for stream: %s", streamID)

	// Get files for this stream
	files, exists := s.discoveredFiles[streamID.Name]
	if !exists || len(files) == 0 {
		return nil, errs.Precondition(errs.ObjectNotFound, codeNoFilesForStream,
			fmt.Errorf("no files found for stream: %s", streamID.Name))
	}

	// Create stream
	stream := types.NewStream(streamID.Name, streamID.Namespace, &s.config.BucketName)

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
	case FormatXML:
		inferredStream, err = s.inferSchemaForXML(ctx, firstFile, stream)
	default:
		return nil, errs.Precondition(errs.ConfigInvalid, codeUnsupportedFileFormat,
			fmt.Errorf("unsupported file format: %s", s.config.FileFormat))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to infer schema: %w", err)
	}

	// Add _last_modified_time as a cursor field for incremental sync
	inferredStream.UpsertField(lastModifiedField, types.String, false, false)
	inferredStream.WithCursorField(lastModifiedField)

	inferredStream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
	return inferredStream, nil
}

// withFileReader is a helper that manages file reader lifecycle for CSV/JSON/XML formats
// It acquires a reader, ensures cleanup, and executes the provided callback
func (s *S3) withFileReader(ctx context.Context, fileKey string, callback func(io.Reader) (*types.Stream, error)) (*types.Stream, error) {
	reader, err := s.getFileReader(ctx, fileKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get file reader: %w", err)
	}
	defer reader.Close()

	return callback(reader)
}

// withParquetReader is a helper that manages Parquet reader lifecycle
// It acquires a ReaderAt, ensures cleanup, and executes the provided callback
func (s *S3) withParquetReader(ctx context.Context, fileKey string, fileSize int64, callback func(io.Reader) (*types.Stream, error)) (*types.Stream, error) {
	parquetReader, parquetSize, err := s.getParquetReaderAt(ctx, fileKey, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get Parquet reader: %w", err)
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

// inferSchemaForXML infers schema from an XML file
func (s *S3) inferSchemaForXML(ctx context.Context, file FileObject, stream *types.Stream) (*types.Stream, error) {
	return s.withFileReader(ctx, file.FileKey, func(reader io.Reader) (*types.Stream, error) {
		xmlParser := parser.NewXMLParser(*s.config.GetXMLConfig(), stream)
		return xmlParser.InferSchema(ctx, reader)
	})
}

// getFileReader returns a reader for a file with decompression applied.
// Closing the returned reader also closes the underlying object body.
func (s *S3) getFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	// Get the object from storage (%w keeps objstorage.ErrNotFound matchable)
	body, err := s.store.Open(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}

	// Apply decompression if needed (auto-detect from file extension)
	reader, err := getDecompressedReader(body, key)
	if err != nil {
		body.Close()
		return nil, fmt.Errorf("failed to create decompressed reader: %w", err)
	}

	return reader, nil
}

// getParquetReaderAt returns a reader suitable for Parquet files (io.ReaderAt)
// Uses S3 range reader for streaming or loads into memory based on config
// Returns (readerAt, fileSize, error)
func (s *S3) getParquetReaderAt(ctx context.Context, key string, fileSize int64) (io.ReaderAt, int64, error) {
	parquetConfig := s.config.GetParquetConfig()

	if parquetConfig.StreamingEnabled && fileSize > 0 {
		// Use S3 range requests for streaming (memory-efficient). Ranged reads
		// live on RangeOpener, not the core Store, so assert the capability.
		ranger, ok := s.store.(objstorage.RangeOpener)
		if !ok {
			return nil, 0, fmt.Errorf("configured store does not support ranged reads required for streaming Parquet")
		}
		logger.Debugf("Using S3 range requests for Parquet file: %s", key)
		return objstorage.NewReaderAt(ctx, ranger, key, fileSize), fileSize, nil
	}

	// Fallback: Load entire file into memory (%w keeps objstorage.ErrNotFound matchable)
	logger.Debugf("Loading Parquet file into memory: %s", key)
	body, err := s.store.Open(ctx, key)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read Parquet file: %w", err)
	}

	return bytes.NewReader(data), int64(len(data)), nil
}

// getDecompressedReader returns an appropriate reader based on file extension
// Auto-detects compression from file extension (.gz)
// Closing the returned reader closes both the decompressor and the body
func getDecompressedReader(body io.ReadCloser, key string) (io.ReadCloser, error) {
	lowerKey := strings.ToLower(key)

	// Check if file has gzip extension
	if strings.HasSuffix(lowerKey, ".gz") {
		gzipReader, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		logger.Debugf("Using gzip decompression for file: %s", key)
		// gzip.Reader.Close does not close the underlying body; close both
		return utils.NewCompositeReadCloser(gzipReader, gzipReader, body), nil
	}

	// No compression detected, return body as-is
	return body, nil
}

// CloseConnection closes the S3 client connection
func (s *S3) CloseConnection() {
	logger.Info("Closing S3 connection")
	// S3 client doesn't require explicit cleanup
}
