package driver

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/parser"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// ============================================
// Backfill: Chunk Management & Processing
// ============================================
// This file contains all backfill-related logic for processing discovered files

// GetOrSplitChunks returns chunks for parallel processing of files
// Groups multiple small files into ~2GB chunks to reduce state size
// Large files (>2GB) are kept as individual chunks
func (s *S3) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	streamName := stream.Name()
	files, exists := s.discoveredFiles[streamName]

	if !exists || len(files) == 0 {
		logger.Warnf("No files found for stream %s, returning empty chunk set", streamName)
		return types.NewSet[types.Chunk](), nil
	}

	logger.Infof("Backfill: Processing %d files for stream %s", len(files), streamName)

	// Calculate total file size across all files
	var totalSize int64
	for _, file := range files {
		totalSize += file.Size
	}

	// Convert to human-readable format
	sizeInMB := float64(totalSize) / (1024 * 1024)
	var sizeDisplay string
	if sizeInMB < 1 {
		sizeDisplay = fmt.Sprintf("%.2f KB", float64(totalSize)/1024)
	} else if sizeInMB < 1024 {
		sizeDisplay = fmt.Sprintf("%.2f MB", sizeInMB)
	} else {
		sizeDisplay = fmt.Sprintf("%.2f GB", sizeInMB/1024)
	}

	// Use file count as a proxy for sync stats instead of estimated rows
	pool.AddRecordsToSyncStats(int64(len(files)))

	// Group files into chunks based on size to optimize state storage and parallel processing
	chunks := s.groupFilesIntoChunks(files)

	logger.Infof("Creating %d chunks for stream %s (total files: %d, total size: %s)",
		chunks.Len(), streamName, len(files), sizeDisplay)

	return chunks, nil
}

// groupFilesIntoChunks groups multiple small files into ~2GB chunks while keeping large files separate
// This reduces state size (fewer chunks to track) while maintaining parallel processing efficiency
func (s *S3) groupFilesIntoChunks(files []FileObject) *types.Set[types.Chunk] {
	// Use EffectiveParquetSize (2GB) as target chunk size for grouping files
	// This aligns with Parquet file sizing strategy across the codebase
	targetChunkSize := constants.EffectiveParquetSize
	largeFileThreshold := targetChunkSize // Files > 2GB get their own chunk

	chunks := types.NewSet[types.Chunk]()
	var currentChunkFiles []string
	var currentChunkSize int64

	// Helper to flush accumulated files into a chunk
	flushChunk := func() {
		if len(currentChunkFiles) > 0 {
			chunks.Insert(types.Chunk{
				Min: currentChunkFiles, // Array of file keys
				Max: currentChunkSize,  // Total size of files in this chunk
			})
			logger.Debugf("Created chunk with %d files (%d MB)",
				len(currentChunkFiles), currentChunkSize/(1024*1024))
			currentChunkFiles = nil
			currentChunkSize = 0
		}
	}

	for _, file := range files {
		// Large files get their own chunk (single file per chunk)
		if file.Size >= largeFileThreshold {
			// Flush any accumulated small files first
			flushChunk()

			// Add large file as separate chunk (array with single element for consistency)
			chunks.Insert(types.Chunk{
				Min: []string{file.FileKey}, // Array of one file key (consistent with small files)
				Max: file.Size,              // File size
			})
			logger.Debugf("Large file (%d MB) in separate chunk: %s",
				file.Size/(1024*1024), file.FileKey)
			continue
		}

		// Group small files into chunks
		if currentChunkSize+file.Size > targetChunkSize && len(currentChunkFiles) > 0 {
			// Current chunk would exceed target size, flush it
			flushChunk()
		}

		// Add file to current chunk
		currentChunkFiles = append(currentChunkFiles, file.FileKey)
		currentChunkSize += file.Size
	}

	// Flush remaining files
	flushChunk()

	return chunks
}

// ChunkIterator reads and processes records from S3 file(s) in a chunk
// Handles chunks as arrays (Min = []string): single file = array size 1, grouped files = array size > 1
func (s *S3) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	// Convert chunk.Min to []string, handling both []string and []interface{} (from state deserialization)
	var fileKeys []string

	switch v := chunk.Min.(type) {
	case []string:
		// Direct string array - most common case
		fileKeys = v

	case []interface{}:
		// Handle []interface{} from state deserialization after restart
		// JSON unmarshaling converts []string to []interface{} when Min is type `any`
		fileKeys = make([]string, len(v))
		for i, item := range v {
			key, ok := item.(string)
			if !ok {
				return fmt.Errorf("invalid file key type in chunk: expected string, got %T", item)
			}
			fileKeys[i] = key
		}

	default:
		return fmt.Errorf("invalid chunk Min type: expected []string, got %T", chunk.Min)
	}

	// Process all files in the chunk
	// Single large file: array size = 1
	// Grouped small files: array size > 1
	logger.Infof("Processing chunk with %d file(s)", len(fileKeys))

	for i, key := range fileKeys {
		logger.Debugf("Processing file %d/%d in chunk: %s", i+1, len(fileKeys), key)
		fileSize := s.getFileSize(stream.Name(), key)
		lastModified := s.getFileLastModified(stream.Name(), key)
		if err := s.processFile(ctx, stream, key, fileSize, lastModified, processFn); err != nil {
			return fmt.Errorf("failed to process file %s in chunk: %s", key, err)
		}
	}

	return nil
}

// processFile handles the processing of a single S3 file using the parser package
// lastModified is passed as parameter to avoid redundant file metadata lookups
func (s *S3) processFile(ctx context.Context, stream types.StreamInterface, key string, fileSize int64, lastModified string, processFn abstract.BackfillMsgFn) error {
	// For Parquet streaming, use S3 range requests (no need to load entire file into memory)
	if s.config.FileFormat == FormatParquet {
		parquetConfig := s.config.GetParquetConfig()
		if parquetConfig.StreamingEnabled && fileSize > 0 {
			// Create S3 range reader for streaming
			logger.Infof("Processing Parquet file with streaming (S3 range requests): %s (size: %.2f MB)",
				key, float64(fileSize)/(1024*1024))
			rangeReader := NewS3RangeReader(ctx, s.client, s.config.BucketName, key, fileSize)
			wrapper := parser.NewParquetReaderWrapper(rangeReader, fileSize)
			return s.parseFileWithReader(ctx, stream, key, wrapper, lastModified, processFn)
		}
	}

	// For non-streaming paths, get reader for the file (S3-specific: handles S3 API, decompression)
	reader, _, err := s.getFileReader(ctx, key)
	if err != nil {
		// Check if file was deleted between discovery and processing
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
			logger.Warnf("File %s was deleted or not found, skipping", key)
			return nil // Don't fail the entire sync for a missing file
		}
		return fmt.Errorf("failed to get reader: %s", err)
	}
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	return s.parseFileWithReader(ctx, stream, key, reader, lastModified, processFn)
}

// parseFileWithReader handles the common parsing logic for all file formats
// This eliminates duplicate code between streaming and non-streaming paths
func (s *S3) parseFileWithReader(ctx context.Context, stream types.StreamInterface, key string, reader io.Reader, lastModified string, processFn abstract.BackfillMsgFn) error {
	// Create callback adapter - add _last_modified_time field to each record
	callback := func(ctx context.Context, record map[string]any) error {
		// Inject LastModified timestamp into each record
		record[lastModifiedField] = lastModified
		return processFn(ctx, record)
	}

	// Convert StreamInterface to underlying Stream for parser
	underlyingStream := &types.Stream{
		Name:      stream.Name(),
		Namespace: stream.Namespace(),
		Schema:    stream.Schema(),
	}

	// Use appropriate parser based on file format
	var parseErr error
	switch s.config.FileFormat {
	case FormatCSV:
		csvParser := parser.NewCSVParser(*s.config.GetCSVConfig(), underlyingStream)
		parseErr = csvParser.StreamRecords(ctx, reader, callback)
	case FormatJSON:
		jsonParser := parser.NewJSONParser(*s.config.GetJSONConfig(), underlyingStream)
		parseErr = jsonParser.StreamRecords(ctx, reader, callback)
	case FormatParquet:
		parquetParser := parser.NewParquetParser(*s.config.GetParquetConfig(), underlyingStream)
		parseErr = parquetParser.StreamRecords(ctx, reader, callback)
	default:
		return fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}

	if parseErr != nil {
		return fmt.Errorf("failed to process file %s: %s", key, parseErr)
	}

	return nil
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

// getFileLastModified retrieves the LastModified timestamp from discovered files
// Returns empty string if file not found
func (s *S3) getFileLastModified(streamName, fileKey string) string {
	files, exists := s.discoveredFiles[streamName]
	if !exists {
		return ""
	}

	for _, file := range files {
		if file.FileKey == fileKey {
			return file.LastModified
		}
	}

	return ""
}
