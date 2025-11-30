package driver

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/parser"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	pq "github.com/parquet-go/parquet-go"
)

// ============================================
// SECTION 1: Cursor & Filtering
// ============================================
// This section handles the unified cursor-based filtering logic
// that enables both backfill (cursor="") and incremental (cursor=timestamp) modes

// getCursorFromState retrieves the cursor timestamp from state
// Returns empty string for backfill (no state), or the last synced timestamp for incremental
//
// Backfill mode: No cursor configured or no previous state → returns ""
// Incremental mode: Cursor exists in state → returns timestamp (e.g., "2024-01-02T10:00:00Z")
func (s *S3) getCursorFromState(stream types.StreamInterface) string {
	// Try to cast to ConfiguredStream to access state
	configuredStream, ok := stream.(*types.ConfiguredStream)
	if !ok {
		// Not a configured stream, this is backfill - return empty string (process all files)
		logger.Debugf("Stream %s is not configured, using backfill mode (cursor: epoch)", stream.Name())
		return ""
	}

	// Get primary cursor field from stream configuration
	primaryCursor, _ := configuredStream.Cursor()
	if primaryCursor == "" {
		// No cursor configured, this is backfill
		logger.Debugf("Stream %s has no cursor configured, using backfill mode (cursor: epoch)", stream.Name())
		return ""
	}

	// Get cursor value from state
	cursorValue := s.state.GetCursor(configuredStream, primaryCursor)
	if cursorValue == nil {
		// First sync - return empty string (process all files)
		logger.Infof("Stream %s has no previous state, using backfill mode (cursor: epoch)", stream.Name())
		return ""
	}

	// Parse cursor as string (timestamp format: 2006-01-02T15:04:05Z)
	lastSynced, ok := cursorValue.(string)
	if !ok {
		logger.Warnf("Invalid cursor format in state for stream %s, using backfill mode", stream.Name())
		return ""
	}

	logger.Infof("Stream %s using incremental mode (cursor: %s)", stream.Name(), lastSynced)
	return lastSynced
}

// filterFilesByCursor filters files based on LastModified timestamp cursor
// This is the core logic that unifies backfill and incremental sync:
//
// For backfill (empty cursor):     Returns all files
// For incremental (cursor != ""):  Returns only files where LastModified > cursor
//
// The cursor timestamp is in ISO 8601 format (2006-01-02T15:04:05Z) and uses
// string comparison which works correctly for this format.
func (s *S3) filterFilesByCursor(files []FileObject, cursorTimestamp string) []FileObject {
	// If no cursor (backfill), return all files
	if cursorTimestamp == "" {
		logger.Debugf("No cursor timestamp, processing all %d files (backfill mode)", len(files))
		return files
	}

	// Filter files modified after cursor
	var filteredFiles []FileObject
	for _, file := range files {
		if file.LastModified > cursorTimestamp {
			filteredFiles = append(filteredFiles, file)
			logger.Debugf("File %s will be processed (modified: %s > cursor: %s)", 
				file.FileKey, file.LastModified, cursorTimestamp)
		} else {
			logger.Debugf("File %s skipped (modified: %s <= cursor: %s)", 
				file.FileKey, file.LastModified, cursorTimestamp)
		}
	}

	logger.Infof("Filtered %d files to process out of %d total (incremental mode, cursor: %s)", 
		len(filteredFiles), len(files), cursorTimestamp)
	return filteredFiles
}

// ============================================
// SECTION 2: Chunk Management
// ============================================
// This section handles grouping files into optimized chunks for parallel processing

// GetOrSplitChunks returns chunks for parallel processing of files
// Unified implementation: filters files by cursor (epoch for backfill, state value for incremental)
// Groups multiple small files into ~2GB chunks to reduce state size
// Large files (>2GB) are kept as individual chunks
func (s *S3) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	streamName := stream.Name()
	files, exists := s.discoveredFiles[streamName]

	if !exists || len(files) == 0 {
		logger.Warnf("No files found for stream %s, returning empty chunk set", streamName)
		return types.NewSet[types.Chunk](), nil
	}

	// Get cursor from state for incremental filtering
	// For backfill (no state), this returns empty string which means all files pass the filter
	cursorTimestamp := s.getCursorFromState(stream)
	
	// Filter files based on LastModified cursor
	filesToProcess := s.filterFilesByCursor(files, cursorTimestamp)
	
	if len(filesToProcess) == 0 {
		logger.Infof("No files to process for stream %s after cursor filtering (cursor: %s)", streamName, cursorTimestamp)
		return types.NewSet[types.Chunk](), nil
	}

	logger.Infof("Processing %d files for stream %s (filtered from %d total, cursor: %s)", 
		len(filesToProcess), streamName, len(files), cursorTimestamp)

	// Calculate total file size across files to process
	var totalSize int64
	for _, file := range filesToProcess {
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
	pool.AddRecordsToSyncStats(int64(len(filesToProcess)))

	// Group files into chunks based on size to optimize state storage and parallel processing
	chunks := s.groupFilesIntoChunks(filesToProcess)

	logger.Infof("Creating %d chunks for stream %s (total files: %d, total size: %s)",
		chunks.Len(), streamName, len(filesToProcess), sizeDisplay)

	return chunks, nil
}

// groupFilesIntoChunks groups multiple small files into ~2GB chunks while keeping large files separate
// This reduces state size (fewer chunks to track) while maintaining parallel processing efficiency
func (s *S3) groupFilesIntoChunks(files []FileObject) *types.Set[types.Chunk] {
	const targetChunkSize int64 = 2 * 1024 * 1024 * 1024 // 2GB target chunk size
	const largeFileThreshold int64 = targetChunkSize      // Files > 2GB get their own chunk

	chunks := types.NewSet[types.Chunk]()
	var currentChunkFiles []string
	var currentChunkSize int64

	for _, file := range files {
		// Large files get their own chunk (single file per chunk)
		if file.Size >= largeFileThreshold {
			// Flush any accumulated small files first
			if len(currentChunkFiles) > 0 {
				chunks.Insert(types.Chunk{
					Min: currentChunkFiles,  // Array of file keys
					Max: currentChunkSize,   // Total size of files in this chunk
				})
				currentChunkFiles = nil
				currentChunkSize = 0
			}

			// Add large file as separate chunk (array with single element for consistency)
			chunks.Insert(types.Chunk{
				Min: []string{file.FileKey},  // Array of one file key (consistent with small files)
				Max: file.Size,               // File size
			})
			logger.Debugf("Large file (%d MB) in separate chunk: %s", 
				file.Size/(1024*1024), file.FileKey)
			continue
		}

		// Group small files into chunks
		if currentChunkSize+file.Size > targetChunkSize && len(currentChunkFiles) > 0 {
			// Current chunk would exceed target size, flush it
			chunks.Insert(types.Chunk{
				Min: currentChunkFiles,  // Array of file keys
				Max: currentChunkSize,   // Total size of files in this chunk
			})
			logger.Debugf("Created chunk with %d files (%d MB)", 
				len(currentChunkFiles), currentChunkSize/(1024*1024))
			currentChunkFiles = nil
			currentChunkSize = 0
		}

		// Add file to current chunk
		currentChunkFiles = append(currentChunkFiles, file.FileKey)
		currentChunkSize += file.Size
	}

	// Flush remaining files
	if len(currentChunkFiles) > 0 {
		chunks.Insert(types.Chunk{
			Min: currentChunkFiles,  // Array of file keys
			Max: currentChunkSize,   // Total size of files in this chunk
		})
		logger.Debugf("Created final chunk with %d files (%d MB)", 
			len(currentChunkFiles), currentChunkSize/(1024*1024))
	}

	return chunks
}

// ============================================
// SECTION 3: Chunk Processing
// ============================================
// This section handles reading and processing records from S3 files

// ChunkIterator reads and processes records from an S3 file or multiple files
// Handles chunks as arrays (Min = []string): single file = array size 1, grouped files = array size > 1
func (s *S3) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	// Check if this is a typed array or interface array (from JSON state deserialization)
	switch v := chunk.Min.(type) {
	case []string:
		// Chunk with file(s) - process each file sequentially
		// Single large file: array size = 1
		// Grouped small files: array size > 1
		// TODO: Consider parallelizing file processing within chunks for better performance
		// OR simplify by making each file its own chunk (remove grouping logic entirely)
		logger.Infof("Processing chunk with %d file(s)", len(v))
		for i, key := range v {
			logger.Debugf("Processing file %d/%d in chunk: %s", i+1, len(v), key)
			fileSize := s.getFileSize(stream.Name(), key)
			if err := s.processFile(ctx, stream, key, fileSize, processFn); err != nil {
				return fmt.Errorf("failed to process file %s in chunk: %w", key, err)
			}
		}
		return nil

	case []interface{}:
		// Handle []interface{} from state deserialization after restart
		// JSON unmarshaling converts []string to []interface{} when Min is type `any`
		logger.Infof("Processing chunk with %d file(s) (from state)", len(v))
		for i, item := range v {
			key, ok := item.(string)
			if !ok {
				return fmt.Errorf("invalid file key type in chunk: expected string, got %T", item)
			}
			logger.Debugf("Processing file %d/%d in chunk: %s", i+1, len(v), key)
			fileSize := s.getFileSize(stream.Name(), key)
			if err := s.processFile(ctx, stream, key, fileSize, processFn); err != nil {
				return fmt.Errorf("failed to process file %s in chunk: %w", key, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("invalid chunk Min type: expected []string, got %T", chunk.Min)
	}
}

// processFile handles the processing of a single S3 file using the parser package
func (s *S3) processFile(ctx context.Context, stream types.StreamInterface, key string, fileSize int64, processFn abstract.BackfillMsgFn) error {
	// Get reader for the file (S3-specific: handles S3 API, decompression, range requests)
	reader, err := s.getStreamReader(ctx, key, fileSize)
	if err != nil {
		// Check if file was deleted between discovery and processing
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
			logger.Warnf("File %s was deleted or not found, skipping", key)
			return nil // Don't fail the entire sync for a missing file
		}
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Create callback adapter from abstract.BackfillMsgFn to parser.RecordCallback
	callback := func(ctx context.Context, record map[string]any) error {
		return processFn(ctx, record)
	}

	// Use appropriate parser based on file format
	// Note: stream is StreamInterface, we need to get the underlying Stream for the parser
	underlyingStream := &types.Stream{
		Name:      stream.Name(),
		Namespace: stream.Namespace(),
		Schema:    stream.Schema(),
	}
	
	var parseErr error
	switch s.config.FileFormat {
	case FormatCSV:
		csvParser := parser.NewCSVParser(*s.config.GetCSVConfig(), underlyingStream)
		parseErr = csvParser.StreamRecords(ctx, reader, s.config.BatchSize, callback)
	case FormatJSON:
		jsonParser := parser.NewJSONParser(*s.config.GetJSONConfig(), underlyingStream)
		parseErr = jsonParser.StreamRecords(ctx, reader, s.config.BatchSize, callback)
	case FormatParquet:
		// For Parquet, use the streaming implementation with range requests if enabled
		parquetConfig := s.config.GetParquetConfig()
		if parquetConfig.StreamingEnabled && fileSize > 0 {
			// Use S3 range reader for streaming
			return s.processParquetFileStreamingWithParser(ctx, stream, key, fileSize, processFn)
		}
		// Fallback: load into memory and parse
		parquetParser := parser.NewParquetParser(*parquetConfig, underlyingStream)
		parseErr = parquetParser.StreamRecords(ctx, reader, s.config.BatchSize, callback)
	default:
		return fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}

	if parseErr != nil {
		return fmt.Errorf("failed to process file %s: %w", key, parseErr)
	}

	return nil
}

// getStreamReader returns a reader for streaming file data (S3-specific logic)
func (s *S3) getStreamReader(ctx context.Context, key string, fileSize int64) (io.Reader, error) {
	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}

	// Apply decompression if needed (auto-detect from file extension)
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		result.Body.Close()
		return nil, fmt.Errorf("failed to create decompressed reader: %w", err)
	}

	return reader, nil
}

// processParquetFileStreamingWithParser uses S3 range requests with the parser package
func (s *S3) processParquetFileStreamingWithParser(ctx context.Context, stream types.StreamInterface, key string, fileSize int64, processFn abstract.BackfillMsgFn) error {
	logger.Infof("Processing Parquet file with streaming (S3 range requests): %s (size: %.2f MB)",
		key, float64(fileSize)/(1024*1024))

	// Create S3 range reader
	rangeReader := NewS3RangeReader(ctx, s.client, s.config.BucketName, key, fileSize)
	
	// Wrap reader to make it compatible with parser requirements
	wrapper := &parquetReaderWrapper{
		readerAt: rangeReader,
		size:     fileSize,
	}

	// Create callback adapter
	callback := func(ctx context.Context, record map[string]any) error {
		return processFn(ctx, record)
	}

	// Use parser to stream records
	// Note: stream is StreamInterface, we need to get the underlying Stream for the parser
	underlyingStream := &types.Stream{
		Name:      stream.Name(),
		Namespace: stream.Namespace(),
		Schema:    stream.Schema(),
	}
	
	parquetParser := parser.NewParquetParser(*s.config.GetParquetConfig(), underlyingStream)
	return parquetParser.StreamRecords(ctx, wrapper, s.config.BatchSize, callback)
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

// ============================================
// SECTION 4: Format-Specific Processors
// ============================================
// This section contains processors for different file formats: CSV, JSON, and Parquet

// processCSVFile reads and processes a CSV file
func (s *S3) processCSVFile(ctx context.Context, reader io.Reader, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	csvConfig := s.config.GetCSVConfig()
	
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(csvConfig.Delimiter[0])
	if csvConfig.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < csvConfig.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	if csvConfig.HasHeader {
		// Read header row
		var err error
		headers, err = csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV headers: %w", err)
		}
	} else {
		// Generate default column names based on first row
		firstRow, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read first row: %w", err)
		}
		for i := range firstRow {
			headers = append(headers, fmt.Sprintf("column_%d", i))
		}
		// Process the first row as data
		record := make(map[string]any)
		for i, value := range firstRow {
			if i < len(headers) {
				fieldType, _ := stream.Schema().GetType(headers[i])
				convertedValue := s.convertValue(value, fieldType)
				record[headers[i]] = convertedValue
			}
		}
		if err := processFn(ctx, record); err != nil {
			return fmt.Errorf("failed to process first record: %w", err)
		}
	}

	// Process records in batches
	batch := make([]map[string]any, 0, s.config.BatchSize)
	recordCount := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Warnf("Error reading CSV row %d: %v", recordCount, err)
			continue
		}

		// Convert row to map
		record := make(map[string]any)
		for i, value := range row {
			if i < len(headers) {
				// Convert value based on schema type
				fieldType, _ := stream.Schema().GetType(headers[i])
				convertedValue := s.convertValue(value, fieldType)
				record[headers[i]] = convertedValue
			}
		}

		batch = append(batch, record)
		recordCount++

		// Process batch when it reaches batch size
		if len(batch) >= s.config.BatchSize {
			for _, rec := range batch {
				if err := processFn(ctx, rec); err != nil {
					return fmt.Errorf("failed to process record: %w", err)
				}
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Process remaining records in batch
	for _, rec := range batch {
		if err := processFn(ctx, rec); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
	}

	logger.Infof("Processed %d records from CSV file", recordCount)
	return nil
}

// processJSONFile reads and processes a JSON file
func (s *S3) processJSONFile(ctx context.Context, reader io.Reader, processFn abstract.BackfillMsgFn) error {
	jsonConfig := s.config.GetJSONConfig()
	recordCount := 0

	if jsonConfig.LineDelimited {
		// Line-delimited JSON (JSONL)
		decoder := json.NewDecoder(reader)

		for {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var record map[string]interface{}
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				logger.Warnf("Error reading JSON record %d: %v", recordCount, err)
				continue
			}

			if err := processFn(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}
	} else {
		// JSON array - stream elements one at a time to avoid loading entire array in memory
		decoder := json.NewDecoder(reader)

		// Read opening bracket '['
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read opening bracket: %w", err)
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return fmt.Errorf("expected JSON array, got: %v", t)
		}

		// Stream array elements one by one
		for decoder.More() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var record map[string]interface{}
			if err := decoder.Decode(&record); err != nil {
				return fmt.Errorf("failed to decode JSON record: %w", err)
			}

			if err := processFn(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}

		// Read closing bracket ']'
		t, err = decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read closing bracket: %w", err)
		}
		if delim, ok := t.(json.Delim); !ok || delim != ']' {
			return fmt.Errorf("expected closing bracket, got: %v", t)
		}
	}

	logger.Infof("Processed %d records from JSON file", recordCount)
	return nil
}

// processParquetFile reads and processes a Parquet file
func (s *S3) processParquetFile(ctx context.Context, reader io.Reader, processFn abstract.BackfillMsgFn) error {
	// Read all data into memory (required for parquet-go)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Open parquet file
	pqFile, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Get schema to know column names
	schema := pqFile.Schema()
	fields := schema.Fields()
	columnNames := make([]string, len(fields))
	for i, field := range fields {
		columnNames[i] = field.Name()
	}
	
	recordCount := 0

	// Iterate through all row groups
	for _, rowGroup := range pqFile.RowGroups() {
		numRows := rowGroup.NumRows()
		
		// Read all columns into memory for this row group
		columnData := make([][]pq.Value, len(fields))
		for colIdx, columnChunk := range rowGroup.ColumnChunks() {
			// Read all values from this column
			pages := columnChunk.Pages()
			columnValues := make([]pq.Value, 0, numRows)
			
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to read page: %w", err)
				}
				
				// Read values from the page
				pageValues := make([]pq.Value, page.NumValues())
				_, err = page.Values().ReadValues(pageValues)
				if err != nil && err != io.EOF {
					return fmt.Errorf("failed to read page values: %w", err)
				}
				
				columnValues = append(columnValues, pageValues...)
			}
			pages.Close()
			
			columnData[colIdx] = columnValues
		}
		
		// Now process row by row
		for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Build the record from column values
			record := make(map[string]interface{})
			for colIdx, columnName := range columnNames {
				if rowIdx < int64(len(columnData[colIdx])) {
					record[columnName] = s.parquetValueToInterface(columnData[colIdx][rowIdx])
				}
			}

			// Process the record
			if err := processFn(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}
	}

	logger.Infof("Processed %d records from Parquet file", recordCount)
	return nil
}

// processParquetFileStreaming uses S3 range requests to stream Parquet files without loading entire file in memory
// This is the memory-efficient implementation addressing Comment 2 from PR review
func (s *S3) processParquetFileStreaming(ctx context.Context, key string, fileSize int64, processFn abstract.BackfillMsgFn) error {
	logger.Infof("Processing Parquet file with streaming (S3 range requests): %s (size: %.2f MB)", 
		key, float64(fileSize)/(1024*1024))

	// Create S3 range reader for streaming access
	rangeReader := NewS3RangeReader(ctx, s.client, s.config.BucketName, key, fileSize)

	// Open Parquet file using range reader (parquet-go will make range requests as needed)
	pqFile, err := pq.OpenFile(rangeReader, fileSize)
	if err != nil {
		return fmt.Errorf("failed to open parquet file with range reader: %w", err)
	}

	// Get schema to know column names
	schema := pqFile.Schema()
	fields := schema.Fields()
	columnNames := make([]string, len(fields))
	for i, field := range fields {
		columnNames[i] = field.Name()
	}

	recordCount := 0
	totalRowGroups := len(pqFile.RowGroups())

	logger.Infof("Parquet file has %d row groups, processing one at a time", totalRowGroups)

	// Process each row group sequentially (only one in memory at a time)
	for rgIdx, rowGroup := range pqFile.RowGroups() {
		logger.Debugf("Processing row group %d/%d (approx %d rows)", 
			rgIdx+1, totalRowGroups, rowGroup.NumRows())

		numRows := rowGroup.NumRows()

		// Read all columns into memory for THIS row group only
		// This limits memory to: one row group size (typically 64-128MB)
		columnData := make([][]pq.Value, len(fields))
		for colIdx, columnChunk := range rowGroup.ColumnChunks() {
			// Read all values from this column (via range requests)
			pages := columnChunk.Pages()
			columnValues := make([]pq.Value, 0, numRows)

			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to read page in row group %d: %w", rgIdx, err)
				}

				// Read values from the page
				pageValues := make([]pq.Value, page.NumValues())
				_, err = page.Values().ReadValues(pageValues)
				if err != nil && err != io.EOF {
					return fmt.Errorf("failed to read page values in row group %d: %w", rgIdx, err)
				}

				columnValues = append(columnValues, pageValues...)
			}
			pages.Close()

			columnData[colIdx] = columnValues
		}

		// Process rows from this row group
		for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Build the record from column values
			record := make(map[string]interface{})
			for colIdx, columnName := range columnNames {
				if rowIdx < int64(len(columnData[colIdx])) {
					record[columnName] = s.parquetValueToInterface(columnData[colIdx][rowIdx])
				}
			}

			// Process the record
			if err := processFn(ctx, record); err != nil {
				return fmt.Errorf("failed to process record in row group %d: %w", rgIdx, err)
			}
			recordCount++
		}

		// Row group data will be garbage collected here before processing next row group
		logger.Debugf("Completed row group %d/%d (%d records)", rgIdx+1, totalRowGroups, numRows)
	}

	logger.Infof("Completed streaming Parquet file: %s (%d records from %d row groups)", 
		key, recordCount, totalRowGroups)
	return nil
}

// parquetValueToInterface converts a parquet.Value to interface{}
func (s *S3) parquetValueToInterface(val pq.Value) interface{} {
	if val.IsNull() {
		return nil
	}

	// Convert based on the value's kind
	switch val.Kind() {
	case pq.Boolean:
		return val.Boolean()
	case pq.Int32:
		return int64(val.Int32())
	case pq.Int64:
		return val.Int64()
	case pq.Float:
		return float64(val.Float())
	case pq.Double:
		return val.Double()
	case pq.ByteArray:
		return string(val.ByteArray())
	default:
		// For other types, convert to string
		return val.String()
	}
}

// convertValue converts a string value to the appropriate type
func (s *S3) convertValue(value string, fieldType types.DataType) interface{} {
	if value == "" || value == "null" {
		return nil
	}

	switch fieldType {
	case types.Int64:
		if intVal, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			return intVal
		}
	case types.Float64:
		if floatVal, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			return floatVal
		}
	case types.Bool:
		lowerVal := strings.ToLower(strings.TrimSpace(value))
		if lowerVal == "true" || lowerVal == "1" {
			return true
		}
		if lowerVal == "false" || lowerVal == "0" {
			return false
		}
	}

	// Default to string
	return value
}

// ============================================
// SECTION 5: Incremental-Specific Methods
// ============================================
// This section contains methods specific to incremental sync operations

// FetchMaxCursorValues returns the maximum LastModified timestamp for all files in the stream
// This is used by the abstract layer to track incremental sync progress
func (s *S3) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	streamName := stream.Name()
	
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		logger.Debugf("No files found for stream %s, returning nil cursor", streamName)
		return nil, nil, nil
	}

	// Find the latest LastModified timestamp among all files in this stream
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	logger.Infof("Max cursor value for stream %s: %s (from %d files)", streamName, maxLastModified, len(files))
	
	// Return as primary cursor (secondary cursor not used for S3)
	return maxLastModified, nil, nil
}

// StreamIncrementalChanges is called by the abstract layer after backfill completes
// For S3, all incremental work is already done by GetOrSplitChunks + Backfill via cursor filtering:
//   1. GetOrSplitChunks filters files by cursor (empty for full-load, timestamp for incremental)
//   2. Backfill processes chunks in parallel via connection groups
//   3. This method becomes a no-op since all work is already complete
func (s *S3) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	streamName := stream.Name()
	
	logger.Infof("Incremental sync already completed for stream %s via unified chunk processing", streamName)
	
	// All files have already been processed by the Backfill phase which:
	// - Called GetOrSplitChunks (with cursor-based filtering)
	// - Grouped files into optimized 2GB chunks
	// - Processed chunks in parallel via abstract layer connection groups
	//
	// No additional work needed here - cursor filtering ensures only new/modified files
	// were included in the chunks that were already processed.
	
	return nil
}

