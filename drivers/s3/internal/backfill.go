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
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	pq "github.com/parquet-go/parquet-go"
)

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

	// Calculate total file size across all files in the stream
	var totalSize int64
	totalFiles := len(files)
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
	pool.AddRecordsToSyncStats(int64(totalFiles))

	// Group files into chunks based on size to optimize state storage and parallel processing
	chunks := s.groupFilesIntoChunks(files)

	logger.Infof("Creating %d chunks for stream %s (total files: %d, total size: %s)",
		chunks.Len(), streamName, totalFiles, sizeDisplay)

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

			// Add large file as separate chunk
			chunks.Insert(types.Chunk{
				Min: file.FileKey,  // Single file key (string)
				Max: file.Size,     // File size
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

// ChunkIterator reads and processes records from an S3 file or multiple files
// Handles both single file chunks (Min = string) and multi-file chunks (Min = []string)
func (s *S3) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	// Check if this is a multi-file chunk or single file chunk
	switch v := chunk.Min.(type) {
	case []string:
		// Multi-file chunk - process each file sequentially within the chunk
		logger.Infof("Processing chunk with %d files", len(v))
		for i, key := range v {
			logger.Debugf("Processing file %d/%d in chunk: %s", i+1, len(v), key)
			fileSize := s.getFileSize(stream.Name(), key)
			if err := s.processFile(ctx, stream, key, fileSize, processFn); err != nil {
				return fmt.Errorf("failed to process file %s in chunk: %w", key, err)
			}
		}
		return nil

	case []interface{}:
		// Handle []interface{} from state deserialization
		logger.Infof("Processing chunk with %d files (interface array)", len(v))
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

	case string:
		// Single file chunk - process directly
		key := v
		var fileSize int64
		if chunk.Max != nil {
			if size, ok := chunk.Max.(int64); ok {
				fileSize = size
			}
		}
		logger.Infof("Processing single file: %s (size: %d bytes)", key, fileSize)
		return s.processFile(ctx, stream, key, fileSize, processFn)

	default:
		return fmt.Errorf("invalid chunk Min type: expected string or []string, got %T", chunk.Min)
	}
}

// processFile handles the processing of a single S3 file
func (s *S3) processFile(ctx context.Context, stream types.StreamInterface, key string, fileSize int64, processFn abstract.BackfillMsgFn) error {

	// For Parquet with streaming enabled, use range requests
	if s.config.FileFormat == FormatParquet && s.config.ParquetStreamingEnabled && fileSize > 0 {
		return s.processParquetFileStreaming(ctx, key, fileSize, processFn)
	}

	// For other formats or fallback, use standard streaming from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if file was deleted between discovery and processing
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
			logger.Warnf("File %s was deleted or not found, skipping", key)
			return nil // Don't fail the entire sync for a missing file
		}
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Wrap with decompression reader if needed
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	// Process based on file format
	switch s.config.FileFormat {
	case FormatCSV:
		return s.processCSVFile(ctx, reader, stream, processFn)
	case FormatJSON:
		return s.processJSONFile(ctx, reader, processFn)
	case FormatParquet:
		// Fallback to in-memory processing if streaming disabled
		return s.processParquetFile(ctx, reader, processFn)
	default:
		return fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}
}

// processCSVFile reads and processes a CSV file
func (s *S3) processCSVFile(ctx context.Context, reader io.Reader, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(s.config.Delimiter[0])
	if s.config.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < s.config.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	if s.config.HasHeader {
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
	recordCount := 0

	if s.config.JSONLineDelimited {
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
