package driver

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	pq "github.com/parquet-go/parquet-go"
)

// inferCSVSchema reads the first few rows of a CSV file to infer the schema
func (s *S3) inferCSVSchema(ctx context.Context, stream *types.Stream, key string) (*types.Stream, error) {
	logger.Infof("Inferring CSV schema for file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Wrap with decompression reader if needed
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	// Create CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(s.config.Delimiter[0])
	if s.config.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < s.config.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	if s.config.HasHeader {
		// Read header row
		headers, err = csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV headers: %w", err)
		}
	} else {
		// Read first data row to determine column count
		firstRow, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read first CSV row: %w", err)
		}
		// Generate column names as column_0, column_1, etc.
		for i := range firstRow {
			headers = append(headers, fmt.Sprintf("column_%d", i))
		}
	}

	// Read a few sample rows to infer data types
	sampleRows := [][]string{}
	maxSamples := 100
	for i := 0; i < maxSamples; i++ {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Warnf("Error reading sample row %d: %v", i, err)
			break
		}
		sampleRows = append(sampleRows, row)
	}

	// Infer types for each column
	for i, header := range headers {
		dataType := s.inferColumnType(sampleRows, i)
		stream.UpsertField(header, dataType, true) // Allow nulls by default
		stream.WithCursorField(header)
	}

	logger.Infof("Inferred schema with %d columns from CSV", len(headers))
	return stream, nil
}

// inferJSONSchema reads the first few records of a JSON file to infer the schema
func (s *S3) inferJSONSchema(ctx context.Context, stream *types.Stream, key string) (*types.Stream, error) {
	logger.Infof("Inferring JSON schema for file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Wrap with decompression reader if needed
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	// Collect sample records using smart JSON format detection
	var sampleRecords []map[string]interface{}
	maxSamples := 100

	// Read all content for format detection
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file: %w", err)
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty JSON file")
	}

	// Parse JSON based on detected format
	sampleRecords, err = s.parseJSONContent(trimmed, key, maxSamples)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(sampleRecords) == 0 {
		return nil, fmt.Errorf("no records found in JSON file")
	}

	// Collect all unique field names across samples
	fieldTypes := make(map[string][]interface{})
	for _, record := range sampleRecords {
		for fieldName, value := range record {
			fieldTypes[fieldName] = append(fieldTypes[fieldName], value)
		}
	}

	// Infer type for each field
	for fieldName, values := range fieldTypes {
		dataType := s.inferJSONFieldType(values)
		stream.UpsertField(fieldName, dataType, true) // Allow nulls by default
		stream.WithCursorField(fieldName)
	}

	logger.Infof("Inferred schema with %d fields from JSON", len(fieldTypes))
	return stream, nil
}

// inferParquetSchema reads Parquet file schema
func (s *S3) inferParquetSchema(ctx context.Context, stream *types.Stream, key string) (*types.Stream, error) {
	logger.Infof("Inferring Parquet schema for file: %s", key)

	// Get file size from discovered files
	fileSize := s.getFileSize(stream.Name, key)

	var pqFile *pq.File
	var err error

	// Use range reader if streaming is enabled and we have file size
	if s.config.ParquetStreamingEnabled && fileSize > 0 {
		logger.Debugf("Using S3 range requests for schema inference")
		rangeReader := NewS3RangeReader(ctx, s.client, s.config.BucketName, key, fileSize)
		pqFile, err = pq.OpenFile(rangeReader, fileSize)
		if err != nil {
			return nil, fmt.Errorf("failed to open parquet file with range reader: %w", err)
		}
	} else {
		// Fallback: read entire file into memory
		logger.Debugf("Using full file read for schema inference (streaming disabled)")
		result, getErr := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.config.BucketName),
			Key:    aws.String(key),
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get object from S3: %w", getErr)
		}
		defer result.Body.Close()

		data, readErr := io.ReadAll(result.Body)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read parquet file: %w", readErr)
		}

		pqFile, err = pq.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("failed to open parquet file: %w", err)
		}
	}

	// Get the schema from parquet file
	schema := pqFile.Schema()

	// Convert parquet schema to Olake schema with proper type mapping
	for _, field := range schema.Fields() {
		olakeType := s.mapParquetTypeToOlake(field.Type())
		nullable := field.Optional()
		stream.UpsertField(field.Name(), olakeType, nullable)
	}

	logger.Infof("Inferred %d columns from Parquet file with proper types", len(schema.Fields()))

	return stream, nil
}

// mapParquetTypeToOlake maps Parquet physical types and logical type annotations to Olake data types
func (s *S3) mapParquetTypeToOlake(pqType pq.Type) types.DataType {
	// First, check for logical type annotations which provide semantic meaning
	if logicalType := pqType.LogicalType(); logicalType != nil {
		// Integer logical types (INT_8, INT_16, INT_32, INT_64)
		if logicalType.Integer != nil {
			switch logicalType.Integer.BitWidth {
			case 8, 16, 32:
				return types.Int32
			case 64:
				return types.Int64
			default:
				logger.Warnf("Unexpected integer bit width %d, defaulting to Int32", logicalType.Integer.BitWidth)
				return types.Int32
			}
		}

		// Timestamp with precision
		if logicalType.Timestamp != nil {
			if logicalType.Timestamp.Unit.Nanos != nil {
				return types.TimestampNano
			} else if logicalType.Timestamp.Unit.Micros != nil {
				return types.TimestampMicro
			} else if logicalType.Timestamp.Unit.Millis != nil {
				return types.TimestampMilli
			}
			return types.Timestamp
		}

		// Time with precision
		if logicalType.Time != nil {
			if logicalType.Time.Unit.Nanos != nil {
				return types.TimestampNano
			} else if logicalType.Time.Unit.Micros != nil {
				return types.TimestampMicro
			} else if logicalType.Time.Unit.Millis != nil {
				return types.TimestampMilli
			}
			return types.String
		}

		// Date
		if logicalType.Date != nil {
			return types.Timestamp
		}

		// Decimal (stored as INT32/INT64/BYTE_ARRAY)
		if logicalType.Decimal != nil {
			return types.Float64
		}

		// String-based types: UTF8, JSON, UUID, Enum, BSON
		if logicalType.UTF8 != nil || logicalType.Json != nil || logicalType.UUID != nil ||
			logicalType.Enum != nil || logicalType.Bson != nil {
			return types.String
		}

		// List (arrays)
		if logicalType.List != nil {
			return types.Array
		}

		// Map (objects)
		if logicalType.Map != nil {
			return types.Object
		}
	}

	// Physical type mapping (no logical type annotation)
	switch pqType.Kind() {
	case pq.Boolean:
		return types.Bool
	case pq.Int32:
		return types.Int32
	case pq.Int64:
		return types.Int64
	case pq.Int96:
		// Int96 is typically used for timestamps in legacy Parquet files
		return types.Timestamp
	case pq.Float:
		return types.Float32
	case pq.Double:
		return types.Float64
	case pq.ByteArray, pq.FixedLenByteArray:
		// Byte arrays without logical type annotation default to string
		return types.String
	default:
		// Unknown types default to string for safety
		logger.Warnf("Unknown Parquet type %v, defaulting to string", pqType.Kind())
		return types.String
	}
}

// inferColumnType infers the data type of a CSV column from sample values
func (s *S3) inferColumnType(sampleRows [][]string, columnIndex int) types.DataType {
	if len(sampleRows) == 0 {
		return types.String
	}

	hasInt := false
	hasFloat := false
	hasBool := false

	for _, row := range sampleRows {
		if columnIndex >= len(row) {
			continue
		}

		value := strings.TrimSpace(row[columnIndex])
		if value == "" || strings.ToLower(value) == "null" {
			continue
		}

		// Try to parse as different types
		if _, err := strconv.ParseInt(value, 10, 64); err == nil {
			hasInt = true
		} else if _, err := strconv.ParseFloat(value, 64); err == nil {
			hasFloat = true
		} else if strings.ToLower(value) == "true" || strings.ToLower(value) == "false" {
			hasBool = true
		}
	}

	// Determine type based on what we found
	if hasBool && !hasInt && !hasFloat {
		return types.Bool
	}
	if hasFloat {
		return types.Float64
	}
	if hasInt {
		return types.Int64
	}

	// Default to string
	return types.String
}

// inferJSONFieldType infers the data type of a JSON field from sample values
func (s *S3) inferJSONFieldType(values []interface{}) types.DataType {
	if len(values) == 0 {
		return types.String
	}

	hasInt := false
	hasFloat := false
	hasBool := false
	hasString := false
	hasObject := false
	hasArray := false

	for _, value := range values {
		if value == nil {
			continue
		}

		switch v := value.(type) {
		case bool:
			hasBool = true
		case float64:
			// JSON numbers are always float64
			if v == float64(int64(v)) {
				hasInt = true
			} else {
				hasFloat = true
			}
		case string:
			hasString = true
		case map[string]interface{}:
			hasObject = true
		case []interface{}:
			hasArray = true
		}
	}

	// Determine type based on what we found
	if hasBool && !hasInt && !hasFloat && !hasString && !hasObject && !hasArray {
		return types.Bool
	}
	if hasFloat {
		return types.Float64
	}
	if hasInt && !hasFloat {
		return types.Int64
	}
	if hasObject || hasArray {
		return types.String // Complex types should be stored as JSON string
	}

	// Default to string
	return types.String
}

// getReader returns an appropriate reader based on file extension
// Auto-detects compression from file extension rather than global config
func (s *S3) getReader(body io.Reader, key string) (io.Reader, error) {
	lowerKey := strings.ToLower(key)

	// Check if file has gzip extension - this takes precedence over config
	if strings.HasSuffix(lowerKey, ".gz") {
		gzipReader, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		logger.Debugf("Using gzip decompression for file: %s", key)
		return gzipReader, nil
	}

	// TODO: Add support for .zip files if needed
	// if strings.HasSuffix(lowerKey, ".zip") { ... }

	// No compression detected, return body as-is
	return body, nil
}

// parseJSONContent intelligently parses JSON content based on its structure
// Supports: JSON Array, JSONL (line-delimited), single JSON object, and nested structures
func (s *S3) parseJSONContent(data []byte, key string, maxSamples int) ([]map[string]interface{}, error) {
	firstChar := data[0]

	switch firstChar {
	case '[':
		// JSON Array format: [{"key":"value"}, {"key":"value"}]
		return s.parseJSONArray(data, key, maxSamples)

	case '{':
		// Could be either:
		// 1. JSONL (line-delimited): {"key":"value"}\n{"key":"value"}\n
		// 2. Single JSON object: {"key":"value"}
		return s.parseJSONLOrObject(data, key, maxSamples)

	default:
		return nil, fmt.Errorf("invalid JSON format: expected '[' or '{', got '%c'", firstChar)
	}
}

// parseJSONArray handles JSON array format: [{"key":"value"}, ...]
func (s *S3) parseJSONArray(data []byte, key string, maxSamples int) ([]map[string]interface{}, error) {
	logger.Debugf("Parsing JSON array format for file: %s", key)

	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("failed to decode JSON array: %w", err)
	}

	logger.Infof("Parsed JSON array with %d records from file: %s", len(records), key)

	if len(records) > maxSamples {
		return records[:maxSamples], nil
	}
	return records, nil
}

// parseJSONLOrObject handles:
// 1. JSONL format: {"key":"value"}\n{"key":"value"}\n...
// 2. Single object: {"key":"value"}
// 3. Newline-delimited with extra whitespace
func (s *S3) parseJSONLOrObject(data []byte, key string, maxSamples int) ([]map[string]interface{}, error) {
	// Try parsing as JSONL (line-delimited) first
	records, isJSONL, err := s.tryParseJSONL(data, maxSamples)
	if err == nil && isJSONL {
		logger.Debugf("Parsed JSONL (line-delimited) format for file: %s", key)
		logger.Infof("Parsed %d records from JSONL file: %s", len(records), key)
		return records, nil
	}

	// If not JSONL, try parsing as a single JSON object
	var singleRecord map[string]interface{}
	if err := json.Unmarshal(data, &singleRecord); err != nil {
		// If both failed, return a more helpful error
		if isJSONL {
			return nil, fmt.Errorf("failed to parse as JSONL: %w", err)
		}
		return nil, fmt.Errorf("failed to parse as single JSON object or JSONL: %w", err)
	}

	logger.Debugf("Parsed single JSON object for file: %s", key)
	return []map[string]interface{}{singleRecord}, nil
}

// tryParseJSONL attempts to parse data as line-delimited JSON
// Returns (records, isJSONL, error)
// isJSONL indicates if the data appears to be JSONL format (multiple lines with objects)
func (s *S3) tryParseJSONL(data []byte, maxSamples int) ([]map[string]interface{}, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	records := []map[string]interface{}{}

	for i := 0; i < maxSamples; i++ {
		var record map[string]interface{}
		err := decoder.Decode(&record)
		
		if err == io.EOF {
			break
		}
		
		if err != nil {
			// If we got at least one record, it might be JSONL with invalid trailing data
			if len(records) > 0 {
				logger.Warnf("JSONL parsing stopped at record %d due to error: %v", i, err)
				return records, true, nil
			}
			// First record failed, not JSONL
			return nil, false, err
		}

		records = append(records, record)
	}

	// If we parsed multiple records or there's more data after first decode, it's JSONL
	isJSONL := len(records) > 1 || (len(records) == 1 && decoder.More())
	
	return records, isJSONL, nil
}
