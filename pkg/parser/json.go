package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// JSONParser implements the Parser interface for JSON files
type JSONParser struct {
	config JSONConfig
	stream *types.Stream
}

// NewJSONParser creates a new JSON parser with the given configuration
func NewJSONParser(config JSONConfig, stream *types.Stream) *JSONParser {
	return &JSONParser{
		config: config,
		stream: stream,
	}
}

// InferSchema reads the first few records of a JSON file to infer the schema
// Supports JSONL (line-delimited), JSON Array, and single JSON object formats
func (p *JSONParser) InferSchema(ctx context.Context, reader io.Reader) (*types.Stream, error) {
	logger.Debug("Inferring JSON schema from sample data")

	// Collect sample records using smart JSON format detection
	maxSamples := 100

	// Limit data read for schema inference to prevent OOM on large files
	// 10MB should be enough to get 100 sample records for most JSON files
	const maxBytesForInference = 10 * 1024 * 1024 // 10MB
	limitedReader := io.LimitReader(reader, maxBytesForInference)

	data, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file: %w", err)
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty JSON file")
	}

	// Parse JSON based on detected format
	sampleRecords, err := p.parseJSONContent(trimmed, maxSamples)
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
		dataType := inferJSONFieldType(values)
		p.stream.UpsertField(fieldName, dataType, true) // Allow nulls by default
	}

	logger.Infof("Inferred schema with %d fields from JSON", len(fieldTypes))
	return p.stream, nil
}

// StreamRecords reads and streams JSON records with context support
func (p *JSONParser) StreamRecords(ctx context.Context, reader io.Reader, callback RecordCallback) error {
	recordCount := 0

	if p.config.LineDelimited {
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

			if err := callback(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}
	} else {
		// JSON array - stream elements one by one to avoid OOM
		decoder := json.NewDecoder(reader)

		// Read opening bracket
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read JSON array start: %w", err)
		}

		// Verify it's an array
		if delim, ok := token.(json.Delim); !ok || delim != '[' {
			return fmt.Errorf("expected JSON array, got: %v", token)
		}

		// Stream elements one by one
		for decoder.More() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var record map[string]interface{}
			if err := decoder.Decode(&record); err != nil {
				logger.Warnf("Error reading JSON record %d: %v", recordCount, err)
				continue
			}

			if err := callback(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}
	}

	logger.Infof("Processed %d records from JSON file", recordCount)
	return nil
}

// parseJSONContent intelligently parses JSON content based on its structure
// Supports: JSON Array, JSONL (line-delimited), single JSON object
func (p *JSONParser) parseJSONContent(data []byte, maxSamples int) ([]map[string]interface{}, error) {
	firstChar := data[0]

	switch firstChar {
	case '[':
		// JSON Array format: [{"key":"value"}, {"key":"value"}]
		return p.parseJSONArray(data, maxSamples)

	case '{':
		// Could be either:
		// 1. JSONL (line-delimited): {"key":"value"}\n{"key":"value"}\n
		// 2. Single JSON object: {"key":"value"}
		return p.parseJSONLOrObject(data, maxSamples)

	default:
		return nil, fmt.Errorf("invalid JSON format: expected '[' or '{', got '%c'", firstChar)
	}
}

// parseJSONArray handles JSON array format: [{"key":"value"}, ...]
// Uses streaming decoder to avoid loading entire array into memory
func (p *JSONParser) parseJSONArray(data []byte, maxSamples int) ([]map[string]interface{}, error) {
	logger.Debug("Parsing JSON array format")

	decoder := json.NewDecoder(bytes.NewReader(data))

	// Read opening bracket
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON array start: %w", err)
	}

	// Verify it's an array
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got: %v", token)
	}

	// Stream elements one by one (up to maxSamples)
	records := make([]map[string]interface{}, 0, maxSamples)
	for decoder.More() && len(records) < maxSamples {
		var record map[string]interface{}
		if err := decoder.Decode(&record); err != nil {
			// If we have some records, that's enough for schema inference
			if len(records) > 0 {
				logger.Warnf("Stopped reading JSON array after %d records due to error: %v", len(records), err)
				break
			}
			return nil, fmt.Errorf("failed to decode JSON array element: %w", err)
		}
		records = append(records, record)
	}

	logger.Infof("Parsed %d records from JSON array for schema inference", len(records))
	return records, nil
}

// parseJSONLOrObject handles:
// 1. JSONL format: {"key":"value"}\n{"key":"value"}\n...
// 2. Single object: {"key":"value"}
func (p *JSONParser) parseJSONLOrObject(data []byte, maxSamples int) ([]map[string]interface{}, error) {
	// Try parsing as JSONL (line-delimited) first
	records, isJSONL, err := p.tryParseJSONL(data, maxSamples)
	if err == nil && isJSONL {
		logger.Debug("Parsed JSONL (line-delimited) format")
		logger.Infof("Parsed %d records from JSONL", len(records))
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

	logger.Debug("Parsed single JSON object")
	return []map[string]interface{}{singleRecord}, nil
}

// tryParseJSONL attempts to parse data as line-delimited JSON
// Returns (records, isJSONL, error)
func (p *JSONParser) tryParseJSONL(data []byte, maxSamples int) ([]map[string]interface{}, bool, error) {
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

	// If we got multiple records, it's JSONL
	return records, len(records) > 1, nil
}

// inferJSONFieldType infers the data type of a JSON field from sample values
// FIXED: Always use Float64 for numeric values to avoid Int64/Float64 conflicts
// since JSON doesn't distinguish between integers and floats
func inferJSONFieldType(values []interface{}) types.DataType {
	if len(values) == 0 {
		return types.String
	}

	hasNumber := false
	allBool := true
	allObject := true
	allArray := true
	nonNullCount := 0

	for _, value := range values {
		if value == nil {
			continue
		}

		nonNullCount++

		switch value.(type) {
		case bool:
			hasNumber = false
			allObject = false
			allArray = false
		case float64:
			// JSON numbers are always decoded as float64
			// Always treat as Float64 to avoid schema conflicts
			hasNumber = true
			allBool = false
			allObject = false
			allArray = false
		case string:
			hasNumber = false
			allBool = false
			allObject = false
			allArray = false
		case map[string]interface{}:
			hasNumber = false
			allBool = false
			allArray = false
		case []interface{}:
			hasNumber = false
			allBool = false
			allObject = false
		default:
			// Unknown type, mark all as false
			hasNumber = false
			allBool = false
			allObject = false
			allArray = false
		}
	}

	// If no non-null values found, default to string
	if nonNullCount == 0 {
		return types.String
	}

	// Determine type based on what values were seen
	// Priority: Bool > Float64 (for any number) > Object/Array > String
	if allBool {
		return types.Bool
	}
	if hasNumber {
		// Always use Float64 for JSON numbers to avoid Int64/Float64 conflicts
		// This is safer because JSON doesn't distinguish between int and float
		return types.Float64
	}
	if allObject || allArray {
		return types.String // Complex types should be stored as JSON string
	}

	// Default to string
	return types.String
}
