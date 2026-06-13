package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

const (
	jsonSchemaMaxSamples           = 100
	jsonSchemaMaxBytesForInference = 10 * 1024 * 1024
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
	return p.InferSchemaFromReaders(ctx, reader)
}

// InferSchemaFromReaders infers a JSON schema from one or more independent JSON readers.
// Each reader may be JSONL, a JSON array, or a single JSON object.
func (p *JSONParser) InferSchemaFromReaders(ctx context.Context, readers ...io.Reader) (*types.Stream, error) {
	logger.Debug("Inferring JSON schema from sample data")

	if len(readers) == 0 {
		return nil, fmt.Errorf("no JSON readers provided")
	}

	allRecords := []map[string]interface{}{}
	for i, reader := range readers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Limit data read for schema inference to prevent OOM on large files.
		// 10MB should be enough to get 100 sample records for most JSON files.
		limitedReader := io.LimitReader(reader, jsonSchemaMaxBytesForInference)

		data, err := io.ReadAll(limitedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to read JSON sample %d: %s", i+1, err)
		}

		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 {
			return nil, fmt.Errorf("empty JSON sample %d", i+1)
		}

		// Parse JSON based on detected format.
		sampleRecords, err := p.parseJSONContent(trimmed, jsonSchemaMaxSamples)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON sample %d: %s", i+1, err)
		}

		if len(sampleRecords) == 0 {
			return nil, fmt.Errorf("no records found in JSON sample %d", i+1)
		}

		allRecords = append(allRecords, sampleRecords...)
	}

	if len(allRecords) == 0 {
		return nil, fmt.Errorf("no records found in JSON file")
	}

	recordsForResolve := allRecords
	if len(allRecords) > 1 {
		// Resolve marks fields nullable when a field seen earlier is missing later.
		// Replaying the first record makes that comparison circular, so fields first
		// seen in a later sample are still checked against earlier records.
		recordsForResolve = append(recordsForResolve, allRecords[0])
	}

	if err := typeutils.Resolve(p.stream, recordsForResolve...); err != nil {
		return nil, fmt.Errorf("failed to resolve JSON schema: %s", err)
	}

	logger.Infof("Inferred schema from JSON file")
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
				return fmt.Errorf("failed to process record: %s", err)
			}
			recordCount++
		}
	} else {
		// JSON array - stream elements one by one to avoid OOM
		decoder := json.NewDecoder(reader)

		// Read opening bracket
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read JSON array start: %s", err)
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
				return fmt.Errorf("failed to process record: %s", err)
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
		return nil, fmt.Errorf("failed to read JSON array start: %s", err)
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
			return nil, fmt.Errorf("failed to decode JSON array element: %s", err)
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
			return nil, fmt.Errorf("failed to parse as JSONL: %s", err)
		}
		return nil, fmt.Errorf("failed to parse as single JSON object or JSONL: %s", err)
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
