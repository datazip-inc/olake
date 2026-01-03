package parser

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// errNullValue is a sentinel error indicating a null/empty value was encountered.
// This is used instead of returning (nil, nil) to satisfy the nilnil linter.
var errNullValue = errors.New("null value")

// CSVParser implements the Parser interface for CSV files
type CSVParser struct {
	config CSVConfig
	stream *types.Stream
}

// NewCSVParser creates a new CSV parser with the given configuration
func NewCSVParser(config CSVConfig, stream *types.Stream) *CSVParser {
	return &CSVParser{
		config: config,
		stream: stream,
	}
}

// InferSchema reads the first few rows of a CSV file to infer the schema
// Uses small samples to avoid loading entire file into memory
func (p *CSVParser) InferSchema(_ context.Context, reader io.Reader) (*types.Stream, error) {
	logger.Debug("Inferring CSV schema from sample data")

	// Create CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(p.config.Delimiter[0])
	if p.config.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < p.config.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	var err error
	if p.config.HasHeader {
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

	// Read a few sample rows to infer data types (max 100 samples)
	// Process one record at a time similar to MongoDB driver
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

		// Convert row to record map for resolver
		record := make(map[string]interface{})
		for j, value := range row {
			if j < len(headers) {
				// Try to convert value to appropriate Go type for resolver inference
				convertedValue := convertValueForInference(value)
				record[headers[j]] = convertedValue
			}
		}

		// Resolve schema one record at a time
		if err := typeutils.Resolve(p.stream, record); err != nil {
			return nil, fmt.Errorf("failed to resolve schema for record %d: %w", i, err)
		}
	}
	logger.Infof("Inferred schema with %d columns from CSV", len(headers))
	return p.stream, nil
}

// StreamRecords reads and streams CSV records with context support
func (p *CSVParser) StreamRecords(ctx context.Context, reader io.Reader, callback RecordCallback) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(p.config.Delimiter[0])
	if p.config.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < p.config.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	if p.config.HasHeader {
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
				fieldType, err := p.stream.Schema.GetType(headers[i])
				if err != nil {
					return fmt.Errorf("failed to get type for field %s: %w", headers[i], err)
				}
				convertedValue, err := convertValue(value, fieldType)
				if err != nil {
					if errors.Is(err, errNullValue) {
						// errNullValue indicates a valid null/empty value
						record[headers[i]] = nil
					} else {
						return fmt.Errorf("failed to convert value for field %s: %w", headers[i], err)
					}
				} else {
					record[headers[i]] = convertedValue
				}
			}
		}
		if err := callback(ctx, record); err != nil {
			return fmt.Errorf("failed to process first record: %w", err)
		}
	}

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
				fieldType, err := p.stream.Schema.GetType(headers[i])
				if err != nil {
					return fmt.Errorf("failed to get type for field %s: %w", headers[i], err)
				}
				convertedValue, err := convertValue(value, fieldType)
				if err != nil {
					if errors.Is(err, errNullValue) {
						// errNullValue indicates a valid null/empty value
						record[headers[i]] = nil
					} else {
						return fmt.Errorf("failed to convert value for field %s in row %d: %w", headers[i], recordCount, err)
					}
				} else {
					record[headers[i]] = convertedValue
				}
			}
		}

		if err := callback(ctx, record); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
		recordCount++
	}

	logger.Infof("Processed %d records from CSV file", recordCount)
	return nil
}

// convertValueForInference converts a CSV string value to appropriate Go type for schema inference
func convertValueForInference(value string) interface{} {
	trimmed := strings.TrimSpace(value)

	// Handle null/empty values
	if trimmed == "" || strings.ToLower(trimmed) == "null" {
		return nil
	}

	// Try boolean first (highest priority in original logic)
	if lowerValue := strings.ToLower(trimmed); lowerValue == "true" || lowerValue == "false" {
		if boolVal, err := strconv.ParseBool(trimmed); err == nil {
			return boolVal
		}
	}

	// Try integer
	if intVal, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return intVal
	}

	// Try float
	if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
		// Handle NaN - JSON doesn't support NaN, so convert to nil
		if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
			return nil
		}
		return floatVal
	}

	// Try timestamp
	if timestampVal, err := typeutils.ReformatDate(trimmed); err == nil {
		return timestampVal
	}

	// Default to string
	return trimmed
}

// convertValue converts a string value to the appropriate type based on schema
func convertValue(value string, fieldType types.DataType) (interface{}, error) {
	trimmed := strings.TrimSpace(value)

	// Handle null/empty values
	if trimmed == "" || strings.ToLower(trimmed) == "null" {
		return nil, errNullValue
	}

	// Convert based on field type
	switch fieldType {
	case types.Int32:
		intVal, err := strconv.ParseInt(trimmed, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to integer: %w", trimmed, err)
		}
		return int32(intVal), nil
	case types.Int64:
		intVal, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to int64: %w", trimmed, err)
		}
		return intVal, nil
	case types.Float32:
		floatVal, err := strconv.ParseFloat(trimmed, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float32: %w", trimmed, err)
		}
		// Handle NaN/Inf - JSON doesn't support these values, so convert to nil
		if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
			return nil, errNullValue
		}
		return float32(floatVal), nil
	case types.Float64:
		floatVal, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float64: %w", trimmed, err)
		}
		// Handle NaN/Inf - JSON doesn't support these values, so convert to nil
		if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
			return nil, errNullValue
		}
		return floatVal, nil
	case types.Bool:
		boolVal, err := strconv.ParseBool(trimmed)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to boolean: %w", trimmed, err)
		}
		return boolVal, nil
	case types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano:
		// Parse timestamp string using ReformatDate which handles multiple formats
		timestampVal, err := typeutils.ReformatDate(trimmed)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to timestamp: %w", trimmed, err)
		}
		return timestampVal, nil
	case types.Object:
		// Try to parse as JSON object
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
			// If parsing fails, return as string to avoid data loss
			logger.Warnf("Failed to parse '%s' as JSON object, treating as string: %v", trimmed, err)
			return trimmed, nil
		}
		return obj, nil
	case types.Array:
		// Try to parse as JSON array
		var arr []interface{}
		if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
			// If parsing fails, return as string to avoid data loss
			logger.Warnf("Failed to parse '%s' as JSON array, treating as string: %v", trimmed, err)
			return trimmed, nil
		}
		return arr, nil
	case types.Unknown:
		// Unknown type defaults to string
		return trimmed, nil
	}

	// Default to string (including types.String, types.Null)
	return trimmed, nil
}
