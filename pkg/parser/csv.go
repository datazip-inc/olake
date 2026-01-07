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
			return nil, fmt.Errorf("failed to skip row %d: %s", i, err)
		}
	}

	var headers []string
	var err error
	if p.config.HasHeader {
		// Read header row
		headers, err = csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV headers: %s", err)
		}
	} else {
		// Read first data row to determine column count
		firstRow, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read first CSV row: %s", err)
		}
		// Generate column names as column_0, column_1, etc.
		for i := range firstRow {
			headers = append(headers, fmt.Sprintf("column_%d", i))
		}
	}

	// Read a few sample rows to infer data types (max 100 samples)
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
		dataType := inferColumnType(sampleRows, i)
		p.stream.UpsertField(header, dataType, true) // Allow nulls by default
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
			return fmt.Errorf("failed to skip row %d: %s", i, err)
		}
	}

	var headers []string
	if p.config.HasHeader {
		// Read header row
		var err error
		headers, err = csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV headers: %s", err)
		}
	} else {
		// Generate default column names based on first row
		firstRow, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read first row: %s", err)
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
					return fmt.Errorf("failed to get type for field %s: %s", headers[i], err)
				}
				convertedValue, err := convertValue(value, fieldType)
				if err != nil {
					if errors.Is(err, errNullValue) {
						// errNullValue indicates a valid null/empty value
						record[headers[i]] = nil
					} else {
						return fmt.Errorf("failed to convert value for field %s: %s", headers[i], err)
					}
				} else {
					record[headers[i]] = convertedValue
				}
			}
		}
		if err := callback(ctx, record); err != nil {
			return fmt.Errorf("failed to process first record: %s", err)
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
					return fmt.Errorf("failed to get type for field %s: %s", headers[i], err)
				}
				convertedValue, err := convertValue(value, fieldType)
				if err != nil {
					if errors.Is(err, errNullValue) {
						// errNullValue indicates a valid null/empty value
						record[headers[i]] = nil
					} else {
						return fmt.Errorf("failed to convert value for field %s in row %d: %s", headers[i], recordCount, err)
					}
				} else {
					record[headers[i]] = convertedValue
				}
			}
		}

		if err := callback(ctx, record); err != nil {
			return fmt.Errorf("failed to process record: %s", err)
		}
		recordCount++
	}

	logger.Infof("Processed %d records from CSV file", recordCount)
	return nil
}

// inferColumnType infers the data type of a CSV column from sample values
func inferColumnType(sampleRows [][]string, columnIndex int) types.DataType {
	if len(sampleRows) == 0 {
		return types.String
	}

	allInt := true
	allFloat := true
	allBool := true
	allTimestamp := true
	nonNullCount := 0

	for _, row := range sampleRows {
		if columnIndex >= len(row) {
			continue
		}

		value := strings.TrimSpace(row[columnIndex])
		if value == "" || strings.ToLower(value) == "null" {
			continue
		}

		nonNullCount++

		// Check if this value can be parsed as each type
		// If any value fails to parse, that type is ruled out
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			allInt = false
		}

		if _, err := strconv.ParseFloat(value, 64); err != nil {
			allFloat = false
		}

		lowerValue := strings.ToLower(value)
		if lowerValue != "true" && lowerValue != "false" {
			allBool = false
		}

		// Check if value can be parsed as timestamp
		if _, err := typeutils.ReformatDate(value, true); err != nil {
			allTimestamp = false
		}
	}

	// If no non-null values found, default to string
	if nonNullCount == 0 {
		return types.String
	}

	// Determine type based on what ALL values can be parsed as
	// Priority: Bool > Float > Timestamp > String
	if allBool {
		return types.Bool
	}
	if allFloat || allInt {
		return types.Float64
	}
	if allTimestamp {
		// Try to detect timestamp precision from first non-null value
		for _, row := range sampleRows {
			if columnIndex >= len(row) {
				continue
			}
			value := strings.TrimSpace(row[columnIndex])
			if value == "" || strings.ToLower(value) == "null" {
				continue
			}
			if t, err := typeutils.ReformatDate(value, true); err == nil {
				return typeutils.TypeFromValue(t)
			}
		}
		// Default to TimestampMicro if we can't detect precision
		return types.TimestampMicro
	}

	// Default to string
	return types.String
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
			return nil, fmt.Errorf("failed to convert '%s' to integer: %s", trimmed, err)
		}
		return int32(intVal), nil
	case types.Int64:
		intVal, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to int64: %s", trimmed, err)
		}
		return intVal, nil
	case types.Float32:
		floatVal, err := strconv.ParseFloat(trimmed, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float32: %s", trimmed, err)
		}
		// Handle NaN/Inf - JSON doesn't support these values, so convert to nil
		if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
			return nil, errNullValue
		}
		return float32(floatVal), nil
	case types.Float64:
		floatVal, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float64: %s", trimmed, err)
		}
		// Handle NaN/Inf - JSON doesn't support these values, so convert to nil
		if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
			return nil, errNullValue
		}
		return floatVal, nil
	case types.Bool:
		boolVal, err := strconv.ParseBool(trimmed)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to boolean: %s", trimmed, err)
		}
		return boolVal, nil
	case types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano:
		// Parse timestamp string using ReformatDate which handles multiple formats
		timestampVal, err := typeutils.ReformatDate(trimmed, true)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to timestamp: %s", trimmed, err)
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
