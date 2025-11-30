package parser

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

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
func (p *CSVParser) InferSchema(ctx context.Context, reader io.Reader) (*types.Stream, error) {
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

// StreamRecords reads and streams CSV records with batching and context support
func (p *CSVParser) StreamRecords(ctx context.Context, reader io.Reader, batchSize int, callback RecordCallback) error {
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
				fieldType, _ := p.stream.Schema.GetType(headers[i])
				convertedValue := convertValue(value, fieldType)
				record[headers[i]] = convertedValue
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
				fieldType, _ := p.stream.Schema.GetType(headers[i])
				convertedValue := convertValue(value, fieldType)
				record[headers[i]] = convertedValue
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

// inferColumnType infers the data type of a CSV column from sample values
func inferColumnType(sampleRows [][]string, columnIndex int) types.DataType {
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

// convertValue converts a string value to the appropriate type based on schema
func convertValue(value string, fieldType types.DataType) interface{} {
	trimmed := strings.TrimSpace(value)

	// Handle null/empty values
	if trimmed == "" || strings.ToLower(trimmed) == "null" {
		return nil
	}

	// Convert based on field type
	switch fieldType {
	case types.Int32, types.Int64:
		if intVal, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return intVal
		}
	case types.Float32, types.Float64:
		if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return floatVal
		}
	case types.Bool:
		if boolVal, err := strconv.ParseBool(trimmed); err == nil {
			return boolVal
		}
	}

	// Default to string
	return trimmed
}

