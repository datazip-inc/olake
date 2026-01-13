package parser

import (
	"context"
	"io"

	"github.com/datazip-inc/olake/types"
)

// Parser defines the interface for file format parsers
// This interface separates parsing logic from storage operations (S3, GCS, etc.)
type Parser interface {
	// InferSchema reads a small sample from the reader to infer schema
	// Should not load entire file into memory
	InferSchema(ctx context.Context, reader io.Reader) (*types.Stream, error)

	// StreamRecords reads records from the reader and calls callback for each record
	// Supports context cancellation to prevent resource leaks
	// Batching is handled by the destination layer, not the parser
	StreamRecords(ctx context.Context, reader io.Reader, callback RecordCallback) error
}

// RecordCallback is called for each record during streaming
// Return error to stop processing
type RecordCallback func(ctx context.Context, record map[string]any) error

// CSVConfig holds CSV-specific parsing configuration
type CSVConfig struct {
	Delimiter      string `json:"delimiter"`       // Default: ","
	HasHeader      bool   `json:"has_header"`      // Default: true
	SkipRows       int    `json:"skip_rows"`       // Number of rows to skip at the beginning
	QuoteCharacter string `json:"quote_character"` // Default: "\""
}

// JSONConfig holds JSON-specific parsing configuration
type JSONConfig struct {
	LineDelimited bool `json:"line_delimited"` // Default: true (JSONL format)
}

// ParquetConfig holds Parquet-specific parsing configuration
type ParquetConfig struct {
	StreamingEnabled bool `json:"streaming_enabled"` // Default: true - use range requests
}
