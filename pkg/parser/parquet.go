package parser

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	pq "github.com/parquet-go/parquet-go"
)

// ParquetParser implements the Parser interface for Parquet files
// Note: Parquet schema inference doesn't need to read data, just metadata
type ParquetParser struct {
	config ParquetConfig
	stream *types.Stream
}

// NewParquetParser creates a new Parquet parser with the given configuration
func NewParquetParser(config ParquetConfig, stream *types.Stream) *ParquetParser {
	return &ParquetParser{
		config: config,
		stream: stream,
	}
}

// InferSchema reads Parquet file metadata to infer the schema
// For Parquet, schema is stored in file metadata, so we don't need to read data
// NOTE: reader must be io.ReaderAt for Parquet (use S3RangeReader or bytes.Reader)
func (p *ParquetParser) InferSchema(ctx context.Context, reader io.Reader) (*types.Stream, error) {
	logger.Debug("Inferring Parquet schema from file metadata")

	// Prepare reader and get file size
	readerAt, fileSize, err := prepareParquetReader(reader)
	if err != nil {
		return nil, err
	}

	// Open Parquet file to read schema
	pqFile, err := pq.OpenFile(readerAt, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Get the schema from parquet file
	schema := pqFile.Schema()

	// Convert parquet schema to Olake schema with proper type mapping
	for _, field := range schema.Fields() {
		olakeType := mapParquetTypeToOlake(field.Type())
		nullable := field.Optional()
		p.stream.UpsertField(field.Name(), olakeType, nullable)
	}

	logger.Infof("Inferred schema with %d fields from Parquet", len(schema.Fields()))
	return p.stream, nil
}

// StreamRecords reads and streams Parquet records with context support
// NOTE: reader must be io.ReaderAt for Parquet (use S3RangeReader or bytes.Reader)
func (p *ParquetParser) StreamRecords(ctx context.Context, reader io.Reader, callback RecordCallback) error {
	// Prepare reader and get file size
	readerAt, fileSize, err := prepareParquetReader(reader)
	if err != nil {
		return err
	}

	// Open Parquet file
	pqFile, err := pq.OpenFile(readerAt, fileSize)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Get schema to know column names
	schema := pqFile.Schema()
	fields := schema.Fields()

	recordCount := 0
	totalRowGroups := len(pqFile.RowGroups())

	// Process row groups one at a time to limit memory usage
	for rgIdx, rowGroup := range pqFile.RowGroups() {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		logger.Debugf("Processing row group %d/%d (approx %d rows)",
			rgIdx+1, totalRowGroups, rowGroup.NumRows())

		numRows := rowGroup.NumRows()

		// Read all columns into memory for THIS row group only
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
			// Check context cancellation every N rows
			if rowIdx%1000 == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			// Reconstruct row from column data
			record := make(map[string]any)
			for colIdx, field := range fields {
				if rowIdx < int64(len(columnData[colIdx])) {
					value := parquetValueToInterfaceWithType(columnData[colIdx][rowIdx], field.Type())
					record[field.Name()] = value
				}
			}

			if err := callback(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}

		logger.Debugf("Completed row group %d/%d (%d total records so far)",
			rgIdx+1, totalRowGroups, recordCount)
	}

	logger.Infof("Processed %d records from Parquet file", recordCount)
	return nil
}

// mapParquetTypeToOlake maps Parquet data types to Olake data types
func mapParquetTypeToOlake(pqType pq.Type) types.DataType {

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

		// Timestamp with precision (stored as INT64)
		// We convert to epoch seconds, so map to Timestamp
		if logicalType.Timestamp != nil {
			return types.Timestamp
		}

		// Time with precision (stored as INT32 or INT64)
		// We convert to seconds, so map to Int64
		if logicalType.Time != nil {
			return types.Int64
		}

		// Date (stored as INT32 - days since epoch)
		// We convert to epoch seconds, so map to Timestamp
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
		// We convert to epoch seconds, so map to Timestamp
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

// parquetValueToInterfaceWithType converts a parquet.Value to a Go interface{}
// Takes the field type to handle special cases like decimals and temporal types
// parquetValueToInterfaceWithType converts a parquet.Value to a Go interface{}
// Uses native parquet-go decimal support for proper conversion
func parquetValueToInterfaceWithType(val pq.Value, fieldType pq.Type) interface{} {
	if val.IsNull() {
		return nil
	}

	logicalType := fieldType.LogicalType()

	// Handle temporal types with logical type annotations
	if logicalType != nil {
		// Date (days since Unix epoch, stored as INT32)
		if logicalType.Date != nil {
			days := val.Int32()
			seconds := int64(days) * 86400
			t := time.Unix(seconds, 0).UTC()
			return t.Format(time.RFC3339)
		}

		// Timestamp (stored as INT64 with different precision)
		if logicalType.Timestamp != nil {
			rawValue := val.Int64()
			var t time.Time
			if logicalType.Timestamp.Unit.Nanos != nil {
				t = time.Unix(0, rawValue).UTC()
			} else if logicalType.Timestamp.Unit.Micros != nil {
				t = time.Unix(0, rawValue*1000).UTC()
			} else if logicalType.Timestamp.Unit.Millis != nil {
				t = time.Unix(0, rawValue*1_000_000).UTC()
			} else {
				t = time.Unix(rawValue, 0).UTC()
			}
			return t.Format(time.RFC3339)
		}

		// Time (stored as INT32 or INT64 with different precision)
		if logicalType.Time != nil {
			var rawValue int64
			if val.Kind() == pq.Int32 {
				rawValue = int64(val.Int32())
			} else {
				rawValue = val.Int64()
			}

			var seconds int64
			if logicalType.Time.Unit.Nanos != nil {
				seconds = rawValue / 1_000_000_000
			} else if logicalType.Time.Unit.Micros != nil {
				seconds = rawValue / 1_000_000
			} else if logicalType.Time.Unit.Millis != nil {
				seconds = rawValue / 1_000
			} else {
				seconds = rawValue
			}
			return seconds
		}

		// IMPROVED: Decimal handling using native parquet-go support
		if logicalType.Decimal != nil {
			// Try to use the native Decimal() method first

			if floatVal, err := strconv.ParseFloat(val.String(), 64); err == nil {
				return floatVal
			}

			// Fallback: manual conversion if native method fails
			scale := int(logicalType.Decimal.Scale)

			switch val.Kind() {
			case pq.Double, pq.Float:
				return val.Double()

			case pq.Int32:
				rawValue := int64(val.Int32())
				return convertScaledInt(rawValue, scale)

			case pq.Int64:
				rawValue := val.Int64()
				return convertScaledInt(rawValue, scale)

			case pq.FixedLenByteArray, pq.ByteArray:
				// Use native byte array to decimal conversion
				return convertByteArrayDecimal(val.ByteArray(), scale)

			default:
				return val.Double()
			}
		}
	}

	// Handle non-decimal types
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
	case pq.ByteArray, pq.FixedLenByteArray:
		byteData := val.ByteArray()
		if utf8.Valid(byteData) {
			return string(byteData)
		}
		return base64.StdEncoding.EncodeToString(byteData)
	case pq.Int96:
		// Legacy timestamp format
		nanos := val.Int64()
		t := time.Unix(0, nanos).UTC()
		return t.Format(time.RFC3339)
	default:
		return val.String()
	}
}

// convertScaledInt converts a scaled integer to float64
func convertScaledInt(rawValue int64, scale int) float64 {
	if scale == 0 {
		return float64(rawValue)
	}

	divisor := 1.0
	for i := 0; i < scale; i++ {
		divisor *= 10.0
	}
	return float64(rawValue) / divisor
}

// convertByteArrayDecimal converts a byte array decimal to float64
// Handles big-endian signed integers properly
func convertByteArrayDecimal(byteData []byte, scale int) float64 {
	if len(byteData) == 0 {
		return 0.0
	}

	// Check if it's a string representation
	if utf8.Valid(byteData) {
		strVal := string(byteData)
		if floatVal, err := strconv.ParseFloat(strVal, 64); err == nil {
			return floatVal
		}
	}

	// Convert big-endian byte array to signed integer
	// First byte indicates sign in two's complement
	isNegative := (byteData[0] & 0x80) != 0

	var rawValue int64

	if len(byteData) <= 8 {
		// Convert bytes to int64
		for _, b := range byteData {
			rawValue = rawValue<<8 | int64(b)
		}

		// Handle two's complement for negative numbers
		if isNegative && len(byteData) < 8 {
			// Sign extend: set all higher bits to 1
			mask := int64(-1) << uint(len(byteData)*8)
			rawValue |= mask
		}
	} else {
		// For values larger than 8 bytes, use math/big for precision
		return convertLargeByteArrayDecimal(byteData, scale)
	}

	return convertScaledInt(rawValue, scale)
}

// convertLargeByteArrayDecimal handles decimals larger than int64 using math/big
func convertLargeByteArrayDecimal(byteData []byte, scale int) float64 {
	// Use big.Int for arbitrary precision
	bigInt := new(big.Int).SetBytes(byteData)

	// Check if negative (two's complement)
	if (byteData[0] & 0x80) != 0 {
		// Calculate two's complement: flip bits and add 1
		ones := new(big.Int).Lsh(big.NewInt(1), uint(len(byteData)*8))
		ones.Sub(ones, big.NewInt(1))
		bigInt.Xor(bigInt, ones)
		bigInt.Add(bigInt, big.NewInt(1))
		bigInt.Neg(bigInt)
	}

	// Convert to float64 with scaling
	floatVal, _ := new(big.Float).SetInt(bigInt).Float64()

	if scale > 0 {
		divisor := 1.0
		for i := 0; i < scale; i++ {
			divisor *= 10.0
		}
		floatVal /= divisor
	}

	return floatVal
}

// prepareParquetReader validates and prepares a reader for Parquet file operations
// Returns the io.ReaderAt interface and file size needed for parquet-go
func prepareParquetReader(reader io.Reader) (io.ReaderAt, int64, error) {
	// Parquet requires io.ReaderAt interface
	readerAt, ok := reader.(io.ReaderAt)
	if !ok {
		return nil, 0, fmt.Errorf("parquet parser requires io.ReaderAt, got %T", reader)
	}

	// Determine file size (needed for OpenFile)
	var fileSize int64
	if seeker, ok := reader.(io.Seeker); ok {
		size, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to determine file size: %w", err)
		}
		// Seek back to beginning
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to seek to start: %w", err)
		}
		fileSize = size
	} else {
		return nil, 0, fmt.Errorf("parquet parser requires io.Seeker to determine file size")
	}

	return readerAt, fileSize, nil
}

// ParquetReaderWrapper wraps an io.ReaderAt with size info and implements io.Seeker
// This allows the Parquet parser to determine file size via Seek
// Used when reading from sources like S3 that provide ReaderAt but not Seeker
type ParquetReaderWrapper struct {
	readerAt io.ReaderAt
	size     int64
	offset   int64
}

// NewParquetReaderWrapper creates a new wrapper for io.ReaderAt that also implements io.Seeker
func NewParquetReaderWrapper(readerAt io.ReaderAt, size int64) *ParquetReaderWrapper {
	return &ParquetReaderWrapper{
		readerAt: readerAt,
		size:     size,
		offset:   0,
	}
}

func (w *ParquetReaderWrapper) ReadAt(p []byte, off int64) (n int, err error) {
	return w.readerAt.ReadAt(p, off)
}

func (w *ParquetReaderWrapper) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		w.offset = offset
	case io.SeekCurrent:
		w.offset += offset
	case io.SeekEnd:
		w.offset = w.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if w.offset < 0 {
		w.offset = 0
	}
	if w.offset > w.size {
		w.offset = w.size
	}

	return w.offset, nil
}

func (w *ParquetReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = w.readerAt.ReadAt(p, w.offset)
	w.offset += int64(n)
	return n, err
}
