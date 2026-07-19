package parser

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"time"
	"unicode/utf8"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/shopspring/decimal"
)

const (
	// julianDayOfUnixEpoch is the Julian day number for 1970-01-01, the origin Int96
	// timestamps are offset from.
	julianDayOfUnixEpoch = 2440588

	// rowReadBatchSize is how many rows are read from a row group at a time, bounding how
	// much of a row group is held in memory while still amortizing reads over the network.
	rowReadBatchSize = 256
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
func (p *ParquetParser) InferSchema(_ context.Context, reader io.Reader) (*types.Stream, error) {
	logger.Debug("Inferring Parquet schema from file metadata")

	// Prepare reader and get file size
	readerAt, fileSize, err := prepareParquetReader(reader)
	if err != nil {
		return nil, err
	}

	// Open Parquet file to read schema
	pqFile, err := pq.OpenFile(readerAt, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %s", err)
	}

	// Get the schema from parquet file
	schema := pqFile.Schema()

	// Convert parquet schema to Olake schema with proper type mapping
	for _, field := range schema.Fields() {
		olakeType := mapParquetNodeToOlake(field)
		nullable := field.Optional()
		p.stream.UpsertField(field.Name(), olakeType, nullable, false)
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
		return fmt.Errorf("failed to open parquet file: %s", err)
	}

	// Get schema to know column names
	schema := pqFile.Schema()
	fields := schema.Fields()

	// Leaf column index each top-level field starts at. A field spans more than one leaf
	// when it is a list, map or nested struct, so this is not the field's own index.
	leafOffsets := make([]int, len(fields))
	hasGroupField := false
	for i, field := range fields {
		if i > 0 {
			leafOffsets[i] = leafOffsets[i-1] + leafCount(fields[i-1])
		}
		if isMultiValued(field) {
			hasGroupField = true
		}
	}

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

		if err := p.streamRowGroup(ctx, rowGroup, schema, fields, leafOffsets, hasGroupField, callback, &recordCount); err != nil {
			return fmt.Errorf("failed to read row group %d: %s", rgIdx, err)
		}

		logger.Debugf("Completed row group %d/%d (%d total records so far)",
			rgIdx+1, totalRowGroups, recordCount)
	}

	logger.Infof("Processed %d records from Parquet file", recordCount)
	return nil
}

// streamRowGroup reads one row group a batch of rows at a time. Rows are read whole rather
// than column by column so that repeated and nested columns, whose value count per row
// varies, stay attributable to the row they came from.
func (p *ParquetParser) streamRowGroup(ctx context.Context, rowGroup pq.RowGroup, schema *pq.Schema,
	fields []pq.Field, leafOffsets []int, hasGroupField bool, callback RecordCallback, recordCount *int,
) error {
	rows := rowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]pq.Row, rowReadBatchSize)
	for {
		n, readErr := rows.ReadRows(rowBuf)
		for i := 0; i < n; i++ {
			if *recordCount%1000 == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			record, err := buildRecord(schema, fields, leafOffsets, hasGroupField, rowBuf[i])
			if err != nil {
				return err
			}
			if err := callback(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %s", err)
			}
			*recordCount++
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("failed to read rows: %s", readErr)
		}
	}
}

// buildRecord turns one parquet row into a record. Leaf fields are converted from their own
// parquet value so that logical types (dates, decimals, timestamps) keep their meaning;
// group fields are assembled by parquet-go, which resolves the repetition and definition
// levels that describe nested structure.
func buildRecord(schema *pq.Schema, fields []pq.Field, leafOffsets []int, hasGroupField bool, row pq.Row) (map[string]any, error) {
	var groups map[string]any
	if hasGroupField {
		groups = map[string]any{}
		if err := schema.Reconstruct(&groups, row); err != nil {
			return nil, fmt.Errorf("failed to reconstruct row: %s", err)
		}
	}

	record := make(map[string]any, len(fields))
	for i, field := range fields {
		if isMultiValued(field) {
			// Reconstruct yields the logical shape ([]any for lists, map[string]any for
			// maps and structs) but strips the leaves' logical types: parquet-go assigns
			// raw physical values into any-typed destinations. convertReconstructed walks
			// the value alongside the schema to give every nested leaf the same conversion
			// a plain leaf column gets.
			record[field.Name()] = convertReconstructed(groups[field.Name()], field)
			continue
		}
		value, found := leafValue(row, leafOffsets[i])
		if !found {
			record[field.Name()] = nil
			continue
		}
		record[field.Name()] = parquetValueToInterfaceWithType(value, field.Type())
	}
	return record, nil
}

// leafValue finds the value a non-repeated leaf column contributed to a row. Row values are
// ordered by column but the index is not the column number once groups are present.
func leafValue(row pq.Row, column int) (pq.Value, bool) {
	for _, value := range row {
		if int(value.Column()) == column {
			return value, true
		}
	}
	return pq.Value{}, false
}

// isMultiValued reports whether a field can contribute more than one value to a row, which
// is true of groups (lists, maps, nested structs) and of repeated leaves. Those cannot be
// read as one value at a fixed column index the way a plain leaf can.
func isMultiValued(node pq.Node) bool {
	return !node.Leaf() || node.Repeated()
}

// leafCount reports how many leaf columns a schema node occupies.
func leafCount(node pq.Node) int {
	if node.Leaf() {
		return 1
	}
	total := 0
	for _, field := range node.Fields() {
		total += leafCount(field)
	}
	return total
}

// convertReconstructed walks a value assembled by schema.Reconstruct alongside its schema
// node and applies the leaf conversion to every nested leaf, so a date inside a struct or a
// list of timestamps carries the same time.Time a plain leaf column would, not the raw
// int32 day count or int64 tick count reconstruction assigns into any-typed destinations.
// A value whose shape does not match its node falls back to normalizeReconstructed rather
// than guessing.
func convertReconstructed(value any, node pq.Node) any {
	if value == nil {
		return nil
	}

	if node.Leaf() {
		// A repeated leaf reconstructs into a slice of its leaf values.
		if elements, ok := value.([]any); ok {
			for i, element := range elements {
				elements[i] = convertReconstructed(element, node)
			}
			return elements
		}
		// Rewrapping the reconstructed Go value as a parquet value of the column's own
		// type routes it through the exact conversion plain leaf columns get.
		return parquetValueToInterfaceWithType(pq.ValueOf(value), node.Type())
	}

	if logicalType := node.Type().LogicalType(); logicalType != nil {
		switch {
		case logicalType.List != nil:
			if element := listElementNode(node); element != nil {
				if elements, ok := value.([]any); ok {
					for i, entry := range elements {
						elements[i] = convertReconstructed(entry, element)
					}
					return elements
				}
			}
			return normalizeReconstructed(value)
		case logicalType.Map != nil:
			if valueNode := mapValueNode(node); valueNode != nil {
				if entries, ok := value.(map[string]any); ok {
					for key, entry := range entries {
						entries[key] = convertReconstructed(entry, valueNode)
					}
					return entries
				}
			}
			return normalizeReconstructed(value)
		}
	}

	// A group with no List/Map annotation is a nested struct; a repeated one reconstructs
	// into a slice of struct values.
	if elements, ok := value.([]any); ok && node.Repeated() {
		for i, element := range elements {
			elements[i] = convertReconstructed(element, node)
		}
		return elements
	}
	if entries, ok := value.(map[string]any); ok {
		for _, field := range node.Fields() {
			if entry, ok := entries[field.Name()]; ok {
				entries[field.Name()] = convertReconstructed(entry, field)
			}
		}
		return entries
	}
	return normalizeReconstructed(value)
}

// listElementNode resolves the element node of a LIST-annotated group the way parquet-go's
// reconstruction does (the .list.element layout, tolerating the .item name some pyarrow and
// polars versions wrote). Nil when the shape is unexpected.
func listElementNode(node pq.Node) pq.Node {
	if list := fieldByName(node, "list"); list != nil {
		for _, name := range []string{"element", "item"} {
			if element := fieldByName(list, name); element != nil {
				return element
			}
		}
	}
	return nil
}

// mapValueNode resolves the value node of a MAP-annotated group the way parquet-go's
// reconstruction does (a repeated .key_value or legacy .map group carrying key and value).
// Nil when the shape is unexpected.
func mapValueNode(node pq.Node) pq.Node {
	for _, name := range []string{"key_value", "map"} {
		if keyValue := fieldByName(node, name); keyValue != nil && !keyValue.Leaf() {
			if value := fieldByName(keyValue, "value"); value != nil {
				return value
			}
		}
	}
	return nil
}

func fieldByName(node pq.Node, name string) pq.Node {
	for _, field := range node.Fields() {
		if field.Name() == name {
			return field
		}
	}
	return nil
}

// normalizeReconstructed rewrites the byte slices parquet-go reconstructs into the text the
// rest of the pipeline expects, applying the same UTF-8 test used for leaf byte arrays.
func normalizeReconstructed(value any) any {
	switch typed := value.(type) {
	case []byte:
		if utf8.Valid(typed) {
			return string(typed)
		}
		return base64.StdEncoding.EncodeToString(typed)
	case map[string]any:
		for key, element := range typed {
			typed[key] = normalizeReconstructed(element)
		}
		return typed
	case []any:
		for i, element := range typed {
			typed[i] = normalizeReconstructed(element)
		}
		return typed
	default:
		return value
	}
}

// mapParquetNodeToOlake maps a Parquet schema node to an Olake data type. Group nodes
// (lists, maps and nested structs) carry no physical type, so they are resolved from the
// node shape before mapParquetTypeToOlake is reached: calling Kind() on a group panics.
func mapParquetNodeToOlake(node pq.Node) types.DataType {
	if node.Leaf() {
		// A repeated leaf holds a list of values, even though it has a physical type.
		if node.Repeated() {
			return types.Array
		}
		return mapParquetTypeToOlake(node.Type())
	}
	if logicalType := node.Type().LogicalType(); logicalType != nil {
		if logicalType.List != nil {
			return types.Array
		}
		if logicalType.Map != nil {
			return types.Object
		}
	}
	// A group with no List/Map annotation is a nested struct.
	return types.Object
}

// mapParquetTypeToOlake maps Parquet data types to Olake data types
func mapParquetTypeToOlake(pqType pq.Type) types.DataType {
	// First, check for logical type annotations which provide semantic meaning
	if logicalType := pqType.LogicalType(); logicalType != nil {
		// Integer logical types (INT_8, INT_16, INT_32, INT_64)
		if logicalType.Integer != nil {
			// Unsigned values use the full width of their physical type, so they are
			// widened one step to avoid wrapping negative (matching how the MySQL driver
			// maps "unsigned int" to Int64). Unsigned 64-bit has no wider signed type to
			// widen into and stays Int64, as "unsigned bigint" does there.
			if !logicalType.Integer.IsSigned {
				switch logicalType.Integer.BitWidth {
				case 8, 16:
					return types.Int32
				case 32, 64:
					return types.Int64
				}
			}
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

		// Timestamp with precision (stored as INT64), mapped at the precision the unit
		// declares; the value conversion keeps the full precision as a time.Time.
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

		// Time with precision (stored as INT32 or INT64)
		// We convert to seconds, so map to Int64
		if logicalType.Time != nil {
			return types.Int64
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

// parquetValueToInterface converts a parquet.Value to a Go interface{}
func parquetValueToInterfaceWithType(val pq.Value, fieldType pq.Type) interface{} {
	if val.IsNull() {
		return nil
	}

	logicalType := fieldType.LogicalType()

	// Handle temporal types with logical type annotations
	if logicalType != nil {
		// Date (days since Unix epoch, stored as INT32). Returned as a time.Time rather
		// than a formatted string so no precision is lost to a format and reparse, and so
		// the value matches what the database drivers emit for a date.
		if logicalType.Date != nil {
			days := val.Int32()
			seconds := int64(days) * 86400
			return time.Unix(seconds, 0).UTC()
		}

		// Timestamp (stored as INT64 with different precision).
		// Millis and micros must not be scaled up into nanoseconds: int64 nanoseconds only
		// span about 1678-2262, so a timestamp like 9999-12-31 overflows and wraps back to
		// 1816. time.UnixMilli and time.UnixMicro carry the full range.
		if logicalType.Timestamp != nil {
			rawValue := val.Int64()
			var t time.Time
			if logicalType.Timestamp.Unit.Nanos != nil {
				t = time.Unix(0, rawValue).UTC()
			} else if logicalType.Timestamp.Unit.Micros != nil {
				t = time.UnixMicro(rawValue).UTC()
			} else if logicalType.Timestamp.Unit.Millis != nil {
				t = time.UnixMilli(rawValue).UTC()
			} else {
				t = time.Unix(rawValue, 0).UTC()
			}
			return t
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

		// Decimal stored as INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY
		if logicalType.Decimal != nil {
			dec, err := decodeParquetDecimal(val, logicalType.Decimal.Scale)
			if err != nil {
				logger.Warnf("decimal decode failed: %v", err)
				return nil
			}
			v, _ := dec.Float64()
			return v
		}

		// UUID (stored as a 16 byte FIXED_LEN_BYTE_ARRAY) as the canonical 8-4-4-4-12 hex
		// string, matching what the database drivers emit. Without this the bytes fall
		// through to the byte array handling below and surface as an opaque blob.
		if logicalType.UUID != nil {
			if b := val.ByteArray(); len(b) == 16 {
				return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
			}
			logger.Warnf("uuid column carried %d bytes, expected 16", len(val.ByteArray()))
		}

		// Unsigned 32 bit integers live in an INT32 physical column, so the top half of the
		// range reads back negative unless the bits are reinterpreted and widened. The
		// narrower widths need no help (they fit in an int32 already) and unsigned 64 has
		// nothing wider to widen into, so both take the physical path below.
		if logicalType.Integer != nil && !logicalType.Integer.IsSigned && logicalType.Integer.BitWidth == 32 {
			//nolint:gosec // G115: reinterpreting the physical bits as unsigned is the intent
			return int64(uint32(val.Int32()))
		}
	}

	// Handle non-decimal types. Int32 and Float keep their native Go width so the value
	// matches the Int32/Float32 the schema infers for the column; widening them here would
	// have the destination promote the column to long/double.
	switch val.Kind() {
	case pq.Boolean:
		return val.Boolean()
	case pq.Int32:
		return val.Int32()
	case pq.Int64:
		return val.Int64()
	case pq.Float:
		return val.Float()
	case pq.Double:
		return val.Double()
	case pq.ByteArray, pq.FixedLenByteArray:
		byteData := val.ByteArray()
		if utf8.Valid(byteData) {
			return string(byteData)
		}
		return base64.StdEncoding.EncodeToString(byteData)
	case pq.Int96:
		// Int96 is a legacy timestamp, and the schema maps it to Timestamp, so return a
		// time.Time. String() emitted the raw 96 bit integer in decimal, which disagreed
		// with the schema and collapsed the column to a string.
		return int96ToTime(val.Int96())
	default:

		// For Group types (nested structures, maps, lists) and unknown types,
		// use the string representation which serializes the nested structure
		return val.String()
	}
}

// int96ToTime decodes the legacy Impala/Hive Int96 timestamp layout: the low 64 bits hold
// nanoseconds within the day and the high 32 bits hold the Julian day number.
func int96ToTime(i96 deprecated.Int96) time.Time {
	nanosOfDay := i96.Int64()
	julianDay := int64(i96[2])
	return time.Unix((julianDay-julianDayOfUnixEpoch)*86400, nanosOfDay).UTC()
}

func decodeParquetDecimal(val pq.Value, scale int32) (decimal.Decimal, error) {
	var unscaled *big.Int

	switch val.Kind() {
	case pq.Int32:
		unscaled = big.NewInt(int64(val.Int32()))

	case pq.Int64:
		unscaled = big.NewInt(val.Int64())

	case pq.FixedLenByteArray, pq.ByteArray:
		raw := val.ByteArray()
		if len(raw) == 0 {
			return decimal.Zero, nil
		}

		unscaled = new(big.Int).SetBytes(raw)

		// two's complement (signed)
		if raw[0]&0x80 != 0 {
			// Check for potential overflow before conversion
			if uint(len(raw)) > (^uint(0))/8 {
				return decimal.Zero, fmt.Errorf("decimal byte array too large for bit length calculation")
			}
			//nolint:gosec // G115: overflow check performed above
			bitLen := uint(len(raw) * 8)
			maxValue := new(big.Int).Lsh(big.NewInt(1), bitLen)
			unscaled.Sub(unscaled, maxValue)
		}

	default:
		return decimal.Zero, fmt.Errorf("unsupported decimal kind: %v", val.Kind())
	}

	//  decimal library handles scale natively
	return decimal.NewFromBigInt(unscaled, -scale), nil
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
			return nil, 0, fmt.Errorf("failed to determine file size: %s", err)
		}
		// Seek back to beginning
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to seek to start: %s", err)
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
