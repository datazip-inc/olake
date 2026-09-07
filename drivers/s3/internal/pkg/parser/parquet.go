package parser

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/datazip-inc/olake/constants"
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

	// parquetTypeStateVersion is the first state version carrying this release's parquet type
	// fixes: Int96 as a timestamp rather than the raw integer string, and unsigned 32 bit
	// integers widened to Int64. Older state keeps the previous behavior so an existing
	// destination column does not change type on upgrade.
	parquetTypeStateVersion = 7
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
// NOTE: reader must be io.ReaderAt for Parquet (use objstorage.ReaderAt or bytes.Reader)
func (p *ParquetParser) InferSchema(_ context.Context, reader io.Reader) (_ *types.Stream, err error) {
	defer func() { err = DecodeFailure(err) }()

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
		olakeType := mapParquetNodeToOlake(field)
		nullable := field.Optional()
		p.stream.UpsertField(field.Name(), olakeType, nullable, false)
	}

	logger.Infof("Inferred schema with %d fields from Parquet", len(schema.Fields()))
	return p.stream, nil
}

// StreamRecords reads and streams Parquet records with context support
// NOTE: reader must be io.ReaderAt for Parquet (use objstorage.ReaderAt or bytes.Reader)
func (p *ParquetParser) StreamRecords(ctx context.Context, reader io.Reader, callback RecordCallback) (err error) {
	defer func() { err = DecodeFailure(err) }()

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

	// Decoder holds the per-file schema state and the leaf-column index used to decode rows.
	decoder := newRowDecoder(pqFile.Schema())

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

		// A flat schema is read column by column, materializing the whole row group at once,
		// which is faster than assembling rows a batch at a time. Nested and repeated columns
		// contribute a variable number of values per row, so they cannot be indexed by row and
		// take the row-at-a-time path instead.
		var streamErr error
		if decoder.hasGroupField {
			streamErr = p.streamRowGroupRows(ctx, rowGroup, decoder, rowReadBatchSize, callback, &recordCount)
		} else {
			streamErr = p.streamRowGroupColumns(ctx, rowGroup, decoder, callback, &recordCount)
		}
		if streamErr != nil {
			return fmt.Errorf("failed to read row group %d: %w", rgIdx, streamErr)
		}

		logger.Debugf("Completed row group %d/%d (%d total records so far)",
			rgIdx+1, totalRowGroups, recordCount)
	}

	logger.Infof("Processed %d records from Parquet file", recordCount)
	return nil
}

// streamRowGroupRows reads one row group a batch of rows at a time. Rows are read whole rather
// than column by column so that repeated and nested columns, whose value count per row varies,
// stay attributable to the row they came from. This is the path for schemas with group or
// repeated fields; flat schemas use the faster streamRowGroupColumns.
func (p *ParquetParser) streamRowGroupRows(ctx context.Context, rowGroup pq.RowGroup,
	decoder *rowDecoder, batchSize int, callback RecordCallback, recordCount *int,
) error {
	rows := rowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]pq.Row, batchSize)
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

			record, err := decoder.decode(rowBuf[i])
			if err != nil {
				return err
			}
			if err := callback(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			*recordCount++
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("failed to read rows: %w", readErr)
		}
	}
}

// streamRowGroupColumns reads a flat row group column by column, materializing every column's
// values and then reconstructing rows by index. parquet-go emits null values inline (one value
// per row position, with the nulls carrying no data), so a column's values stay aligned to the
// stream index even when the column is nullable. It must not be used when the schema has group or
// repeated fields: those contribute a variable number of values per row, so a value's position
// no longer identifies its row. The whole row group is held in memory, as it was before the
// parser batched reads, which is why the caller processes row groups one at a time.
func (p *ParquetParser) streamRowGroupColumns(ctx context.Context, rowGroup pq.RowGroup,
	decoder *rowDecoder, callback RecordCallback, recordCount *int,
) error {
	numRows := rowGroup.NumRows()
	columnChunks := rowGroup.ColumnChunks()
	columnData := make([][]pq.Value, len(columnChunks))
	for colIdx, columnChunk := range columnChunks {
		values, err := readColumnValues(columnChunk, numRows)
		if err != nil {
			return err
		}
		columnData[colIdx] = values
	}

	fields := decoder.fields
	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
		if *recordCount%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		record := make(map[string]any, len(fields))
		for colIdx, field := range fields {
			// A flat field owns exactly one leaf column, in order, so colIdx indexes both.
			var value any
			if colIdx < len(columnData) && rowIdx < int64(len(columnData[colIdx])) {
				value = parquetValueToInterfaceWithType(columnData[colIdx][rowIdx], field.Type())
			}
			record[field.Name()] = value
		}

		if err := callback(ctx, record); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
		*recordCount++
	}
	return nil
}

// readColumnValues reads every value of a column chunk, page by page. Null values are included
// inline, so the returned slice has one entry per row in the row group.
func readColumnValues(columnChunk pq.ColumnChunk, numRows int64) ([]pq.Value, error) {
	pages := columnChunk.Pages()
	defer pages.Close()

	values := make([]pq.Value, 0, numRows)
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			return values, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read page: %w", err)
		}

		// Read into the tail of the result: no per-page buffer, no copy out of it.
		count := int(page.NumValues())
		values = slices.Grow(values, count)
		n, err := page.Values().ReadValues(values[len(values) : len(values)+count])
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read page values: %w", err)
		}
		values = values[:len(values)+n]
	}
}

// rowDecoder converts the rows of one parquet file into records. It holds the per-file schema
// state that used to be threaded through every call and precomputes colToField so a row can
// be decoded in a single pass instead of rescanning the row per leaf field.
type rowDecoder struct {
	schema        *pq.Schema
	fields        []pq.Field
	hasGroupField bool
	// colToField maps a leaf column index to the index of the plain (single-valued) top-level
	// field that owns it, or -1 for columns that are reconstructed rather than read by column.
	colToField []int
}

// newRowDecoder precomputes the schema state a row decode needs. leafOffsets is the leaf
// column index each top-level field starts at; a field spans more than one leaf when it is a
// list, map or nested struct, so this is not the field's own index.
func newRowDecoder(schema *pq.Schema) *rowDecoder {
	fields := schema.Fields()
	leafOffsets := make([]int, len(fields))
	hasGroupField := false
	totalLeaves := 0
	for i, field := range fields {
		leafOffsets[i] = totalLeaves
		totalLeaves += leafCount(field)
		if isMultiValued(field) {
			hasGroupField = true
		}
	}

	colToField := make([]int, totalLeaves)
	for i := range colToField {
		colToField[i] = -1
	}
	for i, field := range fields {
		// Multivalued fields (groups and repeated leaves) span several leaves and are
		// assembled by Reconstruct, so they are not indexed for the single-value column walk.
		if !isMultiValued(field) {
			colToField[leafOffsets[i]] = i
		}
	}

	return &rowDecoder{
		schema:        schema,
		fields:        fields,
		hasGroupField: hasGroupField,
		colToField:    colToField,
	}
}

// decode turns one parquet row into a record. Leaf fields are converted from their own
// parquet value so that logical types (dates, decimals, timestamps) keep their meaning; group
// fields are assembled by parquet-go, which resolves the repetition and definition levels that
// describe nested structure.
func (d *rowDecoder) decode(row pq.Row) (map[string]any, error) {
	var groups map[string]any
	if d.hasGroupField {
		groups = map[string]any{}
		if err := d.schema.Reconstruct(&groups, row); err != nil {
			return nil, fmt.Errorf("failed to reconstruct row: %w", err)
		}
	}

	record := make(map[string]any, len(d.fields))
	for _, field := range d.fields {
		if isMultiValued(field) {
			// Reconstruct yields the logical shape ([]any for lists, map[string]any for maps
			// and structs) but strips the leaves' logical types: parquet-go assigns raw
			// physical values into any-typed destinations. convertReconstructed walks the
			// value alongside the schema to give every nested leaf the same conversion a plain
			// leaf column gets.
			record[field.Name()] = convertReconstructed(groups[field.Name()], field)
			continue
		}
		// Plain leaves default to nil so a column absent from the row still yields its key;
		// the single row pass below overwrites the ones that carry a value.
		record[field.Name()] = nil
	}

	// Row values are ordered by column. A single pass assigns each plain leaf its value rather
	// than scanning the whole row per field, which made row decode O(fields x row_len).
	for i := range row {
		col := int(row[i].Column())
		if col < 0 || col >= len(d.colToField) {
			continue
		}
		fieldIdx := d.colToField[col]
		if fieldIdx < 0 {
			continue
		}
		field := d.fields[fieldIdx]
		record[field.Name()] = parquetValueToInterfaceWithType(row[i], field.Type())
	}
	return record, nil
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
		return convertLeafNode(value, node)
	}
	if logicalType := node.Type().LogicalType(); logicalType != nil {
		switch {
		case logicalType.List != nil:
			return convertListNode(value, node)
		case logicalType.Map != nil:
			return convertMapNode(value, node)
		}
	}
	// A group with no List/Map annotation is a nested struct.
	return convertStructNode(value, node)
}

// convertLeafNode converts a reconstructed leaf value. A repeated leaf reconstructs into a
// slice of its leaf values; a single leaf is rewrapped as a parquet value of the column's own
// type so it routes through the exact conversion plain leaf columns get.
func convertLeafNode(value any, node pq.Node) any {
	if elements, ok := value.([]any); ok {
		for i, element := range elements {
			elements[i] = convertReconstructed(element, node)
		}
		return elements
	}
	return parquetValueToInterfaceWithType(pq.ValueOf(value), node.Type())
}

// convertListNode converts the elements of a LIST-annotated group, resolving the element node
// so each entry gets its leaf conversion. A value whose shape does not match falls back to
// normalizeReconstructed rather than guessing.
func convertListNode(value any, node pq.Node) any {
	if element := listElementNode(node); element != nil {
		if elements, ok := value.([]any); ok {
			for i, entry := range elements {
				elements[i] = convertReconstructed(entry, element)
			}
			return elements
		}
	}
	return normalizeReconstructed(value)
}

// convertMapNode converts the values of a MAP-annotated group, resolving the value node so
// each entry gets its leaf conversion. A value whose shape does not match falls back to
// normalizeReconstructed rather than guessing.
func convertMapNode(value any, node pq.Node) any {
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

// convertStructNode converts a nested struct group. A repeated struct reconstructs into a
// slice of struct values; a single struct into a map keyed by field name. A value whose shape
// does not match falls back to normalizeReconstructed rather than guessing.
func convertStructNode(value any, node pq.Node) any {
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

// mapParquetTypeToOlake maps Parquet data types to Olake data types. Logical type annotations
// carry semantic meaning and take priority; a column with none falls back to its physical type.
func mapParquetTypeToOlake(pqType pq.Type) types.DataType {
	logicalType := pqType.LogicalType()
	switch {
	case logicalType == nil:
		// no annotation; fall through to the physical mapping below
	case logicalType.Integer != nil:
		return mapParquetIntegerType(logicalType.Integer.BitWidth, logicalType.Integer.IsSigned)
	case logicalType.Timestamp != nil:
		// Mapped at the precision the unit declares; the value conversion keeps the full
		// precision as a time.Time.
		switch {
		case logicalType.Timestamp.Unit.Nanos != nil:
			return types.TimestampNano
		case logicalType.Timestamp.Unit.Micros != nil:
			return types.TimestampMicro
		case logicalType.Timestamp.Unit.Millis != nil:
			return types.TimestampMilli
		default:
			return types.Timestamp
		}
	case logicalType.Time != nil:
		// Time is converted to seconds, so it maps to Int64.
		return types.Int64
	case logicalType.Date != nil:
		return types.Timestamp
	case logicalType.Decimal != nil:
		return types.Float64
	case logicalType.UTF8 != nil, logicalType.Json != nil, logicalType.UUID != nil,
		logicalType.Enum != nil, logicalType.Bson != nil:
		return types.String
	case logicalType.List != nil:
		return types.Array
	case logicalType.Map != nil:
		return types.Object
	}
	return mapParquetPhysicalType(pqType)
}

// mapParquetIntegerType widens integer logical types the way the SQL drivers do: unsigned
// values step up one width so the top of the range does not wrap negative (8/16 -> Int32,
// 32/64 -> Int64, matching how the MySQL driver maps "unsigned int" to Int64). Unsigned 64-bit
// has no wider signed type to widen into and stays Int64, as "unsigned bigint" does there.
func mapParquetIntegerType(bitWidth int8, signed bool) types.DataType {
	if !signed {
		switch bitWidth {
		case 8, 16:
			return types.Int32
		case 32:
			// Widened only from the gated version; older state keeps the Int32 pre-gate
			// builds inferred, wrapping values above 2^31-1 negative as they did then.
			if constants.LoadedStateVersion >= parquetTypeStateVersion {
				return types.Int64
			}
			return types.Int32
		case 64:
			return types.Int64
		}
	}
	switch bitWidth {
	case 8, 16, 32:
		return types.Int32
	case 64:
		return types.Int64
	default:
		logger.Warnf("Unexpected integer bit width %d, defaulting to Int32", bitWidth)
		return types.Int32
	}
}

// mapParquetPhysicalType maps a Parquet physical type to an Olake type for columns without a
// logical annotation.
func mapParquetPhysicalType(pqType pq.Type) types.DataType {
	switch pqType.Kind() {
	case pq.Boolean:
		return types.Bool
	case pq.Int32:
		return types.Int32
	case pq.Int64:
		return types.Int64
	case pq.Int96:
		// Int96 is a legacy timestamp. The value conversion returns a time.Time to match (or,
		// before state version 7, the raw integer string; see parquetValueToInterfaceWithType).
		return types.Timestamp
	case pq.Float:
		return types.Float32
	case pq.Double:
		return types.Float64
	case pq.ByteArray, pq.FixedLenByteArray:
		// Byte arrays without logical type annotation default to string.
		return types.String
	default:
		// Unknown types default to string for safety.
		logger.Warnf("Unknown Parquet type %v, defaulting to string", pqType.Kind())
		return types.String
	}
}

// parquetValueToInterfaceWithType converts a parquet.Value to a Go interface{}. A logical type
// annotation gives the value its semantic meaning; a value with none (or one the logical
// handling leaves through) is converted from its physical type.
func parquetValueToInterfaceWithType(val pq.Value, fieldType pq.Type) interface{} {
	if val.IsNull() {
		return nil
	}
	if v, handled := convertLogicalValue(val, fieldType); handled {
		return v
	}
	return convertPhysicalValue(val)
}

// convertLogicalValue converts a value carrying a logical type annotation, returning handled
// false when the column has no annotation or one it does not own (UUID with an unexpected byte
// count, unsigned widths other than 32) so the caller applies the physical conversion instead.
func convertLogicalValue(val pq.Value, fieldType pq.Type) (interface{}, bool) {
	logicalType := fieldType.LogicalType()
	if logicalType == nil {
		return nil, false
	}

	switch {
	// Date (days since Unix epoch, stored as INT32). Returned as a time.Time rather than a
	// formatted string so no precision is lost to a format and reparse, and so the value
	// matches what the database drivers emit for a date.
	case logicalType.Date != nil:
		seconds := int64(val.Int32()) * 86400
		return time.Unix(seconds, 0).UTC(), true

	// Timestamp (stored as INT64 with different precision). Millis and micros must not be
	// scaled up into nanoseconds: int64 nanoseconds only span about 1678-2262, so a timestamp
	// like 9999-12-31 overflows and wraps back to 1816. time.UnixMilli and time.UnixMicro
	// carry the full range.
	case logicalType.Timestamp != nil:
		rawValue := val.Int64()
		switch {
		case logicalType.Timestamp.Unit.Nanos != nil:
			return time.Unix(0, rawValue).UTC(), true
		case logicalType.Timestamp.Unit.Micros != nil:
			return time.UnixMicro(rawValue).UTC(), true
		case logicalType.Timestamp.Unit.Millis != nil:
			return time.UnixMilli(rawValue).UTC(), true
		default:
			return time.Unix(rawValue, 0).UTC(), true
		}

	// Time (stored as INT32 or INT64 with different precision), converted to seconds.
	case logicalType.Time != nil:
		rawValue := val.Int64()
		if val.Kind() == pq.Int32 {
			rawValue = int64(val.Int32())
		}
		switch {
		case logicalType.Time.Unit.Nanos != nil:
			return rawValue / 1_000_000_000, true
		case logicalType.Time.Unit.Micros != nil:
			return rawValue / 1_000_000, true
		case logicalType.Time.Unit.Millis != nil:
			return rawValue / 1_000, true
		default:
			return rawValue, true
		}

	// Decimal stored as INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY.
	case logicalType.Decimal != nil:
		dec, err := decodeParquetDecimal(val, logicalType.Decimal.Scale)
		if err != nil {
			logger.Warnf("decimal decode failed: %v", err)
			return nil, true
		}
		v, _ := dec.Float64()
		return v, true

	// UUID (stored as a 16 byte FIXED_LEN_BYTE_ARRAY) as the canonical 8-4-4-4-12 hex string,
	// matching what the database drivers emit. An unexpected byte count falls through to the
	// physical byte-array handling rather than surface an opaque blob under a UUID label.
	case logicalType.UUID != nil:
		if b := val.ByteArray(); len(b) == 16 {
			return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), true
		}
		logger.Warnf("uuid column carried %d bytes, expected 16", len(val.ByteArray()))

	// Unsigned 32 bit integers live in an INT32 physical column, so the top half of the range
	// reads back negative unless the bits are reinterpreted and widened. The narrower widths
	// fit in an int32 already and unsigned 64 has nothing wider to widen into, so both take
	// the physical path.
	case logicalType.Integer != nil && !logicalType.Integer.IsSigned && logicalType.Integer.BitWidth == 32 &&
		constants.LoadedStateVersion >= parquetTypeStateVersion:
		//nolint:gosec // G115: reinterpreting the physical bits as unsigned is the intent
		return int64(uint32(val.Int32())), true
	}

	return nil, false
}

// convertPhysicalValue converts a value from its physical type. Int32 and Float keep their
// native Go width so the value matches the Int32/Float32 the schema infers for the column;
// widening them here would have the destination promote the column to long/double.
func convertPhysicalValue(val pq.Value) interface{} {
	switch val.Kind() {
	case pq.Boolean:
		return val.Boolean()
	case pq.Int32:
		// Native width only from the gated version. Pre-gate builds widened to int64/float64,
		// which made the destination create the column bigint/double; narrowing it on upgrade
		// is a transition Iceberg cannot make, so older state keeps the wide value.
		if constants.LoadedStateVersion < parquetTypeStateVersion {
			return int64(val.Int32())
		}
		return val.Int32()
	case pq.Int64:
		return val.Int64()
	case pq.Float:
		if constants.LoadedStateVersion < parquetTypeStateVersion {
			return float64(val.Float())
		}
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
		// Int96 is a legacy timestamp. From state version 7 it returns a time.Time so the
		// value agrees with the Timestamp schema; older state emits the raw 96-bit integer as
		// a string, as pre-gate builds did, so an existing destination column stays String
		// across the upgrade instead of changing type.
		if constants.LoadedStateVersion >= parquetTypeStateVersion {
			return int96ToTime(val.Int96())
		}
		return val.String()
	default:
		// For Group types (nested structures, maps, lists) and unknown types, use the string
		// representation which serializes the nested structure.
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
