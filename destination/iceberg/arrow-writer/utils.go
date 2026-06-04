package arrowwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
)

const (
	fileTypeData             = "data"
	fileTypeEqualityDelete   = "equalityDelete"
	fileTypePositionalDelete = "positionalDelete"
	targetDataFileSize       = int64(512 * 1024 * 1024) // 512 MB
	targetDeleteFileSize     = int64(64 * 1024 * 1024)  // 64 MB

	// targetRowGroupBytes is the uncompressed in-memory size at which we
	// roll the current Parquet row group inside the same file. Matches
	// Apache Iceberg's PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT (128 MB) so
	// query engines get effective row-group level stats / bloom-filter /
	// page-index pruning and per-row-group parallelism.
	// https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L128
	targetRowGroupBytes = int64(128 * 1024 * 1024)

	// maxRowGroupRows is a row-count safety cap that matches Iceberg's
	// PARQUET_ROW_GROUP_LIMIT_DEFAULT. pqarrow.FileWriter.WriteBuffered
	// will automatically split a row group when its accumulated row count
	// would exceed this threshold; our byte-driven check above almost
	// always trips first, but this guards against pathologically narrow
	// rows where 128 MB would otherwise mean tens of millions of rows.
	maxRowGroupRows = int64(1_048_576)
)

func getDefaultWriterProps() []parquet.WriterProperty {
	return []parquet.WriterProperty{
		// Apache Iceberg sets its default compression to 'zstd'
		// https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L147
		parquet.WithCompression(compress.Codecs.Zstd),

		// Apache Iceberg default compression level is "null", and thus doesn't set any compression level config (https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L152)
		// ultimately, falls back to the underlying Parquet-Java library's default which is compression level 3 (https://github.com/apache/parquet-java/blob/dfc025e17e21a326addaf0e43c493e085cbac8f4/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/codec/ZstandardCodec.java#L52)
		parquet.WithCompressionLevel(3),

		// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L130-L133
		parquet.WithDataPageSize(1 * 1024 * 1024),

		// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L139-L142
		parquet.WithDictionaryPageSizeLimit(2 * 1024 * 1024),

		// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L612
		parquet.WithDictionaryDefault(true),

		// Apache Iceberg page row count limit: https://github.com/apache/iceberg/blob/68e555b94f4706a2af41dcb561c84007230c0bc1/core/src/main/java/org/apache/iceberg/TableProperties.java#L137
		parquet.WithBatchSize(int64(20000)),

		// Apache Iceberg relies on Parquet-Java: https://github.com/apache/parquet-java/blob/dfc025e17e21a326addaf0e43c493e085cbac8f4/parquet-column/src/main/java/org/apache/parquet/column/ParquetProperties.java#L67
		parquet.WithStats(true),

		// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L171
		parquet.WithVersion(parquet.V1_0),

		// iceberg writes root name as "table" in parquet's meta
		parquet.WithRootName("table"),

		// Row-count safety cap so pqarrow.FileWriter.WriteBuffered will
		// split the in-flight row group if our byte-driven roll has not
		// already triggered. Matches Apache Iceberg's
		// PARQUET_ROW_GROUP_LIMIT_DEFAULT.
		parquet.WithMaxRowGroupLength(maxRowGroupRows),
	}
}

// parquetWriter is a thin wrapper around pqarrow.FileWriter that adds
// byte-driven row-group rolling (Apache Iceberg semantics). One file
// must contain multiple row groups: this is what lets query engines
// prune row groups via min/max stats, bloom filters and page indexes,
// and lets Spark/Trino/DuckDB parallelise reads inside a single file.
//
// The previous implementation drove file.Writer + BufferedRowGroupWriter
// + pqarrow.WriteArrowToColumn by hand and never rolled a new row group
// within a file, so every parquet file produced by OLake ended up with
// exactly one ~512 MB row group. That destroyed every form of
// row-group-level pruning and was the dominant cause of slow downstream
// queries.
type parquetWriter struct {
	wr     *pqarrow.FileWriter
	schema *arrow.Schema
	closed bool
}

func newParquetWriter(ctx context.Context, arrSchema *arrow.Schema, w io.Writer, writerOpts []parquet.WriterProperty, kvMeta metadata.KeyValueMetadata) (*parquetWriter, error) {
	_ = ctx // pqarrow.NewFileWriter manages its own write context internally.

	props := parquet.NewWriterProperties(writerOpts...)
	arrProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithAllocator(memory.NewGoAllocator()),
	)

	fw, err := pqarrow.NewFileWriter(arrSchema, w, props, arrProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create pqarrow file writer: %s", err)
	}

	// Append the iceberg-specific KV metadata ("iceberg.schema",
	// "delete-type", "delete-field-ids", ...) onto the parquet footer so
	// query engines see the same metadata iceberg-java would have written.
	for i := 0; i < kvMeta.Len(); i++ {
		if err := fw.AppendKeyValueMetadata(kvMeta.Keys()[i], kvMeta.Values()[i]); err != nil {
			_ = fw.Close()
			return nil, fmt.Errorf("failed to append KV metadata %q: %s", kvMeta.Keys()[i], err)
		}
	}

	return &parquetWriter{wr: fw, schema: arrSchema}, nil
}

// WriteBuffered appends a record to the current in-memory row group and
// rolls a new buffered row group once the accumulated uncompressed size
// hits targetRowGroupBytes (128 MB), matching Apache Iceberg.
//
// The row-count safety cap (maxRowGroupRows) is enforced automatically
// by pqarrow.FileWriter.WriteBuffered via parquet.WithMaxRowGroupLength.
func (fw *parquetWriter) WriteBuffered(rec arrow.Record) error {
	if err := fw.wr.WriteBuffered(rec); err != nil {
		return fmt.Errorf("failed to write record batch: %s", err)
	}

	// Byte-driven row-group rolling. fw.wr.RowGroupTotalBytesWritten()
	// returns the uncompressed bytes accumulated in the current buffered
	// row group (sum of per-column writer totals), which is the same
	// quantity iceberg-java compares against PARQUET_ROW_GROUP_SIZE_BYTES
	// in ParquetWriter.checkSize(). When we cross the threshold we close
	// the buffered row group (it gets compressed and flushed to the sink)
	// and start a fresh one in the same file.
	if fw.wr.RowGroupTotalBytesWritten() >= targetRowGroupBytes {
		fw.wr.NewBufferedRowGroup()
	}
	return nil
}

func (fw *parquetWriter) Close() error {
	if fw.closed {
		return nil
	}
	fw.closed = true
	return fw.wr.Close()
}

// RowGroupTotalBytesWritten returns the uncompressed bytes accumulated
// in the current open buffered row group (0 if no row group is open).
func (fw *parquetWriter) RowGroupTotalBytesWritten() int64 {
	return fw.wr.RowGroupTotalBytesWritten()
}

// FileMetadata returns the parquet file metadata snapshot. Only valid after Close().
// Used by the iceberg-go backend to build iceberg.DataFile descriptors with full
// column statistics via table.DataFileFromParquetMetadata.
func (fw *parquetWriter) FileMetadata() (*metadata.FileMetaData, error) {
	return fw.wr.FileMetadata()
}

func toArrowType(icebergType string) arrow.DataType {
	switch icebergType {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "int":
		return arrow.PrimitiveTypes.Int32
	case "long":
		return arrow.PrimitiveTypes.Int64
	case "float":
		return arrow.PrimitiveTypes.Float32
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "timestamptz":
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.BinaryTypes.String
	}
}

func parseFieldIDsFromIcebergSchema(schemaJSON string) (map[string]int32, error) {
	var schema struct {
		Fields []struct {
			ID   int32  `json:"id"`
			Name string `json:"name"`
		} `json:"fields"`
	}

	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return nil, fmt.Errorf("failed to parse Iceberg schema JSON: %s", err)
	}

	fieldIDs := make(map[string]int32)
	for _, field := range schema.Fields {
		fieldIDs[field.Name] = field.ID
	}

	return fieldIDs, nil
}

func createFields(schema map[string]string, fieldIDs map[string]int32) []arrow.Field {
	fieldNames := make([]string, 0, len(schema))
	for fieldName := range schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		arrowType := toArrowType(schema[fieldName])
		// OlakeID is used as identifier field, so cannot be nullable
		nullable := fieldName != constants.OlakeID

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     arrowType,
			Nullable: nullable,
			// Add PARQUET:field_id metadata for Iceberg Query Engines compatibility
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", fieldIDs[fieldName]),
			}),
		})
	}

	return fields
}

func createDeleteArrowRecord(records []string, allocator memory.Allocator, schema *arrow.Schema) arrow.Record {
	recordBuilder := array.NewRecordBuilder(allocator, schema)
	defer recordBuilder.Release()

	for _, rec := range records {
		recordBuilder.Field(0).(*array.StringBuilder).Append(rec)
	}

	return recordBuilder.NewRecord()
}

func createPositionalDeleteArrowRecord(posDeletes []PositionalDelete, allocator memory.Allocator, schema *arrow.Schema) arrow.Record {
	recordBuilder := array.NewRecordBuilder(allocator, schema)
	defer recordBuilder.Release()

	for _, del := range posDeletes {
		recordBuilder.Field(0).(*array.StringBuilder).Append(del.FilePath)
		recordBuilder.Field(1).(*array.Int64Builder).Append(del.Position)
	}

	return recordBuilder.NewRecord()
}

func createArrowRecord(records []types.RawRecord, allocator memory.Allocator, schema *arrow.Schema) (arrow.Record, error) {
	recordBuilder := array.NewRecordBuilder(allocator, schema)
	defer recordBuilder.Release()
	for _, record := range records {
		for idx, field := range schema.Fields() {
			var val any
			if olakeVal, exists := record.OlakeColumns[field.Name]; exists {
				// OLake system columns (_olake_id, _olake_timestamp, _op_type, _cdc_timestamp and driver specific cdc columns)
				val = olakeVal
			} else {
				// FlattenAndCleanData pre-shapes record.Data for both modes:
				//   normalization=true:  typed columns + OlakeColumns merged in
				//   normalization=false: StringifiedData + OlakeColumns + partition columns
				// record.Data[field.Name] covers all remaining fields in both cases.
				val = record.Data[field.Name]
			}

			if val == nil {
				recordBuilder.Field(idx).AppendNull()
			} else {
				if err := appendValueToBuilder(recordBuilder.Field(idx), val); err != nil {
					return nil, fmt.Errorf("cannot identify value for the col %s: %s", field.Name, err)
				}
			}
		}
	}

	arrowRecord := recordBuilder.NewRecord()

	return arrowRecord, nil
}

func appendValueToBuilder(builder array.Builder, val interface{}) error {
	switch builder := builder.(type) {
	case *array.BooleanBuilder:
		if boolVal, err := typeutils.ReformatBool(val); err == nil {
			builder.Append(boolVal)
		} else {
			return err
		}
	case *array.Int32Builder:
		if intVal, err := typeutils.ReformatInt32(val); err == nil {
			builder.Append(intVal)
		} else {
			return err
		}
	case *array.Int64Builder:
		if longVal, err := typeutils.ReformatInt64(val); err == nil {
			builder.Append(longVal)
		} else {
			return err
		}
	case *array.Float32Builder:
		if floatVal, err := typeutils.ReformatFloat32(val); err == nil {
			builder.Append(floatVal)
		} else {
			return err
		}
	case *array.Float64Builder:
		if doubleVal, err := typeutils.ReformatFloat64(val); err == nil {
			builder.Append(doubleVal)
		} else {
			return err
		}
	case *array.TimestampBuilder:
		if timeVal, err := typeutils.ReformatDate(val, true); err == nil {
			ts := arrow.Timestamp(timeVal.UnixMicro())
			builder.Append(ts)
		} else {
			return err
		}
	case *array.StringBuilder:
		// OLake converts the data column to json format for a denormalized table
		if mapVal, ok := val.(map[string]interface{}); ok {
			jsonBytes, err := json.Marshal(mapVal)
			if err != nil {
				return fmt.Errorf("failed to marshal map to JSON: %s", err)
			}
			builder.Append(string(jsonBytes))
		} else {
			builder.Append(fmt.Sprintf("%v", val))
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

// NOTE: the previous hand-rolled arrowToParquetSchema / arrowFieldsToParquet
// helpers have been removed. pqarrow.NewFileWriter now derives the Parquet
// schema from the Arrow schema via pqarrow.ToParquet, which honours the
// "PARQUET:field_id" metadata key (see fieldIDFromMeta in arrow-go) and
// produces the same logical types (String/Timestamp/...) we used to set
// by hand. The root group name comes from parquet.WithRootName("table").
