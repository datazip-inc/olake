package arrowwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
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
	}
}

// parquet writer for arrow
type parquetWriter struct {
	wr     *file.Writer
	schema *arrow.Schema
	rgw    file.BufferedRowGroupWriter
	ctx    context.Context
	closed bool
}

func newParquetWriter(arrSchema *arrow.Schema, w io.Writer, writerOpts []parquet.WriterProperty, kvMeta metadata.KeyValueMetadata) (*parquetWriter, error) {
	props := parquet.NewWriterProperties(writerOpts...)
	pqSchema, err := arrowToParquetSchema(arrSchema)
	if err != nil {
		return nil, err
	}

	baseWriter := file.NewParquetWriter(w, pqSchema.Root(),
		file.WithWriterProps(props),
		file.WithWriteMetadata(kvMeta))

	return &parquetWriter{
		wr:     baseWriter,
		schema: arrSchema,
		ctx:    pqarrow.NewArrowWriteContext(context.Background(), nil),
	}, nil
}

func (fw *parquetWriter) newBufferedRowGroup() {
	if fw.rgw != nil {
		fw.rgw.Close()
	}
	fw.rgw = fw.wr.AppendBufferedRowGroup()
}

func (fw *parquetWriter) WriteBuffered(rec arrow.Record) error {
	if fw.rgw == nil {
		fw.newBufferedRowGroup()
	}

	numRows := int(rec.NumRows())
	for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
		col := rec.Column(colIdx)
		field := fw.schema.Field(colIdx)

		cw, err := fw.rgw.Column(colIdx)
		if err != nil {
			return fmt.Errorf("failed to get column writer %d: %w", colIdx, err)
		}

		// for flat schemas, generating definition levels for nullable columns
		var defLevels []int16
		if field.Nullable {
			defLevels = make([]int16, numRows)
			for i := 0; i < numRows; i++ {
				if col.IsNull(i) {
					defLevels[i] = 0
				} else {
					defLevels[i] = 1
				}
			}
		}

		if err := pqarrow.WriteArrowToColumn(fw.ctx, cw, col, defLevels, nil, field.Nullable); err != nil {
			return fmt.Errorf("failed to write column %d (%s): %w", colIdx, field.Name, err)
		}
	}

	return nil
}

func (fw *parquetWriter) Close() error {
	if !fw.closed {
		fw.closed = true
		if fw.rgw != nil {
			if err := fw.rgw.Close(); err != nil {
				return err
			}
		}
		return fw.wr.Close()
	}
	return nil
}

func (fw *parquetWriter) NumRows() int {
	if fw.wr != nil {
		return fw.wr.NumRows()
	}
	return 0
}

func (fw *parquetWriter) RowGroupTotalBytesWritten() int64 {
	if fw.rgw != nil {
		return fw.rgw.TotalBytesWritten()
	}
	return 0
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

func createDeleteArrowRecord(records []types.RawRecord, allocator memory.Allocator, schema *arrow.Schema) (arrow.Record, error) {
	recordBuilder := array.NewRecordBuilder(allocator, schema)
	defer recordBuilder.Release()

	for _, rec := range records {
		recordBuilder.Field(0).(*array.StringBuilder).Append(rec.OlakeID)
	}

	arrowRec := recordBuilder.NewRecord()

	return arrowRec, nil
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

func createArrowRecord(records []types.RawRecord, allocator memory.Allocator, schema *arrow.Schema, normalization bool, olakeTimestamp time.Time) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	recordBuilder := array.NewRecordBuilder(allocator, schema)
	defer recordBuilder.Release()

	for _, record := range records {
		for idx, field := range schema.Fields() {
			var val any
			switch field.Name {
			case constants.OlakeID:
				val = record.OlakeID
			case constants.OlakeTimestamp:
				val = olakeTimestamp
			case constants.OpType:
				val = record.OperationType
			case constants.CdcTimestamp:
				if record.CdcTimestamp != nil {
					val = record.CdcTimestamp
				}
			default:
				if normalization {
					val = record.Data[field.Name]
				} else {
					val = record.Data
				}
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

func arrowToParquetSchema(arrowSchema *arrow.Schema) (*schema.Schema, error) {
	nodes := make(schema.FieldList, 0, arrowSchema.NumFields())

	for _, field := range arrowSchema.Fields() {
		node, err := arrowFieldsToParquet(field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %s", field.Name, err)
		}
		nodes = append(nodes, node)
	}

	// creating root group node with name "table"
	root, err := schema.NewGroupNode("table", parquet.Repetitions.Required, nodes, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to create root group node: %s", err)
	}

	return schema.NewSchema(root), nil
}

// this is important as it does not sets LogicalTypes for types like INT32/INT64
// (eg, IntLogicalType for INT32/INT64)
func arrowFieldsToParquet(field arrow.Field) (schema.Node, error) {
	repetition := parquet.Repetitions.Required
	if field.Nullable {
		repetition = parquet.Repetitions.Optional
	}

	// extracting field ID from metadata
	fieldID := int32(-1)
	if field.Metadata.Len() > 0 {
		if idStr, ok := field.Metadata.GetValue("PARQUET:field_id"); ok {
			if id, err := strconv.ParseInt(idStr, 10, 32); err == nil {
				fieldID = int32(id)
			}
		}
	}

	var pqType parquet.Type
	var logicalType schema.LogicalType
	var typeLength int32 = -1

	switch field.Type.ID() {
	case arrow.BOOL:
		pqType = parquet.Types.Boolean

	case arrow.INT32:
		pqType = parquet.Types.Int32

	case arrow.INT64:
		pqType = parquet.Types.Int64

	case arrow.FLOAT32:
		pqType = parquet.Types.Float

	case arrow.FLOAT64:
		pqType = parquet.Types.Double

	case arrow.STRING:
		pqType = parquet.Types.ByteArray
		logicalType = schema.StringLogicalType{}

	case arrow.TIMESTAMP:
		pqType = parquet.Types.Int64
		tsType := field.Type.(*arrow.TimestampType)
		adjustedToUTC := tsType.TimeZone != ""
		logicalType = schema.NewTimestampLogicalType(adjustedToUTC, schema.TimeUnitMicros)

	default:
		// Default to string for any unsupported types
		pqType = parquet.Types.ByteArray
		logicalType = schema.StringLogicalType{}
	}

	if logicalType != nil {
		return schema.NewPrimitiveNodeLogical(field.Name, repetition, logicalType, pqType, int(typeLength), fieldID)
	}
	return schema.NewPrimitiveNode(field.Name, repetition, pqType, fieldID, typeLength)
}
