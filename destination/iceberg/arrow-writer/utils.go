package arrowwriter

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
)

var (
	// Apache Iceberg sets its default compression to 'zstd'
	// https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L147
	DefaultCompression = parquet.WithCompression(compress.Codecs.Zstd)

	// Apache Iceberg default compression level is "null", and thus doesn't set any compression level config (https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L152)
	// thus, falls back to the underlying Parquet-Java library's default which is compression level 3 (https://github.com/apache/parquet-java/blob/dfc025e17e21a326addaf0e43c493e085cbac8f4/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/codec/ZstandardCodec.java#L52)
	DefaultCompressionLevel = parquet.WithCompressionLevel(3)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L130-L133
	DefaultDataPageSize = parquet.WithDataPageSize(1 * 1024 * 1024)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L139-L142
	DefaultDictionaryPageSizeLimit = parquet.WithDictionaryPageSizeLimit(2 * 1024 * 1024)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L612
	DefaultDictionaryEncoding = parquet.WithDictionaryDefault(true)

	// Apache Iceberg page row count limit: https://github.com/apache/iceberg/blob/68e555b94f4706a2af41dcb561c84007230c0bc1/core/src/main/java/org/apache/iceberg/TableProperties.java#L137
	DefaultBatchSize = parquet.WithBatchSize(int64(20000))

	// Apache Iceberg relies on Parquet-Java: https://github.com/apache/parquet-java/blob/dfc025e17e21a326addaf0e43c493e085cbac8f4/parquet-column/src/main/java/org/apache/parquet/column/ParquetProperties.java#L67
	DefaultStatsEnabled = parquet.WithStats(true)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L171
	DefaultParquetVersion = parquet.WithVersion(parquet.V1_0)

	// iceberg writes root name as "table" in parquet's meta
	DefaultRootName = parquet.WithRootName("table")
)

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

func CreateNormFields(schema map[string]string, fieldIDs map[string]int32) []arrow.Field {
	fieldNames := make([]string, 0, len(schema))
	for fieldName := range schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		arrowType := toArrowType(schema[fieldName])

		nullable := fieldName != constants.OlakeID

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     arrowType,
			Nullable: nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", fieldIDs[fieldName]),
			}),
		})
	}

	return fields
}

func CreateDeNormFields(fieldIDs map[string]int32) []arrow.Field {
	getFieldType := func(name string) (arrow.DataType, bool) {
		switch name {
		case constants.OlakeID, constants.OpType:
			return arrow.BinaryTypes.String, name != constants.OlakeID
		case constants.OlakeTimestamp, constants.CdcTimestamp:
			return arrow.FixedWidthTypes.Timestamp_us, name == constants.CdcTimestamp
		case constants.StringifiedData:
			return arrow.BinaryTypes.String, true
		default:
			return arrow.BinaryTypes.String, true
		}
	}

	fieldnames := make([]string, 0, len(fieldIDs))
	for fieldName := range fieldIDs {
		fieldnames = append(fieldnames, fieldName)
	}

	sort.Strings(fieldnames)

	fields := make([]arrow.Field, 0, len(fieldIDs))
	for _, fieldName := range fieldnames {
		arrowType, nullable := getFieldType(fieldName)

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     arrowType,
			Nullable: nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", fieldIDs[fieldName]),
			}),
		})
	}

	return fields
}

// In OLake, for equality deletes, we use OlakeID as the id
// we create equality delete files for:
// "d" : delete operation
// "u" : update operation
// "c" : insert operation
func extractDeleteRecord(rec types.RawRecord) types.RawRecord {
	if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
		return types.RawRecord{OlakeID: rec.OlakeID}
	}

	return rec
}

func createDeleteArrowRec(records []types.RawRecord, fieldID int32) (arrow.Record, error) {
	// need to check the metadata requirement here as well
	fields := make([]arrow.Field, 0, 1)
	fields = append(fields, arrow.Field{
		Name:     constants.OlakeID,
		Type:     arrow.BinaryTypes.String,
		Nullable: false,
		Metadata: arrow.MetadataFrom(map[string]string{
			"PARQUET:field_id": fmt.Sprintf("%d", fieldID),
		}),
	})

	allocator := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema(fields, nil)
	recordBuilder := array.NewRecordBuilder(allocator, arrowSchema)

	defer recordBuilder.Release()

	for _, rec := range records {
		recordBuilder.Field(0).(*array.StringBuilder).Append(rec.OlakeID)
	}

	arrRec := recordBuilder.NewRecord()

	return arrRec, nil
}

func CreateArrowRecord(records []types.RawRecord, fields []arrow.Field, normalization bool) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	allocator := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema(fields, nil)
	recordBuilder := array.NewRecordBuilder(allocator, arrowSchema)
	defer recordBuilder.Release()

	for _, record := range records {
		for idx, field := range arrowSchema.Fields() {
			var val any
			switch field.Name {
			case constants.OlakeID:
				val = record.OlakeID
			case constants.OlakeTimestamp:
				val = record.OlakeTimestamp
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
				if err := AppendValueToBuilder(recordBuilder.Field(idx), val, field.Name, normalization); err != nil {
					return nil, fmt.Errorf("cannot identify value for the col %v: %v", field.Name, err)
				}
			}
		}
	}

	arrowRecord := recordBuilder.NewRecord()

	return arrowRecord, nil
}

func AppendValueToBuilder(builder array.Builder, val interface{}, fieldName string, normalization bool) error {
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
		if timeVal, err := typeutils.ReformatDate(val); err == nil {
			ts := arrow.Timestamp(timeVal.UnixMicro())
			builder.Append(ts)
		} else {
			return err
		}
	case *array.StringBuilder:
		// OLake converts the data column to json format for a denormalized table
		if mapVal, ok := val.(map[string]interface{}); !normalization && fieldName == constants.StringifiedData && ok {
			jsonBytes, err := json.Marshal(mapVal)
			if err != nil {
				return fmt.Errorf("failed to marshal map to JSON: %w", err)
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
