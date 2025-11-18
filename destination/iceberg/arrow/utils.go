package arrow

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
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

func CreateNormFields(schema map[string]string, fieldIds map[string]int) []arrow.Field {
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
				"PARQUET:field_id": fmt.Sprintf("%d", fieldIds[fieldName]),
			}),
		})
	}

	return fields
}

func CreateDeNormFields(fieldIds map[string]int) []arrow.Field {
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

	fieldnames := make([]string, 0, len(fieldIds))
	for fieldName := range fieldIds {
		fieldnames = append(fieldnames, fieldName)
	}

	sort.Strings(fieldnames)

	fields := make([]arrow.Field, 0, len(fieldIds))
	for _, fieldName := range fieldnames {
		arrowType, nullable := getFieldType(fieldName)

		fields = append(fields, arrow.Field{
			Name: fieldName,
			Type: arrowType,
			Nullable: nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", fieldIds[fieldName]),
			}),
		})
	}

	return fields
}

func CreateDelArrowRec(records []types.RawRecord, fieldId int) (arrow.Record, error) {
	// need to check the metadata requirement here as well
	fields := make([]arrow.Field, 0, 1)
	fields = append(fields, arrow.Field{
		Name:     constants.OlakeID,
		Type:     arrow.BinaryTypes.String,
		Nullable: false,
		Metadata: arrow.MetadataFrom(map[string]string{
			"PARQUET:field_id": fmt.Sprintf("%d", fieldId),
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
				if err := AppendValueToBuilder(recordBuilder.Field(idx), val, field.Type, field.Name, normalization); err != nil {
					return nil, fmt.Errorf("cannot identify value for the col %v", field.Name)
				}
			}
		}
	}

	arrowRecord := recordBuilder.NewRecord()

	return arrowRecord, nil
}

func AppendValueToBuilder(builder array.Builder, val interface{}, fieldType arrow.DataType, fieldName string, normalization bool) error {
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

// PartitionInfo represents an Iceberg partition column with its transform, preserving order.
type PartitionInfo struct {
	Field     string
	Transform string
}

// In OLake, for equality deletes, we use OlakeID as the id
// we create equality delete files for:
// "d" : delete operation
// "u" : update operation
// "c" : insert operation
func ExtractDeleteRecords(records []types.RawRecord) []types.RawRecord {
	deletes := make([]types.RawRecord, 0, len(records))
	for _, rec := range records {
		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			deletes = append(deletes, types.RawRecord{OlakeID: rec.OlakeID})
		}
	}

	return deletes
}

// OLake's arrow writer writes the iceberg schema as a metadata in every parquet file
func BuildIcebergSchemaJSON(schema map[string]string, fieldIds map[string]int, schemaId int) string {
	fieldNames := make([]string, 0, len(schema))
	for fieldName := range schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fieldsJSON := ""
	for i, fieldName := range fieldNames {
		icebergType := schema[fieldName]
		fieldId := fieldIds[fieldName]

		typeStr := fmt.Sprintf(`"%s"`, icebergType)

		// OLakeID is a REQUIRED field, cannot be NULL
		required := fieldName == constants.OlakeID

		if i > 0 {
			fieldsJSON += ","
		}
		fieldsJSON += fmt.Sprintf(`{"id":%d,"name":"%s","required":%t,"type":%s}`,
			fieldId, fieldName, required, typeStr)
	}

	return fmt.Sprintf(`{"type":"struct","schema-id":%d,"fields":[%s]}`, schemaId, fieldsJSON)
}
