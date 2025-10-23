package arrow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func CreateNormFields(schema map[string]string) []arrow.Field {
	fieldNames := make([]string, 0, len(schema)+1)
	for fieldName := range schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		var arrowType arrow.DataType

		icebergType := schema[fieldName]
		switch icebergType {
		case "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
		case "int":
			arrowType = arrow.PrimitiveTypes.Int32
		case "long":
			arrowType = arrow.PrimitiveTypes.Int64
		case "float":
			arrowType = arrow.PrimitiveTypes.Float32
		case "double":
			arrowType = arrow.PrimitiveTypes.Float64
		case "timestamptz":
			arrowType = arrow.FixedWidthTypes.Timestamp_us
		default:
			arrowType = arrow.BinaryTypes.String
		}

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     arrowType,
			Nullable: true,
		})
	}

	return fields
}

func CreateDeNormFields() []arrow.Field {
	fields := make([]arrow.Field, 0, 5)

	fields = append(fields, arrow.Field{Name: "_olake_id", Type: arrow.BinaryTypes.String, Nullable: false})
	fields = append(fields, arrow.Field{Name: "_olake_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false})
	fields = append(fields, arrow.Field{Name: "_op_type", Type: arrow.BinaryTypes.String, Nullable: false})
	fields = append(fields, arrow.Field{Name: "_cdc_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true})
	fields = append(fields, arrow.Field{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true})

	return fields
}

func CreateDeleteFiles(records []types.RawRecord, fieldId int) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	fields := make([]arrow.Field, 0, 1)
	fields = append(fields, arrow.Field{
		Name:     "_olake_id",
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

func CreateArrowRecordWithFields(records []types.RawRecord, fields []arrow.Field, normalization bool) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	if !normalization {
		fields = CreateDeNormFields()
	}

	allocator := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema(fields, nil)
	recordBuilder := array.NewRecordBuilder(allocator, arrowSchema)
	defer recordBuilder.Release()

	for _, record := range records {
		for idx, field := range arrowSchema.Fields() {
			var val any
			switch field.Name {
			case "_olake_id":
				val = record.OlakeID
			case "_olake_timestamp":
				val = record.OlakeTimestamp
			case "_op_type":
				val = record.OperationType
			case "_cdc_timestamp":
				val = record.CdcTimestamp
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
				if err := AppendValueToBuilder(recordBuilder.Field(idx), val, field.Type); err != nil {
					logger.Warnf("failed to append value for field %s: %v", field.Name, err)
					recordBuilder.Field(idx).AppendNull()
				}
			}
		}
	}

	arrowRecord := recordBuilder.NewRecord()

	return arrowRecord, nil
}

func AppendValueToBuilder(builder array.Builder, val interface{}, fieldType arrow.DataType) error {
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
		builder.Append(fmt.Sprintf("%v", val))
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}
