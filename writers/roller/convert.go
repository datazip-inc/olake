package roller

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// RawRecordConverter is the standard data-path Converter: it builds one arrow
// record from a chunk of RawRecords against the given schema, reading each
// column from OlakeColumns (system columns) or Data. Bind it once and hand it to
// NewRoller. The allocator is reused across chunks; a Roller writes sequentially
// so this is safe.
func RawRecordConverter(schema *arrow.Schema) Converter[*types.RawRecord] {
	allocator := memory.NewGoAllocator()
	return func(rows []*types.RawRecord) (arrow.Record, error) {
		return CreateArrowRecord(rows, allocator, schema)
	}
}

// CreateArrowRecord builds an arrow record from RawRecords against schema. Values
// are read from OlakeColumns first (system columns), then Data. Backends that
// need a different row shape (e.g. a stringified-data column) can pre-shape the
// records before calling this.
func CreateArrowRecord(records []*types.RawRecord, allocator memory.Allocator, schema *arrow.Schema) (arrow.Record, error) {
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

func appendValueToBuilder(builder array.Builder, val any) error {
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
		switch v := val.(type) {
		case string:
			builder.Append(v)
		case []byte:
			builder.Append(string(v))
		case map[string]any, []any:
			// composite values (objects/arrays) are stored as JSON text
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal value to JSON: %s", err)
			}
			builder.Append(string(jsonBytes))
		default:
			builder.Append(fmt.Sprintf("%v", val))
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}
