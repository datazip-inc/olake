package types

import (
	"fmt"
	"sort"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/goccy/go-json"
	"github.com/parquet-go/parquet-go"
)

type DataType string

const (
	Null           DataType = "null"
	Int32          DataType = "integer_small"
	Int64          DataType = "integer"
	Float32        DataType = "number_small"
	Float64        DataType = "number"
	String         DataType = "string"
	Bool           DataType = "boolean"
	Object         DataType = "object"
	Array          DataType = "array"
	Unknown        DataType = "unknown"
	Timestamp      DataType = "timestamp"
	TimestampMilli DataType = "timestamp_milli" // storing datetime up to 3 precisions
	TimestampMicro DataType = "timestamp_micro" // storing datetime up to 6 precisions
	TimestampNano  DataType = "timestamp_nano"  // storing datetime up to 9 precisions
)

type Record map[string]any

type RawRecord struct {
	Data           map[string]any `parquet:"data,json"`
	OlakeID        string         `parquet:"_olake_id"`
	OlakeTimestamp time.Time      `parquet:"_olake_timestamp"`
	OperationType  string         `parquet:"_op_type"` // "r" for read/backfill, "c" for create, "u" for update, "d" for delete
	CdcTimestamp   time.Time      `parquet:"_cdc_timestamp"`
}

func CreateRawRecord(olakeID string, data map[string]any, operationType string, cdcTimestamp time.Time) RawRecord {
	return RawRecord{
		OlakeID:       olakeID,
		Data:          data,
		OperationType: operationType,
		CdcTimestamp:  cdcTimestamp,
	}
}

func (r *RawRecord) ToDebeziumFormat(db string, stream string, normalization bool) (string, error) {
	// First create the schema and track field types
	schema := r.createDebeziumSchema(db, stream, normalization)

	// Create the payload with the actual data
	payload := make(map[string]interface{})

	// Add olake_id to payload
	payload[constants.OlakeID] = r.OlakeID

	// Handle data based on normalization flag
	if normalization {
		for key, value := range r.Data {
			payload[key] = value
		}
	} else {
		dataBytes, err := json.Marshal(r.Data)
		if err != nil {
			return "", err
		}
		payload["data"] = string(dataBytes)
	}

	// Add the metadata fields
	payload[constants.OpType] = r.OperationType // "r" for read/backfill, "c" for create, "u" for update
	payload[constants.DBName] = db
	payload[constants.CdcTimestamp] = r.CdcTimestamp
	payload[constants.OlakeTimestamp] = r.OlakeTimestamp

	// Create Debezium format
	debeziumRecord := map[string]interface{}{
		"destination_table": stream,
		"key": map[string]interface{}{
			"schema": map[string]interface{}{
				"type": "struct",
				"fields": []map[string]interface{}{
					{
						"type":     "string",
						"optional": true,
						"field":    constants.OlakeID,
					},
				},
				"optional": false,
			},
			"payload": map[string]interface{}{
				constants.OlakeID: r.OlakeID,
			},
		},
		"value": map[string]interface{}{
			"schema":  schema,
			"payload": payload,
		},
	}

	jsonBytes, err := json.Marshal(debeziumRecord)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func (r *RawRecord) createDebeziumSchema(db string, stream string, normalization bool) map[string]interface{} {
	fields := make([]map[string]interface{}, 0)

	// Add olake_id field first
	fields = append(fields, map[string]interface{}{
		"type":     "string",
		"optional": true,
		"field":    constants.OlakeID,
	})

	if normalization {
		// Collect data fields for sorting
		dataFields := make([]map[string]interface{}, 0, len(r.Data))

		// Add individual data fields
		for key, value := range r.Data {
			field := map[string]interface{}{
				"optional": true,
				"field":    key,
			}

			switch value.(type) {
			case bool:
				field["type"] = "boolean"
			case int, int8, int16, int32:
				field["type"] = "int32"
			case int64:
				field["type"] = "int64"
			case float32:
				field["type"] = "float32"
			case float64:
				field["type"] = "float64"
			case time.Time:
				field["type"] = "timestamptz" // use with timezone as we use default utc
			default:
				field["type"] = "string"
			}

			dataFields = append(dataFields, field)
		}

		// Sorting basis on field names is needed because
		// Iceberg writer detects different schemas for
		// schema evolution based on order columns passed
		sort.Slice(dataFields, func(i, j int) bool {
			return dataFields[i]["field"].(string) < dataFields[j]["field"].(string)
		})

		fields = append(fields, dataFields...)
	} else {
		// For non-normalized mode, add a single data field as string
		fields = append(fields, map[string]interface{}{
			"type":     "string",
			"optional": true,
			"field":    "data",
		})
	}

	// Add metadata fields
	fields = append(fields, []map[string]interface{}{
		{
			"type":     "string",
			"optional": true,
			"field":    constants.OpType,
		},
		{
			"type":     "string",
			"optional": true,
			"field":    constants.DBName,
		},
		{
			"type":     "timestamptz",
			"optional": true,
			"field":    constants.CdcTimestamp,
		},
		{
			"type":     "timestamptz",
			"optional": true,
			"field":    constants.OlakeTimestamp,
		},
	}...)

	return map[string]interface{}{
		"type":     "struct",
		"fields":   fields,
		"optional": false,
		"name":     fmt.Sprintf("%s.%s", db, stream),
	}
}

func (d DataType) ToNewParquet() parquet.Node {
	var n parquet.Node

	switch d {
	case Int32:
		n = parquet.Leaf(parquet.Int32Type)
	case Float32:
		n = parquet.Leaf(parquet.FloatType)
	case Int64:
		n = parquet.Leaf(parquet.Int64Type)
	case Float64:
		n = parquet.Leaf(parquet.DoubleType)
	case String:
		n = parquet.String()
	case Bool:
		n = parquet.Leaf(parquet.BooleanType)
	case Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		n = parquet.Timestamp(parquet.Microsecond)
	case Object, Array:
		// Ensure proper handling of nested structures
		n = parquet.String()
	default:
		n = parquet.Leaf(parquet.ByteArrayType)
	}

	n = parquet.Optional(n) // Ensure the field is nullable
	return n
}
