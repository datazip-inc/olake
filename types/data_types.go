package types

import (
	"fmt"
	"time"

	// "time"

	"github.com/goccy/go-json"
	"github.com/parquet-go/parquet-go"
)

type DataType string

const (
	Null           DataType = "null"
	Int64          DataType = "integer"
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

// TODO: change Olake column names to _ prefixed.
type RawRecord struct {
	Data           map[string]any `parquet:"data,json"`
	OlakeID        string         `parquet:"_olake_id"`
	OlakeTimestamp int64          `parquet:"_olake_insert_time"`
	OperationType  string         `parquet:"_op_type"` // "r" for read/backfill, "c" for create, "u" for update, "d" for delete
	CdcTimestamp   int64          `parquet:"_cdc_timestamp"`
}

func CreateRawRecord(olakeID string, data map[string]any, operationType string, cdcTimestamp int64) RawRecord {
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
	payload["olake_id"] = r.OlakeID

	// Handle data based on normalization flag
	if normalization {
		for key, value := range r.Data {
			if key != "olake_id" {
				// Convert time.Time values to UTC
				if timeVal, isTime := value.(time.Time); isTime {
					payload[key] = timeVal.UTC().Format(time.RFC3339Nano)
				} else {
					payload[key] = value
				}
			}
		}
	} else {
		processedData := make(map[string]interface{})
		for k, v := range r.Data {
			if timeVal, isTime := v.(time.Time); isTime {
				processedData[k] = timeVal.UTC().Format(time.RFC3339Nano)
			} else {
				processedData[k] = v
			}
		}

		dataBytes, err := json.Marshal(processedData)
		if err != nil {
			return "", err
		}
		payload["data"] = string(dataBytes)
	}

	// Add the metadata fields
	payload["__deleted"] = r.OperationType == "delete"
	payload["__op"] = r.OperationType // "r" for read/backfill, "c" for create, "u" for update
	payload["__db"] = db
	payload["__source_ts_ms"] = r.CdcTimestamp

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
						"field":    "olake_id",
					},
				},
				"optional": false,
			},
			"payload": map[string]interface{}{
				"olake_id": r.OlakeID,
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
		"field":    "olake_id",
	})

	if normalization {
		// Add individual data fields
		for key, value := range r.Data {
			// Skip olake_id for normalized mode
			if key == "olake_id" {
				continue
			}

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

			fields = append(fields, field)
		}
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
			"type":     "boolean",
			"optional": true,
			"field":    "__deleted",
		},
		{
			"type":     "string",
			"optional": true,
			"field":    "__op",
		},
		{
			"type":     "string",
			"optional": true,
			"field":    "__db",
		},
		{
			"type":     "int64",
			"optional": true,
			"field":    "__source_ts_ms",
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
	case Int64:
		n = parquet.Leaf(parquet.Int64Type)
	case Float64:
		n = parquet.Leaf(parquet.DoubleType)
	case String:
		n = parquet.String()
	case Bool:
		n = parquet.Leaf(parquet.BooleanType)
	case Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		n = parquet.Leaf(parquet.Int64Type)
		n = parquet.Encoded(n, &parquet.Plain)
	case Object, Array:
		// Ensure proper handling of nested structures
		n = parquet.String()
	default:
		n = parquet.Leaf(parquet.ByteArrayType)
	}

	n = parquet.Optional(n) // Ensure the field is nullable
	return n
}
