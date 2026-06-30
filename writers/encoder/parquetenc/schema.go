package parquetenc

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

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
		// A field tagged PARQUET:logical=JSON keeps the JSON logical type (matches
		// the legacy parquet writer's parquet.JSON() for the denormalized "data" column).
		if lt, ok := field.Metadata.GetValue("PARQUET:logical"); ok && lt == "JSON" {
			logicalType = schema.JSONLogicalType{}
		}

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
