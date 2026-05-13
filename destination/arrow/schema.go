// File: schema.go — OLakeSchema → arrow.Schema conversion.

package arrowdst

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

// ToArrowField builds a single arrow.Field from an OLake DataType. When
// fieldID >= 0 the PARQUET:field_id metadata is attached (required by Iceberg).
func ToArrowField(name string, t types.DataType, nullable bool, fieldID int32) arrow.Field {
	f := arrow.Field{Name: name, Type: olakeTypeToArrow(t), Nullable: nullable}
	if fieldID >= 0 {
		f.Metadata = arrow.NewMetadata(
			[]string{"PARQUET:field_id"},
			[]string{strconv.Itoa(int(fieldID))},
		)
	}
	return f
}

// ToArrowSchema builds an *arrow.Schema from an OLakeSchema.
//   - fieldIDs: per-column field ID metadata (Iceberg). nil/empty -> none.
//   - identifierField: column forced non-null (e.g. "_olake_id"). "" -> all nullable.
func ToArrowSchema(s OLakeSchema, fieldIDs map[string]int32, identifierField string) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(s))
	for name, t := range s {
		nullable := name != identifierField
		id := int32(-1)
		if v, ok := fieldIDs[name]; ok {
			id = v
		}
		fields = append(fields, ToArrowField(name, t, nullable, id))
	}
	return arrow.NewSchema(fields, nil)
}

func olakeTypeToArrow(t types.DataType) arrow.DataType {
	switch t {
	case types.Bool:
		return arrow.FixedWidthTypes.Boolean
	case types.Int32:
		return arrow.PrimitiveTypes.Int32
	case types.Int64:
		return arrow.PrimitiveTypes.Int64
	case types.Float32:
		return arrow.PrimitiveTypes.Float32
	case types.Float64:
		return arrow.PrimitiveTypes.Float64
	case types.Timestamp, types.TimestampMilli:
		return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
	case types.TimestampMicro:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case types.TimestampNano:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	default:
		return arrow.BinaryTypes.String
	}
}

// SystemFieldIsIdentifier reports whether name is the canonical identifier
// column used by Iceberg upserts.
func SystemFieldIsIdentifier(name string) bool {
	return name == constants.OlakeID
}
