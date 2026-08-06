package driver

import (
	"math"

	"github.com/datazip-inc/olake/types"
)

// Define a mapping of MySQL data types to internal data types
var mysqlTypeToDataTypes = map[string]types.DataType{
	// Integer types
	"tinyint":            types.Int32,
	"smallint":           types.Int32,
	"mediumint":          types.Int32,
	"int":                types.Int32,
	"integer":            types.Int32,
	"unsigned int":       types.Int64,
	"unsigned integer":   types.Int64,
	"unsigned smallint":  types.Int32,
	"unsigned tinyint":   types.Int32,
	"unsigned mediumint": types.Int32,
	"unsigned bigint":    types.Int64,
	"bit":                types.Int32,
	"bigint":             types.Int64,

	// Floating point types
	"float":  types.Float32,
	"real":   types.Float32,
	"double": types.Float64,

	// Can handle up to 15 significant digits accurately (e.g., DECIMAL(15,2) or DECIMAL(15,7))
	// Values with 16 digits may have minor rounding. Beyond 16 (from 17) digits will have precision loss.
	"numeric": types.Float64,
	"decimal": types.Float64,

	// String types
	"char":       types.String,
	"varchar":    types.String,
	"tinytext":   types.String,
	"text":       types.String,
	"mediumtext": types.String,
	"longtext":   types.String,

	// Binary types
	"binary":     types.String,
	"varbinary":  types.String,
	"tinyblob":   types.String,
	"blob":       types.String,
	"mediumblob": types.String,
	"longblob":   types.String,

	// Date and time types
	"date":      types.Timestamp,
	"timestamp": types.Timestamp,
	"datetime":  types.Timestamp,
	"year":      types.Int64,

	// time and datetime types treated as string for now
	"time": types.String,

	// JSON type
	"json": types.String,
	// Enum and Set types
	"enum": types.String,
	"set":  types.String,

	// Geometry types
	"geometry":           types.String,
	"point":              types.String,
	"linestring":         types.String,
	"polygon":            types.String,
	"multipoint":         types.String,
	"multilinestring":    types.String,
	"multipolygon":       types.String,
	"geometrycollection": types.String,
}

// MEDIUMINT's 3 bytes are the one MySQL integer width Go has no constant for.
const maxUint24 = 1<<24 - 1

// stripSignExtension masks an UNSIGNED column's value back to its storage width, undoing the sign
// extension the binlog parser applies. The result comes back in the narrowest signed Go type that
// holds the column's whole range, so no case widens further than its own values need.
// UNSIGNED BIGINT is absent on purpose: it has no spare width, so its bits are already final.
func stripSignExtension(value any, columnType string) any {
	switch columnType {
	case "unsigned tinyint":
		if v, ok := value.(int8); ok {
			return int16(v) & math.MaxUint8
		}
	case "unsigned smallint":
		if v, ok := value.(int16); ok {
			return int32(v) & math.MaxUint16
		}
	case "unsigned mediumint":
		if v, ok := value.(int32); ok {
			return v & maxUint24
		}
	case "unsigned int", "unsigned integer":
		if v, ok := value.(int32); ok {
			return int64(v) & math.MaxUint32
		}
	}
	return value
}
