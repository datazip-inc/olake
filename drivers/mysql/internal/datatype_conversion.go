package driver

import "github.com/datazip-inc/olake/types"

// Define a mapping of MySQL data types to internal data types
var mysqlTypeToDataTypes = map[string]types.DataType{
	// Integer types
	"tinyint":   types.Int64,
	"smallint":  types.Int64,
	"mediumint": types.Int64,
	"int":       types.Int64,
	"integer":   types.Int64,
	"bigint":    types.Int64,

	// Floating point types
	"float":   types.Float64,
	"double":  types.Float64,
	"real":    types.Float64,
	"decimal": types.Float64,
	"numeric": types.Float64,

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
	"time":      types.Timestamp,
	"datetime":  types.Timestamp,
	"timestamp": types.Timestamp,
	"year":      types.Int64,

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
