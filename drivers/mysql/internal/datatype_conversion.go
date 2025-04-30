package driver

import "github.com/datazip-inc/olake/types"

// Define a mapping of MySQL data types to internal data types
var mysqlTypeToDataTypes = map[string]types.DataType{
	// Integer types
	"tinyint":            types.Int32,
	"smallint":           types.Int32,
	"mediumint":          types.Int32,
	"int":                types.Int32,
	"integer":            types.Int32,
	"unsigned int":       types.Int32,
	"unsigned smallint":  types.Int32,
	"unsigned tinyint":   types.Int32,
	"unsigned mediumint": types.Int32,
	"bigint":             types.Int64,

	// Floating point types
	"float":   types.Float32,
	"real":    types.Float32,
	"decimal": types.Float32,
	"numeric": types.Float32,
	"double":  types.Float64,

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
	"year":      types.Int64,

	// time and datetime types treated as string for now
	"time":     types.String,
	"datetime": types.String,

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
