package driver

import (
	"regexp"
	"strconv"

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

	// Binary types travel as bytes. BINARY(n) is fixed width; discover resolves the n from
	// COLUMN_TYPE (see fixedBinaryType), the value path only needs the family.
	"binary":     types.Binary,
	"varbinary":  types.Binary,
	"tinyblob":   types.Binary,
	"blob":       types.Binary,
	"mediumblob": types.Binary,
	"longblob":   types.Binary,

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

var columnTypeLengthPattern = regexp.MustCompile(`^\s*\w+\s*\((\d+)\)`)

// fixedBinaryType returns fixed_binary(n) for a BINARY(n) column, using the length that
// information_schema's COLUMN_TYPE carries (e.g. "binary(16)"); any other column keeps the
// DataType the type map produced.
func fixedBinaryType(dataType string, columnType string, mapped types.DataType) types.DataType {
	if mapped != types.Binary || dataType != "binary" {
		return mapped
	}
	match := columnTypeLengthPattern.FindStringSubmatch(columnType)
	if match == nil {
		return mapped
	}
	length, err := strconv.Atoi(match[1])
	if err != nil || length <= 0 {
		return mapped
	}
	return types.FixedBinaryOf(length)
}
