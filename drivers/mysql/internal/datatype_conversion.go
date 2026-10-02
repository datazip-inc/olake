package driver

import (
	"database/sql"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

// mysqlTypeToDataTypes returns the mapping of MySQL data types to internal data types for the state
// version this sync is pinned at
func mysqlTypeToDataTypes() map[string]types.DataType {
	binaryTypeMapping := types.Binary
	if constants.LoadedStateVersion < 8 {
		binaryTypeMapping = types.String
	}
	return map[string]types.DataType{
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
		"binary":     binaryTypeMapping,
		"varbinary":  binaryTypeMapping,
		"tinyblob":   binaryTypeMapping,
		"blob":       binaryTypeMapping,
		"mediumblob": binaryTypeMapping,
		"longblob":   binaryTypeMapping,

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
}

// resolveColumnType returns the type a column carries once its declared length is known: a
// BINARY(n) column the mapping carries as bytes always holds exactly n bytes, so it resolves to
// fixed_binary(n).
func resolveColumnType(dataType string, dataMaxLength sql.NullInt64, mapped types.DataType) types.DataType {
	if dataType != "binary" || dataMaxLength.Int64 == 0 || mapped != types.Binary {
		return mapped
	}
	return types.FixedBinaryOf(int(dataMaxLength.Int64))
}
