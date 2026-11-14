package driver

import (
	"database/sql"
	"slices"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// mysqlTypeToDataTypes returns the mapping of MySQL data types to internal data types for the state
// version this sync is pinned at
func mysqlTypeToDataTypes() map[string]types.DataType {
	binaryTypeMapping := types.Binary
	if !utils.IsBinarySupported() {
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

// resolveColumnType returns the type a column carries once its declared length, precision and scale are known:
// a BINARY(n) column the mapping carries as bytes always holds exactly n bytes, so it resolves to
// fixed_binary(n), and a DECIMAL(p,s) to the type NumericOf gives it.
func resolveColumnType(dataType string, dataMaxLength, precision, scale sql.NullInt64, mapped types.DataType) types.DataType {
	switch {
	case dataType == "decimal" && precision.Valid && scale.Valid:
		return types.NumericOf(precision.Int64, scale.Int64)
	case dataType != "binary" || dataMaxLength.Int64 == 0 || mapped != types.Binary:
		return mapped
	default:
		return types.FixedBinaryOf(int(dataMaxLength.Int64))
	}
}

// mysqlColumnTypeName names a DECIMAL column by its precision and scale, so its values read as a decimal.
// unsignedDecimals are the UNSIGNED ones, whose precision go-sql-driver reports a digit short.
func mysqlColumnTypeName(unsignedDecimals []string) func(colType *sql.ColumnType) string {
	return func(colType *sql.ColumnType) string {
		name := colType.DatabaseTypeName()
		precision, scale, ok := colType.DecimalSize()
		if !ok || name != "DECIMAL" {
			return name
		}
		if slices.Contains(unsignedDecimals, colType.Name()) {
			precision++ // go-sql-driver counts a sign digit mysql leaves out of an UNSIGNED DECIMAL
		}
		return typeutils.DecimalColumnType(name, precision, scale)
	}
}
