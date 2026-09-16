package driver

import (
	"regexp"
	"strconv"
	"strings"

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

type consturctType func(params []int) (types.DataType, bool)

// parameterisedTypes lists the mysql types whose parameters olake models. DATA_TYPE names the type
// without them, so discover resolves the mapped type against what COLUMN_TYPE carries.
var parameterisedTypes = map[string]consturctType{
	"binary": func(params []int) (types.DataType, bool) {
		if len(params) != 1 || params[0] <= 0 {
			return "", false
		}
		return types.FixedBinaryOf(params[0]), true
	},
}

var columnTypeParamsPattern = regexp.MustCompile(`^\s*\w+\s*\(([^)]*)\)`)

// resolveColumnType returns the type a column carries once the parameters in COLUMN_TYPE are known
func resolveColumnType(dataType string, columnType string, mapped types.DataType) types.DataType {
	typeConstructor, parameterised := parameterisedTypes[dataType]
	if !parameterised {
		return mapped
	}
	params := getColumnTypeParams(columnType)

	parameterisedType, ok := typeConstructor(params)
	if !ok {
		return mapped
	}
	return parameterisedType
}

// getColumnTypeParams returns the numbers COLUMN_TYPE carries in parentheses, so "binary(16)" yields
// [16] and "decimal(9,2)" yields [9 2]. A column with no parentheses, or one whose parameters are
// not all numbers, carries none.
func getColumnTypeParams(columnType string) []int {
	match := columnTypeParamsPattern.FindStringSubmatch(columnType)
	if match == nil {
		return nil
	}
	parts := strings.Split(match[1], ",")
	params := make([]int, 0, len(parts))
	for _, part := range parts {
		number, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil
		}
		params = append(params, number)
	}
	return params
}
