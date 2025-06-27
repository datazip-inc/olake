package driver

import (
	"database/sql"
	"strings"

	"github.com/datazip-inc/olake/types"
)

// Oracle type mapping to our internal types
var oracleTypeToDataTypes = map[string]types.DataType{
	// Numeric types
	"int32":         types.Int32,
	"int64":         types.Int64,
	"ibfloat":       types.Float32,
	"binary_float":  types.Float32,
	"ibdouble":      types.Float64,
	"number":        types.Float64,
	"float":         types.Float64,
	"binary_double": types.Float64,

	// String types
	"varchar2":    types.String,
	"nvarchar2":   types.String,
	"char":        types.String,
	"nchar":       types.String,
	"longvarchar": types.String,
	"clob":        types.String,
	"nclob":       types.String,
	"long":        types.String, //LONG
	"raw":         types.String, //RAW
	"longraw":     types.String, //LONG RAW

	// Date/Time types
	"date":             types.TimestampMicro,
	"timestampdty":     types.TimestampMicro,
	"timestamptz_dty":  types.TimestampMicro,
	"timestampltz_dty": types.TimestampMicro,

	// Interval types
	"intervalym_dty": types.String,
	"intervalds_dty": types.String,

	"xmltype": types.String,
	"blob":    types.String,
	"bfile":   types.String,
}

// reformatOracleDatatype removes extra information from type names for matching and returns in golang type
func reformatOracleDatatype(dataType string, precision, scale sql.NullInt64) (types.DataType, bool) {
	switch {
	case strings.HasPrefix(dataType, "TIMESTAMP"):
		return types.TimestampMicro, true

	case strings.HasPrefix(dataType, "INTERVAL"):
		return types.String, true

	case strings.HasPrefix(dataType, "NUMBER"):
		if scale.Valid && scale.Int64 == 0 {
			if precision.Valid && precision.Int64 <= 9 {
				return types.Int32, true
			}
			return types.Int64, true
		}
		return types.Float64, true

	default:
		if val, found := oracleTypeToDataTypes[strings.ToLower(dataType)]; found {
			return val, true
		}
		// Treat unknown data types as strings
		return types.Unknown, false
	}
}
