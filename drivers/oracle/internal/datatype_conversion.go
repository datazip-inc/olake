package driver

import (
	"strings"

	"github.com/datazip-inc/olake/types"
)

// Oracle type mapping to our internal types
var oracleTypeToDataTypes = map[string]types.DataType{
	// Numeric types
	"NUMBER":        types.Float64,
	"FLOAT":         types.Float64,
	"BINARY_FLOAT":  types.Float32,
	"BINARY_DOUBLE": types.Float64,
	"DECIMAL":       types.Float64,

	// String types
	"VARCHAR2":  types.String,
	"NVARCHAR2": types.String,
	"CHAR":      types.String,
	"NCHAR":     types.String,
	"CLOB":      types.String,
	"NCLOB":     types.String,
	"LONG":      types.String,
	"RAW":       types.String,
	"LONG RAW":  types.String,
	"BLOB":      types.String,
	"BFILE":     types.String,

	// Date/Time types
	"DATE":                           types.Timestamp,
	"TIMESTAMP":                      types.Timestamp,
	"TIMESTAMP WITH TIME ZONE":       types.Timestamp,
	"TIMESTAMP WITH LOCAL TIME ZONE": types.Timestamp,

	// Interval types
	"INTERVAL YEAR TO MONTH": types.String,
	"INTERVAL DAY TO SECOND": types.String,
}

// OracleDatatype removes precision and scale information from type names for matching and returns in go lang type
func OracleDatatype(dataType string) (types.DataType, bool) {

	// Handle timestamp variations
	if strings.HasPrefix(dataType, "TIMESTAMP") {
		if strings.Contains(dataType, "WITH TIME ZONE") {
			return oracleTypeToDataTypes["TIMESTAMP WITH TIME ZONE"], true
		}
		if strings.Contains(dataType, "WITH LOCAL TIME ZONE") {
			return oracleTypeToDataTypes["TIMESTAMP WITH LOCAL TIME ZONE"], true
		}
		return oracleTypeToDataTypes["TIMESTAMP"], true
	}

	// Handle interval variations
	if strings.HasPrefix(dataType, "INTERVAL") {
		if strings.Contains(dataType, "TO MONTH") {
			return oracleTypeToDataTypes["INTERVAL YEAR TO MONTH"], true
		}
		if strings.Contains(dataType, "TO SECOND") {
			return oracleTypeToDataTypes["INTERVAL DAY TO SECOND"], true
		}
	}

	if val, found := oracleTypeToDataTypes[dataType]; found {
		return val, true
	}

	// Treat unknown data types as strings
	return types.String, true
}
