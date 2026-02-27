package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/types"
)

// TODO: add support for utf-8 invalid and binary datatypes

// rowversion, timestamp (rowversion synonym), geometry, and geography can produce
// binary or non-UTF-8 values; mapping them to string cause sync to fail.

// mssqlTypeToDataTypes maps SQL Server types to internal data types.
var mssqlTypeToDataTypes = map[string]types.DataType{
	// Integer types
	"tinyint":  types.Int32,
	"smallint": types.Int32,
	"int":      types.Int32,
	"bigint":   types.Int64,

	// Exact numeric
	"decimal": types.Float64,
	"numeric": types.Float64,

	// Approximate numeric
	"float": types.Float64,
	"real":  types.Float32,

	// Bit / boolean-like
	"bit": types.Bool,

	// Money types
	"smallmoney": types.Float64,
	"money":      types.Float64,

	// Character strings
	"char":     types.String,
	"varchar":  types.String,
	"text":     types.String,
	"nchar":    types.String,
	"nvarchar": types.String,
	"ntext":    types.String,
	"sysname":  types.String,
	"json":     types.String,

	// Binary
	"binary":     types.String,
	"varbinary":  types.String,
	"image":      types.String,
	"rowversion": types.String, // Row versioning (timestamp is deprecated synonym for rowversion)
	"timestamp":  types.String, // Note: In SQL Server, timestamp is NOT a date/time type, it's a rowversion

	// Date and time
	"date":           types.Timestamp,
	"smalldatetime":  types.Timestamp,
	"datetime":       types.Timestamp,
	"datetime2":      types.TimestampMicro,
	"datetimeoffset": types.TimestampMicro,
	"time":           types.String,

	// Unique identifiers
	"uniqueidentifier": types.String,

	// Spatial
	"geometry":  types.String,
	"geography": types.String,

	// Other complex types
	"sql_variant": types.String,
	"xml":         types.String,
	"hierarchyid": types.String,
}

// formatUniqueIdentifierBytes converts SQL Server's mixed-endian UNIQUEIDENTIFIER
// byte layout to canonical RFC4122 UUID string representation.
func formatUniqueIdentifierBytes(v []byte) (string, bool) {
	if len(v) != 16 {
		return "", false
	}

	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		v[3], v[2], v[1], v[0], // first 4 bytes (little-endian)
		v[5], v[4], // next 2 bytes (little-endian)
		v[7], v[6], // next 2 bytes (little-endian)
		v[8], v[9], // next 2 bytes (big-endian)
		v[10], v[11], v[12], v[13], v[14], v[15], // last 6 bytes (big-endian)
	), true
}
