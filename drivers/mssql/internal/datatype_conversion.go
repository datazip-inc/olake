package driver

import (
	"github.com/datazip-inc/olake/types"
)

// mssqlTypeToDataTypes maps SQL Server types to internal data types.
//
// Binary types (binary, varbinary, image, rowversion, timestamp, hierarchyid)
// are base64-encoded strings to avoid UTF-8 corruption when the go-mssqldb driver returns raw []byte values.
//
// Unsupported types:
//   - geometry, geography
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

	// Other complex types
	"sql_variant": types.String,
	"xml":         types.String,
	"hierarchyid": types.String,
}
