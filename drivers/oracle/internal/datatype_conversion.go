package driver

import "github.com/datazip-inc/olake/types"

// Define a mapping of Oracle data types to internal data types
var oracleTypeToDataTypes = map[string]types.DataType{
	// String types
	"VARCHAR2":    types.String,
	"NVARCHAR2":   types.String,
	"CHAR":        types.String,
	"NCHAR":       types.String,
	"CLOB":        types.String,
	"NCLOB":       types.String,
	"LONG":        types.String,

	// Numeric types
	"NUMBER":      types.Float,
	"FLOAT":       types.Float,
	"BINARY_FLOAT": types.Float,
	"BINARY_DOUBLE": types.Float,
	"DECIMAL":     types.Float,
	"NUMERIC":     types.Float,
	"INTEGER":     types.Int32,
	"INT":         types.Int32,
	"SMALLINT":    types.Int32,
	"BIGINT":      types.Int64,

	// Date and time types
	"DATE":        types.DateTime,
	"TIMESTAMP":   types.DateTime,
	"TIMESTAMP WITH TIME ZONE": types.DateTime,
	"TIMESTAMP WITH LOCAL TIME ZONE": types.DateTime,
	"INTERVAL YEAR TO MONTH": types.String,
	"INTERVAL DAY TO SECOND": types.String,

	// Binary types
	"BLOB":        types.Binary,
	"BFILE":       types.Binary,
	"RAW":         types.Binary,
	"LONG RAW":    types.Binary,
	"BINARY":      types.Binary,
	"VARBINARY":   types.Binary,

	// Other types
	"XMLTYPE":     types.String,
	"JSON":        types.String,
	"BOOLEAN":     types.Boolean,
} 