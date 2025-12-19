package driver

import "github.com/datazip-inc/olake/types"

var db2TypeToDataTypes = map[string]types.DataType{
	// integers
	"smallint": types.Int32,
	"integer":  types.Int32,
	"bigint":   types.Int64,

	// numeric / decimal
	"real":     types.Float32,
	"double":   types.Float64,
	"decimal":  types.Float64,
	"decfloat": types.Float64,

	// boolean (DB2 supports BOOLEAN in newer versions)
	"boolean": types.Bool,

	// strings
	"chararr":      types.String,
	"chararray":    types.String,
	"character":    types.String,
	"varchar":      types.String,
	"long varchar": types.String,
	"clob":         types.String,
	"graphic":      types.String,
	"vargraphic":   types.String,
	"xml":          types.String,
	"array":        types.String,
	"row":          types.String,

	// binary
	"blob":      types.String,
	"binary":    types.String,
	"varbinary": types.String,

	// date / time
	"time":      types.String,
	"date":      types.Timestamp,
	"timestamp": types.TimestampMicro,
}
