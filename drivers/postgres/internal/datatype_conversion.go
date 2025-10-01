package driver

import (
	"github.com/datazip-inc/olake/types"
)

var pgTypeToDataTypes = map[string]types.DataType{
	// TODO: add proper types (not only int64)
	"bigint":      types.Int64,
	"int8":        types.Int64,
	"tinyint":     types.Int32,
	"integer":     types.Int32,
	"smallint":    types.Int32,
	"smallserial": types.Int32,
	"int":         types.Int32,
	"int2":        types.Int32,
	"int4":        types.Int32,
	"serial":      types.Int32,
	"serial2":     types.Int32,
	"serial4":     types.Int32,
	"serial8":     types.Int64,
	"bigserial":   types.Int64,

	// numbers
	"decimal":          types.Float64,
	"numeric":          types.Float64,
	"double precision": types.Float64,
	"float":            types.Float32,
	"float4":           types.Float32,
	"float8":           types.Float64,
	"real":             types.Float32,

	// boolean
	"bool":    types.Bool,
	"boolean": types.Bool,

	// strings
	"bit varying":       types.String,
	"box":               types.String,
	"bytea":             types.String,
	"character":         types.String,
	"char":              types.String,
	"varbit":            types.String,
	"bit":               types.String,
	"bit(n)":            types.String,
	"varying(n)":        types.String,
	"cidr":              types.String,
	"inet":              types.String,
	"macaddr":           types.String,
	"macaddr8":          types.String,
	"character varying": types.String,
	"text":              types.String,
	"varchar":           types.String,
	"longvarchar":       types.String,
	"circle":            types.String,
	"hstore":            types.String,
	"name":              types.String,
	"uuid":              types.String,
	"json":              types.String,
	"jsonb":             types.String,
	"line":              types.String,
	"lseg":              types.String,
	"money":             types.String,
	"path":              types.String,
	"pg_lsn":            types.String,
	"point":             types.String,
	"polygon":           types.String,
	"tsquery":           types.String,
	"tsvector":          types.String,
	"xml":               types.String,
	"enum":              types.String,
	"tsrange":           types.String,
	"bpchar":            types.String, // blank-padded character

	// date/time
	"time":                        types.String,
	"timez":                       types.String,
	"interval":                    types.String,
	"date":                        types.Timestamp,
	"timestamp":                   types.Timestamp,
	"timestampz":                  types.Timestamp,
	"timestamp with time zone":    types.Timestamp,
	"timestamp without time zone": types.Timestamp,
	"timestamptz":                 types.Timestamp,

	// arrays
	"ARRAY":         types.Array,
	"array":         types.Array,
	"bool[]":        types.Array,
	"int2[]":        types.Array,
	"int4[]":        types.Array,
	"text[]":        types.Array,
	"bytea[]":       types.Array,
	"int8[]":        types.Array,
	"float4[]":      types.Array,
	"float8[]":      types.Array,
	"timestamp[]":   types.Array,
	"date[]":        types.Array,
	"timestamptz[]": types.Array,
	"numeric[]":     types.Array,
	"uuid[]":        types.Array,
	"jsonb[]":       types.Array,
}
