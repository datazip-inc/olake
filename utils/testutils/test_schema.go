package testutils

// GlobalTypeToDataType maps database-specific types (Postgres/MySQL/MongoDB) to internal standard types
var GlobalTypeMapping = map[string]string{
	// Integer Types
	"tinyint":            "int",
	"smallint":           "int",
	"mediumint":          "int",
	"int":                "int",
	"integer":            "int",
	"unsigned int":       "int",
	"unsigned smallint":  "int",
	"unsigned tinyint":   "int",
	"unsigned mediumint": "int",
	"int2":               "int",
	"int4":               "int",
	"smallserial":        "int",
	"serial":             "int",
	"serial2":            "int",
	"serial4":            "int",

	"bigint":    "bigint",
	"int8":      "bigint",
	"serial8":   "bigint",
	"bigserial": "bigint",
	"year":      "bigint",

	// Floating Point Types
	"float":   "float",
	"real":    "float",
	"decimal": "double",
	"numeric": "double",
	"float4":  "float",

	"double":           "double",
	"float8":           "double",
	"double precision": "double",
	"number":           "double",

	// Boolean Types
	"bool":    "boolean",
	"boolean": "boolean",

	// String Types
	"string":            "string",
	"char":              "string",
	"varchar":           "string",
	"tinytext":          "string",
	"text":              "string",
	"mediumtext":        "string",
	"longtext":          "string",
	"character":         "string",
	"character varying": "string",
	"longvarchar":       "string",
	"bpchar":            "string",
	"name":              "string",

	// Binary Types
	"binary":     "string",
	"varbinary":  "string",
	"tinyblob":   "string",
	"blob":       "string",
	"mediumblob": "string",
	"longblob":   "string",
	"bytea":      "string",

	// JSON and Document Types
	"json":   "string",
	"jsonb":  "string",
	"xml":    "string",
	"hstore": "string",

	// Network Types
	"cidr":     "string",
	"inet":     "string",
	"macaddr":  "string",
	"macaddr8": "string",

	// Spatial Types
	"geometry":           "string",
	"point":              "string",
	"linestring":         "string",
	"polygon":            "string",
	"multipoint":         "string",
	"multilinestring":    "string",
	"multipolygon":       "string",
	"geometrycollection": "string",
	"circle":             "string",
	"path":               "string",
	"box":                "string",
	"line":               "string",
	"lseg":               "string",

	// Full Text Search Types
	"tsvector": "string",
	"tsquery":  "string",

	// UUID
	"uuid": "string",

	// Range Types
	"tsrange":   "string",
	"tstzrange": "string",
	"int4range": "string",
	"numrange":  "string",
	"daterange": "string",

	// Array
	"array":      "string",
	"ARRAY":      "string",
	"int2vector": "string",

	// Enum and Set
	"enum": "string",
	"set":  "string",

	// Date/Time
	"date":                        "timestamp",
	"timestamp":                   "timestamp",
	"datetime":                    "timestamp",
	"timestamptz":                 "timestamp",
	"timestamp with time zone":    "timestamp",
	"timestamp without time zone": "timestamp",

	"time":     "string",
	"timez":    "string",
	"interval": "string",

	// Misc
	"pg_lsn":      "string",
	"bit varying": "string",
	"varbit":      "string",
	"bit(n)":      "string",
	"varying(n)":  "string",
}
