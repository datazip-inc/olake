package abstract

// GlobalTypeToDataType maps database-specific types (Postgres/MySQL) to internal standard types
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
	"decimal": "float",
	"numeric": "float",
	"float4":  "float",
	"money":   "float",

	"double":           "double",
	"float8":           "double",
	"double precision": "double",

	// Boolean Types
	"bool":    "boolean",
	"boolean": "boolean",

	// String Types
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

var MySQLSchema = map[string]string{
	"id":                     "unsigned int",
	"id_bigint":              "bigint",
	"id_int":                 "int",
	"id_int_unsigned":        "unsigned int",
	"id_integer":             "int",
	"id_integer_unsigned":    "unsigned int",
	"id_mediumint":           "mediumint",
	"id_mediumint_unsigned":  "unsigned mediumint",
	"id_smallint":            "smallint",
	"id_smallint_unsigned":   "unsigned smallint",
	"id_tinyint":             "tinyint",
	"id_tinyint_unsigned":    "unsigned tinyint",
	"price_decimal":          "decimal",
	"price_double":           "double",
	"price_double_precision": "double",
	"price_float":            "float",
	"price_numeric":          "decimal",
	"price_real":             "double",
	"name_char":              "char",
	"name_varchar":           "varchar",
	"name_text":              "text",
	"name_tinytext":          "tinytext",
	"name_mediumtext":        "mediumtext",
	"name_longtext":          "longtext",
	"name_enum":              "enum",
	"created_date":           "datetime",
	"created_time":           "time",
	"created_timestamp":      "timestamp",
	"is_active":              "tinyint",
	"json_data":              "json",
	"long_varchar":           "mediumtext",
	"name_bool":              "tinyint",
}
