/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testutils

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
