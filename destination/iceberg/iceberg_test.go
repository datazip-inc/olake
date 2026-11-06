package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidTypeForColumn(t *testing.T) {
	testCases := []struct {
		name       string
		columnType string
		valueType  string
		valid      bool
	}{
		{
			name:       "binary column accepts bytes",
			columnType: "binary",
			valueType:  "binary",
			valid:      true,
		},
		{
			name:       "fixed column accepts fixed bytes of the same width",
			columnType: "fixed[16]",
			valueType:  "fixed[16]",
			valid:      true,
		},
		{
			name:       "fixed column accepts width-less bytes, since a value cannot reveal a width",
			columnType: "fixed[16]",
			valueType:  "binary",
			valid:      true,
		},
		{
			name:       "binary column accepts fixed bytes of any width",
			columnType: "binary",
			valueType:  "fixed[16]",
			valid:      true,
		},
		{
			name:       "fixed column rejects a different width",
			columnType: "fixed[16]",
			valueType:  "fixed[32]",
			valid:      false,
		},
		{
			name:       "string column rejects bytes",
			columnType: "string",
			valueType:  "binary",
			valid:      false,
		},
		{
			name:       "binary column accepts text as its bytes",
			columnType: "binary",
			valueType:  "string",
			valid:      true,
		},
		{
			name:       "fixed column rejects text",
			columnType: "fixed[16]",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "iceberg promotes int to long",
			columnType: "int",
			valueType:  "long",
			valid:      true,
		},
		{
			name:       "an int fits a long column as it is",
			columnType: "long",
			valueType:  "int",
			valid:      true,
		},
		{
			name:       "float column accepts double",
			columnType: "float",
			valueType:  "double",
			valid:      true,
		},
		{
			name:       "double column accepts float",
			columnType: "double",
			valueType:  "float",
			valid:      true,
		},
		{
			name:       "anything fits a string column",
			columnType: "string",
			valueType:  "int",
			valid:      true,
		},
		{
			name:       "int column rejects string",
			columnType: "int",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "boolean column rejects int",
			columnType: "boolean",
			valueType:  "int",
			valid:      false,
		},
		{
			name:       "timestamptz column accepts timestamptz",
			columnType: "timestamptz",
			valueType:  "timestamptz",
			valid:      true,
		},
		{
			name:       "timestamptz column rejects string",
			columnType: "timestamptz",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "a column type olake does not map keeps its own value type and rejects string",
			columnType: "date",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "decimal column accepts a narrower decimal of its scale",
			columnType: "decimal(12, 2)",
			valueType:  "decimal(10, 2)",
			valid:      true,
		},
		{
			name:       "iceberg widens a decimal's precision",
			columnType: "decimal(10, 2)",
			valueType:  "decimal(12, 2)",
			valid:      true,
		},
		{
			name:       "decimal column holds a smaller scale exactly",
			columnType: "decimal(11, 3)",
			valueType:  "decimal(10, 2)",
			valid:      true,
		},
		{
			name:       "decimal column rejects a larger scale",
			columnType: "decimal(10, 2)",
			valueType:  "decimal(10, 3)",
			valid:      false,
		},
		{
			name:       "decimal column rejects double",
			columnType: "decimal(10, 2)",
			valueType:  "double",
			valid:      false,
		},
		{
			name:       "double column accepts decimal",
			columnType: "double",
			valueType:  "decimal(10, 2)",
			valid:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, isValidTypeForColumn(tc.columnType, tc.valueType))
		})
	}
}

func TestIsPromotionRequired(t *testing.T) {
	testCases := []struct {
		name     string
		oldType  string
		newType  string
		required bool
	}{
		{
			name:     "int widens to long",
			oldType:  "int",
			newType:  "long",
			required: true,
		},
		{
			name:     "a decimal widens its precision",
			oldType:  "decimal(10, 2)",
			newType:  "decimal(12, 2)",
			required: true,
		},
		{
			name:     "a narrower decimal needs nothing",
			oldType:  "decimal(12, 2)",
			newType:  "decimal(10, 2)",
			required: false,
		},
		{
			name:     "a decimal never changes its scale",
			oldType:  "decimal(10, 2)",
			newType:  "decimal(12, 3)",
			required: false,
		},
		{
			name:     "a double never becomes a decimal",
			oldType:  "double",
			newType:  "decimal(10, 2)",
			required: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.required, isPromotionRequired(tc.oldType, tc.newType))
		})
	}
}

// TestParseSchema reads the schema the java writer prints.
func TestParseSchema(t *testing.T) {
	testCases := []struct {
		name   string
		schema string
		want   map[string]string
	}{
		{
			name:   "an identifier field ends in (id)",
			schema: "table {\n  1: _olake_id: required string (id)\n}",
			want:   map[string]string{"_olake_id": "string"},
		},
		{
			name:   "a decimal type holds a space",
			schema: "table {\n  2: price: optional decimal(10, 2)\n}",
			want:   map[string]string{"price": "decimal(10, 2)"},
		},
		{
			name:   "a documented field ends in its doc",
			schema: "table {\n  3: qty: optional long (units sold)\n}",
			want:   map[string]string{"qty": "long"},
		},
		{
			name:   "a fixed type",
			schema: "table {\n  4: digest: optional fixed[16]\n}",
			want:   map[string]string{"digest": "fixed[16]"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := parseSchema(tc.schema)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, schema)
		})
	}
}
