package iceberg

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

func TestIsValidTypeForColumnBinary(t *testing.T) {
	testCases := []struct {
		name       string
		columnType string
		incoming   types.DataType
		valid      bool
	}{
		{
			name:       "binary column accepts bytes",
			columnType: "binary",
			incoming:   types.Binary,
			valid:      true,
		},
		{
			name:       "fixed column accepts fixed bytes of the same width",
			columnType: "fixed[16]",
			incoming:   types.FixedBinaryOf(16),
			valid:      true,
		},
		{
			name:       "fixed column accepts width-less bytes, since a value cannot reveal a width",
			columnType: "fixed[16]",
			incoming:   types.Binary,
			valid:      true,
		},
		{
			name:       "binary column accepts fixed bytes of any width",
			columnType: "binary",
			incoming:   types.FixedBinaryOf(16),
			valid:      true,
		},
		{
			name:       "fixed column rejects a different width",
			columnType: "fixed[16]",
			incoming:   types.FixedBinaryOf(32),
			valid:      false,
		},
		{
			name:       "string column rejects bytes",
			columnType: "string",
			incoming:   types.Binary,
			valid:      false,
		},
		{
			name:       "binary column accepts text as its bytes",
			columnType: "binary",
			incoming:   types.String,
			valid:      true,
		},
		{
			name:       "fixed column rejects text",
			columnType: "fixed[16]",
			incoming:   types.String,
			valid:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, isValidTypeForColumn(tc.columnType, tc.incoming))
		})
	}
}

func TestIsValidTypeForColumnNumeric(t *testing.T) {
	testCases := []struct {
		name       string
		columnType string
		incoming   types.DataType
		valid      bool
	}{
		{
			name:       "iceberg promotes int to long",
			columnType: "int",
			incoming:   types.Int64,
			valid:      true,
		},
		{
			name:       "an int fits a long column as it is",
			columnType: "long",
			incoming:   types.Int32,
			valid:      true,
		},
		{
			name:       "float column accepts double",
			columnType: "float",
			incoming:   types.Float64,
			valid:      true,
		},
		{
			name:       "double column accepts float",
			columnType: "double",
			incoming:   types.Float32,
			valid:      true,
		},
		{
			name:       "anything fits a string column",
			columnType: "string",
			incoming:   types.Int32,
			valid:      true,
		},
		{
			name:       "int column rejects string",
			columnType: "int",
			incoming:   types.String,
			valid:      false,
		},
		{
			name:       "boolean column rejects int",
			columnType: "boolean",
			incoming:   types.Int32,
			valid:      false,
		},
		{
			name:       "timestamptz column accepts a value detected at another precision",
			columnType: "timestamptz",
			incoming:   types.TimestampMicro,
			valid:      true,
		},
		{
			name:       "timestamptz column accepts the coarsest precision",
			columnType: "timestamptz",
			incoming:   types.Timestamp,
			valid:      true,
		},
		{
			name:       "timestamptz column accepts the finest precision",
			columnType: "timestamptz",
			incoming:   types.TimestampNano,
			valid:      true,
		},
		{
			name:       "timestamptz column rejects string",
			columnType: "timestamptz",
			incoming:   types.String,
			valid:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, isValidTypeForColumn(tc.columnType, tc.incoming))
		})
	}
}
