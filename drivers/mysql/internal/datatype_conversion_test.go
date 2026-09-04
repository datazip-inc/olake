package driver

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

// TestFixedBinaryType asserts that discover turns BINARY(n) into fixed_binary(n) from
// information_schema's COLUMN_TYPE and leaves every other column alone.
func TestFixedBinaryType(t *testing.T) {
	assert.Equal(t, types.FixedBinaryOf(16), fixedBinaryType("binary", "binary(16)", types.Binary))
	assert.Equal(t, types.FixedBinaryOf(1), fixedBinaryType("binary", "binary(1)", types.Binary))
	assert.Equal(t, types.Binary, fixedBinaryType("binary", "binary", types.Binary), "no length in COLUMN_TYPE keeps variable bytes")
	assert.Equal(t, types.Binary, fixedBinaryType("binary", "binary(0)", types.Binary))
	assert.Equal(t, types.Binary, fixedBinaryType("varbinary", "varbinary(16)", types.Binary), "VARBINARY(n) is a maximum, not a width")
	assert.Equal(t, types.Binary, fixedBinaryType("blob", "blob", types.Binary))
	assert.Equal(t, types.String, fixedBinaryType("char", "char(16)", types.String))
}

func TestMysqlBinaryTypesMapToBinary(t *testing.T) {
	for _, mysqlType := range []string{"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"} {
		assert.Equal(t, types.Binary, mysqlTypeToDataTypes[mysqlType], mysqlType)
	}
	for _, mysqlType := range []string{"char", "varchar", "tinytext", "text", "mediumtext", "longtext"} {
		assert.Equal(t, types.String, mysqlTypeToDataTypes[mysqlType], mysqlType)
	}
}
