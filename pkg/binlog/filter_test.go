package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsBinaryCollation pins the collation IDs the binlog uses to tell binary columns from text:
// 63 is the binary charset; utf8mb4 (45, 255) and latin1 (8) are text.
func TestIsBinaryCollation(t *testing.T) {
	assert.True(t, isBinaryCollation(63))
	assert.False(t, isBinaryCollation(45))
	assert.False(t, isBinaryCollation(255))
	assert.False(t, isBinaryCollation(8))
	assert.False(t, isBinaryCollation(0))
	assert.False(t, isBinaryCollation(1<<40), "out of range ids are not binary")
}

// TestWireTypeNames asserts the type-name rewrites that let the converter map a column by its
// real storage type: binary-charset CHAR/VARCHAR are BINARY/VARBINARY, text-charset BLOBs are TEXTs.
func TestWireTypeNames(t *testing.T) {
	assert.Equal(t, "BINARY", binaryTypeName("CHAR"))
	assert.Equal(t, "VARBINARY", binaryTypeName("VARCHAR"))
	assert.Equal(t, "BLOB", binaryTypeName("BLOB"))
	assert.Equal(t, "LONGBLOB", binaryTypeName("LONGBLOB"))

	assert.Equal(t, "TINYTEXT", textTypeName("TINYBLOB"))
	assert.Equal(t, "TEXT", textTypeName("BLOB"))
	assert.Equal(t, "MEDIUMTEXT", textTypeName("MEDIUMBLOB"))
	assert.Equal(t, "LONGTEXT", textTypeName("LONGBLOB"))
	assert.Equal(t, "VARCHAR", textTypeName("VARCHAR"))
}
