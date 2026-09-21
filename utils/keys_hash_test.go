package utils

import (
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/assert"
)

// TestGetKeysHashBytes asserts that binary primary keys produce a text olake id: hex for a single
// key, and hex inside the composite hash input, so a key with non-UTF-8 bytes never reaches a
// proto string field.
func TestGetKeysHashBytes(t *testing.T) {
	assert.Equal(t, "ff0080", GetKeysHash(map[string]any{"id": []byte{0xff, 0x00, 0x80}}, "id"))
	assert.Equal(t, "42", GetKeysHash(map[string]any{"id": 42}, "id"), "non-binary keys are unchanged")

	composite := map[string]any{"a": []byte{0xff}, "b": "x"}
	assert.Equal(t, GetKeysHash(map[string]any{"a": "ff", "b": "x"}, "a", "b"), GetKeysHash(composite, "a", "b"),
		"composite keys hash the hex form of byte values")

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })
	constants.LoadedStateVersion = 7

	key := []byte{0xff, 0x00, 0x80}
	assert.Equal(t, fmt.Sprintf("%v", key), GetKeysHash(map[string]any{"id": key}, "id"),
		"state written before version 8 hashed the printed form of a byte key")
	assert.Equal(t, GetKeysHash(map[string]any{"a": fmt.Sprintf("%v", []byte{0xff}), "b": "x"}, "a", "b"),
		GetKeysHash(composite, "a", "b"), "composite keys hash the printed form before version 8")
}
