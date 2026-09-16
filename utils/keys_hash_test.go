package utils

import (
	"testing"

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
}
