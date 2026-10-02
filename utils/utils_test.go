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
	key := []byte{0xff, 0x00, 0x80}
	composite := map[string]any{"a": []byte{0xff}, "b": "x"}

	testCases := []struct {
		name         string
		stateVersion int
		record       map[string]any
		keys         []string
		expected     string
	}{
		{
			name:         "single binary key is its hex",
			stateVersion: constants.LatestStateVersion,
			record:       map[string]any{"id": key},
			keys:         []string{"id"},
			expected:     "ff0080",
		},
		{
			name:         "non-binary keys are unchanged",
			stateVersion: constants.LatestStateVersion,
			record:       map[string]any{"id": 42},
			keys:         []string{"id"},
			expected:     "42",
		},
		{
			name:         "composite keys hash the hex form of byte values",
			stateVersion: constants.LatestStateVersion,
			record:       composite,
			keys:         []string{"a", "b"},
			expected:     GetKeysHash(map[string]any{"a": "ff", "b": "x"}, "a", "b"),
		},
		{
			name:         "state written before version 8 hashed the printed form of a byte key",
			stateVersion: 7,
			record:       map[string]any{"id": key},
			keys:         []string{"id"},
			expected:     fmt.Sprintf("%v", key),
		},
		{
			name:         "composite keys hash the printed form before version 8",
			stateVersion: 7,
			record:       composite,
			keys:         []string{"a", "b"},
			expected:     GetKeysHash(map[string]any{"a": fmt.Sprintf("%v", []byte{0xff}), "b": "x"}, "a", "b"),
		},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = tc.stateVersion
			assert.Equal(t, tc.expected, GetKeysHash(tc.record, tc.keys...))
		})
	}
}
