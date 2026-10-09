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

func TestIsType(t *testing.T) {
	testCases := []struct {
		name      string
		dataType  string
		pattern   string
		numParams int
		ok        bool
		params    []int
	}{
		{
			name:      "one parameter",
			dataType:  "fixed_binary(16)",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        true,
			params:    []int{16},
		},
		{
			name:      "iceberg spelling",
			dataType:  "fixed[16]",
			pattern:   "fixed[%d]",
			numParams: 1,
			ok:        true,
			params:    []int{16},
		},
		{
			name:      "two parameters",
			dataType:  "decimal(9,2)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        true,
			params:    []int{9, 2},
		},
		{
			name:      "zero is a parameter",
			dataType:  "decimal(38,0)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        true,
			params:    []int{38, 0},
		},
		{
			name:      "parameters are not range checked",
			dataType:  "decimal(2,9)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        true,
			params:    []int{2, 9},
		},
		{
			name:      "a space must be in the pattern",
			dataType:  "decimal(9, 2)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        false,
		},
		{
			name:      "a spaced pattern",
			dataType:  "decimal(9, 2)",
			pattern:   "decimal(%d, %d)",
			numParams: 2,
			ok:        true,
			params:    []int{9, 2},
		},
		{
			name:      "too few parameters",
			dataType:  "decimal(9)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        false,
		},
		{
			name:      "too many parameters",
			dataType:  "decimal(9,2,3)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        false,
		},
		{
			name:      "numParams disagrees with the pattern",
			dataType:  "fixed_binary(16)",
			pattern:   "fixed_binary(%d)",
			numParams: 2,
			ok:        false,
		},
		{
			name:      "negative parameter",
			dataType:  "decimal(9,-1)",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        false,
		},
		{
			name:      "fractional parameter",
			dataType:  "fixed_binary(1.5)",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "empty parameter",
			dataType:  "fixed_binary()",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "overflowing parameter",
			dataType:  "fixed_binary(99999999999999999999)",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "unclosed",
			dataType:  "fixed[16",
			pattern:   "fixed[%d]",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "trailing text",
			dataType:  "fixed_binary(16)x",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "another family",
			dataType:  "fixed[16]",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "the bare family name",
			dataType:  "decimal",
			pattern:   "decimal(%d,%d)",
			numParams: 2,
			ok:        false,
		},
		{
			name:      "empty type",
			dataType:  "",
			pattern:   "fixed_binary(%d)",
			numParams: 1,
			ok:        false,
		},
		{
			name:      "a pattern without parameters matches itself",
			dataType:  "binary",
			pattern:   "binary",
			numParams: 0,
			ok:        true,
		},
		{
			name:      "a pattern without parameters has none to return",
			dataType:  "binary",
			pattern:   "binary",
			numParams: 1,
			ok:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ok, params := IsType(tc.dataType, tc.pattern, tc.numParams)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.params, params)

			scanned := make([]int, tc.numParams)
			assert.Equal(t, tc.ok, ScanType(tc.dataType, tc.pattern, scanned))
			if tc.ok && tc.numParams > 0 {
				assert.Equal(t, tc.params, scanned)
			}
		})
	}
}
