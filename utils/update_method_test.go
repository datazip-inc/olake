package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateMethodType(t *testing.T) {
	testCases := []struct {
		name     string
		in       any
		expected string
	}{
		// absent: configs saved before a driver had update_method
		{name: "nil", in: nil, expected: ""},
		{name: "cdc", in: map[string]any{"type": "CDC"}, expected: "CDC"},
		{name: "standalone", in: map[string]any{"type": "Standalone"}, expected: "Standalone"},
		{
			name:     "cdc with settings",
			in:       map[string]any{"type": "CDC", "initial_wait_time": 120},
			expected: "CDC",
		},
		// a legacy object without the discriminator reads as absent
		{name: "no type key", in: map[string]any{"initial_wait_time": 120}, expected: ""},
		{name: "not an object", in: "CDC", expected: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, UpdateMethodType(tc.in))
		})
	}
}
