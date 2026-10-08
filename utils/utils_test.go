package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHumanDuration(t *testing.T) {
	tests := []struct {
		name     string
		in       time.Duration
		expected string
	}{
		{name: "zero", in: 0, expected: "0 seconds"},
		{name: "one second", in: time.Second, expected: "1 second"},
		{name: "seconds", in: 45 * time.Second, expected: "45 seconds"},
		{name: "one minute", in: time.Minute, expected: "1 minute"},
		{name: "fractional minutes", in: 90 * time.Second, expected: "1.5 minutes"},
		{name: "just under an hour", in: 59 * time.Minute, expected: "59 minutes"},
		{name: "one hour", in: time.Hour, expected: "1 hour"},
		{name: "fractional hours", in: 90 * time.Minute, expected: "1.5 hours"},
		{name: "hours", in: 12 * time.Hour, expected: "12 hours"},
		{name: "one day", in: 24 * time.Hour, expected: "1 day"},
		{name: "fractional days", in: 36 * time.Hour, expected: "1.5 days"},
		{name: "days", in: 7 * 24 * time.Hour, expected: "7 days"},
		{name: "fractional days rounded", in: 180 * time.Hour, expected: "7.5 days"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, HumanDuration(tc.in))
		})
	}
}

func TestUpdateMethodType(t *testing.T) {
	tests := []struct {
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
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, UpdateMethodType(tc.in))
		})
	}
}
