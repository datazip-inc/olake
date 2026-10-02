package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHumanDuration(t *testing.T) {
	testCases := []struct {
		in       time.Duration
		expected string
	}{
		{0, "0 hours"},
		{time.Hour, "1 hour"},
		{12 * time.Hour, "12 hours"},
		{36 * time.Hour, "1.5 days"},
		{24 * time.Hour, "1 day"},
		{180 * time.Hour, "7.5 days"},
		{7 * 24 * time.Hour, "7 days"},
		{90 * time.Minute, "1.5 hours"},
	}
	for _, tc := range testCases {
		assert.Equal(t, tc.expected, HumanDuration(tc.in), tc.in.String())
	}
}
