package utils

import (
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestReformat(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{name: "empty", key: "", expected: ""},
		{name: "lowercases letters", key: "ColumnName", expected: "columnname"},
		{name: "keeps digits", key: "col_2024", expected: "col_2024"},
		{name: "replaces spaces and punctuation", key: "order id (usd)", expected: "order_id__usd_"},
		{name: "replaces every special symbol", key: "a-b.c$d", expected: "a_b_c_d"},
		{name: "digits only", key: "12345", expected: "12345"},
		{name: "all special", key: "!@#", expected: "___"},
		// non-ascii letters are not in the accepted set, and one rune yields one underscore
		{name: "accented letter", key: "café", expected: "caf_"},
		{name: "cyrillic", key: "цена", expected: "____"},
		{name: "emoji", key: "a🙂b", expected: "a_b"},
		{name: "full width digit", key: "col１", expected: "col_"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, Reformat(tc.key))
		})
	}
}

func TestReformatIsValidUTF8(t *testing.T) {
	for _, key := range []string{"café", "naïve", "цена", "a🙂b", "ＦＵＬＬ"} {
		assert.True(t, utf8.ValidString(Reformat(key)), "mangled %q", key)
	}
}

func TestGetKeysHash(t *testing.T) {
	record := map[string]interface{}{
		"id":      42,
		"name":    "olake",
		"deleted": false,
	}

	t.Run("single key returns the value itself", func(t *testing.T) {
		assert.Equal(t, "42", GetKeysHash(record, "id"))
	})

	t.Run("single missing key falls through to the hash", func(t *testing.T) {
		got := GetKeysHash(record, "absent")
		assert.Len(t, got, 32)
		// a missing key hashes as its nil value, so any other record missing it hashes the same
		assert.Equal(t, GetKeysHash(map[string]interface{}{"other": 5}, "absent"), got)
	})

	t.Run("hashes the values, not the key names", func(t *testing.T) {
		assert.Equal(t,
			GetKeysHash(map[string]interface{}{"x": 1, "y": 2}, "x", "y"),
			GetKeysHash(map[string]interface{}{"p": 1, "q": 2}, "p", "q"))
	})

	t.Run("values are taken in key order", func(t *testing.T) {
		assert.NotEqual(t,
			GetKeysHash(map[string]interface{}{"x": 1, "y": 2}, "x", "y"),
			GetKeysHash(map[string]interface{}{"x": 2, "y": 1}, "x", "y"))
	})

	t.Run("key order does not change the hash", func(t *testing.T) {
		assert.Equal(t, GetKeysHash(record, "id", "name"), GetKeysHash(record, "name", "id"))
	})

	t.Run("no keys hashes the whole record", func(t *testing.T) {
		assert.Equal(t, GetHash(record), GetKeysHash(record))
	})

	t.Run("different values give different hashes", func(t *testing.T) {
		other := map[string]interface{}{"id": 43, "name": "olake"}
		assert.NotEqual(t, GetKeysHash(record, "id", "name"), GetKeysHash(other, "id", "name"))
	})

	t.Run("nil and missing values are indistinguishable", func(t *testing.T) {
		withNil := map[string]interface{}{"id": nil, "name": "olake"}
		withMissing := map[string]interface{}{"name": "olake"}
		assert.Equal(t, GetKeysHash(withNil, "id", "name"), GetKeysHash(withMissing, "id", "name"))
	})
}

func TestGetHashOrdersKeys(t *testing.T) {
	first := map[string]interface{}{"b": 2, "a": 1}
	second := map[string]interface{}{"a": 1, "b": 2}
	assert.Equal(t, GetHash(first), GetHash(second))
}

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
