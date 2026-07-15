// Package utils holds the small, dependency-light helpers that are shared between the olake
// product (which re-exports them from its own utils package) and the tests tree. Keeping the
// single implementation here removes the duplicate copies the two trees used to carry.
package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Ternary returns cond ? a : b.
func Ternary(cond bool, a, b any) any {
	if cond {
		return a
	}
	return b
}

// Average returns the average of the given values, 0 for an empty slice.
func Average[T int | int8 | int16 | int32 | int64 | float32 | float64](values []T) float64 {
	if len(values) == 0 {
		return 0.0
	}
	var sum float64
	for _, v := range values {
		sum += float64(v)
	}
	return sum / float64(len(values))
}

// UnmarshalFile reads the JSON file at path into dest. credsFile is accepted for signature
// compatibility with the product helper; this shared copy never decrypts (the tests pass false).
func UnmarshalFile(file string, dest any, credsFile bool) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("file not found : %s", err)
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("failed to unmarshal file[%s]: %s", file, err)
	}
	return nil
}

// NormalizedEqual compares two JSON documents ignoring whitespace and ordering.
func NormalizedEqual(strune1, strune2 string) bool {
	normalize := func(s string) (string, error) {
		start := strings.IndexRune(s, '{')
		end := strings.LastIndex(s, "}")
		if start < 0 || end < 0 || start > end {
			return "", fmt.Errorf("no valid JSON object found")
		}
		core := s[start : end+1]
		core = strings.ReplaceAll(core, " ", "")
		core = strings.ReplaceAll(core, "\n", "")
		core = strings.ReplaceAll(core, "\t", "")
		return core, nil
	}

	c1, err := normalize(strune1)
	if err != nil {
		return false
	}
	c2, err := normalize(strune2)
	if err != nil {
		return false
	}

	rune1 := []rune(c1)
	rune2 := []rune(c2)
	if len(rune1) != len(rune2) {
		return false
	}
	sort.Slice(rune1, func(i, j int) bool { return rune1[i] < rune1[j] })
	sort.Slice(rune2, func(i, j int) bool { return rune2[i] < rune2[j] })
	return string(rune1) == string(rune2)
}

// Reformat lowercases key and replaces every non-alphanumeric symbol with '_'.
func Reformat(key string) string {
	key = strings.ToLower(key)
	var result strings.Builder
	for _, symbol := range key {
		if (symbol >= 'a' && symbol <= 'z') || (symbol >= '0' && symbol <= '9') {
			result.WriteRune(symbol)
		} else {
			result.WriteRune('_')
		}
	}
	return result.String()
}
