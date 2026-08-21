// This file holds the small, dependency-light helpers the tests tree needs. Each one is a copy
// of an olake helper of the same name: the tests are a separate module and deliberately do not
// depend on olake, so they carry their own copy rather than sharing one through lib.
package testutils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
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

// UnmarshalFile reads the JSON file at path into dest. The ignored bool mirrors olake's
// credsFile flag for signature compatibility; this copy never decrypts.
func UnmarshalFile(file string, dest any, _ bool) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("file not found : %s", err)
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("failed to unmarshal file[%s]: %s", file, err)
	}
	return nil
}

// FileLoggerWithPath marshals content to JSON and writes it to path, truncating any existing file.
func FileLoggerWithPath(content any, path string) error {
	if path == "" {
		return fmt.Errorf("path is not set")
	}
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %s", err)
	}
	if err := os.WriteFile(path, contentBytes, 0600); err != nil {
		return fmt.Errorf("failed to write data to file: %s", err)
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

// Reformat lowercases key and replaces every non-alphanumeric symbol with '_', matching how
// olake normalizes column names before writing them to a destination.
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

// ParseFloat64 converts a value to float64.
func ParseFloat64(v any) (float64, error) {
	switch v := v.(type) {
	case json.Number:
		return v.Float64()
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to change string %v to float64: %s", v, err)
		}
		return f, nil
	}
	return 0, fmt.Errorf("failed to change %v (type:%T) to float64", v, v)
}

// Concurrent runs execute for every element of array with the given concurrency limit.
func Concurrent[T any](ctx context.Context, array []T, concurrency int, execute func(ctx context.Context, one T, executionNumber int) error) error {
	executor, ctx := errgroup.WithContext(ctx)
	executor.SetLimit(concurrency)

	for idx, one := range array {
		executor.Go(func() error {
			return execute(ctx, one, idx)
		})
	}

	return executor.Wait()
}

// RetryOnBackoff retries f up to attempts times with doubling backoff. olake's version
// additionally logs each attempt and gives up early on constants.ErrNonRetryable, neither of
// which the tests need.
func RetryOnBackoff(ctx context.Context, attempts int, sleep time.Duration, f func(ctx context.Context) error) (err error) {
	for cur := range attempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err = f(ctx); err == nil {
				return nil
			}
		}
		if attempts > 1 && cur != attempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
				sleep = sleep * 2
			}
		}
	}
	return err
}

func Combine(components ...string) string {
	parts := make([]string, 0, len(components))
	for _, str := range components {
		if str != "" {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, "_")
}

// CopyJSONWithEdit reads the JSON at src, applies edit, and writes the result to dst --
// used to derive a per-suite config from a shared base file without touching the base.
func CopyJSONWithEdit(src, dst string, edit func(map[string]interface{}) error) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", src, err)
	}
	doc, err := ParseJSONDoc(raw)
	if err != nil {
		return fmt.Errorf("failed to parse %s: %s", src, err)
	}
	if err := edit(doc); err != nil {
		return err
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %s", dst, err)
	}
	return WriteHostFile(dst, out)
}

// ParseJSONDoc decodes a JSON object keeping numbers as json.Number, so values the edit does not
// touch round-trip as their original literals instead of through float64 (which corrupts int64s
// beyond 2^53 and renders large values in scientific notation).
func ParseJSONDoc(raw []byte) (map[string]interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc map[string]interface{}
	return doc, dec.Decode(&doc)
}

// copyDirFiles copies every file in src into dst, replacing what is already there. Files only:
// a driver's data-format fixtures are a directory of their own, copied as their own source.
func copyDirFiles(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", src, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := CopyFile(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// SeedColumnsExcluded is the fixture-side guard for seed exclusion: it verifies every requested
// column is one the fixture knows how to leave out, so an unknown name fails loudly instead of
// silently seeding a column the baseline cannot survive.
func SeedColumnsExcluded(excluded, supported []string) (map[string]bool, error) {
	drop := make(map[string]bool, len(excluded))
	for _, col := range excluded {
		if !slices.Contains(supported, col) {
			return nil, fmt.Errorf("column %q cannot be excluded from the seed data; the fixture supports excluding only %s",
				col, strings.Join(supported, ", "))
		}
		drop[col] = true
	}
	return drop, nil
}
