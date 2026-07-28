// This file holds the small, dependency-light helpers the tests tree needs. Each one is a copy
// of an olake helper of the same name: the tests are a separate module and deliberately do not
// depend on olake, so they carry their own copy rather than sharing one through lib.
package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

// UnmarshalFile reads the JSON file at path into dest. credsFile is accepted for signature
// compatibility with olake's helper; this copy never decrypts (the tests always pass false).
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

// FileLoggerWithPath marshals content to JSON and writes it to path, truncating any existing file.
func FileLoggerWithPath(content any, path string) error {
	if path == "" {
		return fmt.Errorf("path is not set")
	}
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %s", err)
	}
	if err := os.WriteFile(path, contentBytes, 0644); err != nil {
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
