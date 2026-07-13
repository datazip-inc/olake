package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// The tests tree is a black-box test suite with no dependency on the olake Go module; the
// few facts it shares with the driver are part of the observable contract and are stated
// here directly.
const (
	// cdcTimestampColumn is the CDC metadata column every driver writes to the destination.
	cdcTimestampColumn = "_cdc_timestamp"
)

// skipCDCDrivers are the drivers that do not support CDC mode.
var skipCDCDrivers = []string{"oracle", "db2"}

// ternary returns a if cond, else b.
func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// average returns the average of the given values, 0 for an empty slice.
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// UnmarshalFile reads the JSON file at path into dest.
func UnmarshalFile(path string, dest any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", path, err)
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("failed to unmarshal %s: %s", path, err)
	}
	return nil
}

// writeJSONFile marshals content and writes it to path, creating or truncating the file.
func writeJSONFile(path string, content any) error {
	data, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content for %s: %s", path, err)
	}
	return os.WriteFile(path, data, 0644)
}

// Concurrent runs execute for every element of array with the given concurrency limit,
// returning the first error (and canceling the shared context).
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

// RetryOnBackoff retries f up to attempts times, sleeping between tries.
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
		if cur < attempts-1 {
			time.Sleep(sleep)
		}
	}
	return err
}

// normalizedEqual compares two JSON documents ignoring whitespace and ordering (character
// multiset comparison, matching the discover-output check used since the original harness).
func normalizedEqual(strune1, strune2 string) bool {
	normalize := func(s string) (string, error) {
		// Slice out exactly from the first '{' to the last '}'
		start := strings.IndexRune(s, '{')
		end := strings.LastIndex(s, "}")
		if start < 0 || end < 0 || start > end {
			return "", fmt.Errorf("no valid JSON object found")
		}
		core := s[start : end+1]
		// remove whitespace
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

// reformat lowercases key and replaces every non-alphanumeric symbol with '_', mirroring
// how the driver normalizes destination column names.
func reformat(key string) string {
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
