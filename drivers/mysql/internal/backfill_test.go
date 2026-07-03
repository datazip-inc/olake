package driver

import (
	"database/sql"
	"math"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake/types"
)

func TestSplitEvenlyForInt(t *testing.T) {
	tests := []struct {
		name        string
		input       *NumericChunkBounds
		expected    []types.Chunk
		expectError bool
	}{
		// evenly divides a positive integer range into fixed-width chunks
		{
			name: "even positive range",
			input: &NumericChunkBounds{
				MinBoundary: 0,
				MaxBoundary: 100,
				ChunkStep:   25,
			},
			expected: []types.Chunk{
				{Min: nil, Max: "0"},
				{Min: "0", Max: "25"},
				{Min: "25", Max: "50"},
				{Min: "50", Max: "75"},
				{Min: "75", Max: "100"},
				{Min: "100", Max: nil},
			},
		},
		// supports ranges that cross from negative values to positive values
		{
			name: "negative range",
			input: &NumericChunkBounds{
				MinBoundary: -10,
				MaxBoundary: 10,
				ChunkStep:   10,
			},
			expected: []types.Chunk{
				{Min: nil, Max: "-10"},
				{Min: "-10", Max: "0"},
				{Min: "0", Max: "10"},
				{Min: "10", Max: nil},
			},
		},
		// creates only open-ended edge chunks when step size is larger than the range
		{
			name: "step larger than range",
			input: &NumericChunkBounds{
				MinBoundary: 10,
				MaxBoundary: 20,
				ChunkStep:   50,
			},
			expected: []types.Chunk{
				{Min: nil, Max: "10"},
				{Min: "10", Max: nil},
			},
		},
		// rejects a zero step to avoid a non-progressing chunk loop
		{
			name: "zero step",
			input: &NumericChunkBounds{
				MinBoundary: 1,
				MaxBoundary: 2,
				ChunkStep:   0,
			},
			expectError: true,
		},
		// detects int64 overflow while calculating the next chunk boundary
		{
			name: "overflow",
			input: &NumericChunkBounds{
				MinBoundary: math.MaxInt64 - 1,
				MaxBoundary: math.MaxInt64,
				ChunkStep:   2,
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := splitEvenlyForInt(tc.input)
			if tc.expectError {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("split evenly for int: %s", err)
			}
			assertChunksEqual(t, chunks, tc.expected)
		})
	}
}

func TestIsNumericAndEvenDistributed(t *testing.T) {
	tests := []struct {
		name           string
		minVal         any
		maxVal         any
		approxRowCount int64
		chunkSize      int64
		dataType       string
		expected       bool
	}{
		// accepts numeric primary keys when the estimated distribution is even enough
		{name: "supported bigint", minVal: 1, maxVal: 100, approxRowCount: 100, chunkSize: 10, dataType: "BIGINT", expected: true},
		// skips numeric chunking for non-numeric column types
		{name: "unsupported type", minVal: 1, maxVal: 100, approxRowCount: 100, chunkSize: 10, dataType: "varchar"},
		// skips numeric chunking when the minimum value cannot be parsed as int64
		{name: "invalid minimum", minVal: "not-a-number", maxVal: 100, approxRowCount: 100, chunkSize: 10, dataType: "bigint"},
		// skips numeric chunking when the maximum value cannot be parsed as int64
		{name: "invalid maximum", minVal: 1, maxVal: "not-a-number", approxRowCount: 100, chunkSize: 10, dataType: "bigint"},
		// skips numeric chunking for sparse key ranges that would create uneven chunks
		{name: "sparse distribution", minVal: 1, maxVal: 10002, approxRowCount: 10, chunkSize: 10, dataType: "bigint"},
		// skips numeric chunking when row-count statistics are unavailable
		{name: "zero row estimate", minVal: 1, maxVal: 100, approxRowCount: 0, chunkSize: 10, dataType: "bigint"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bounds := isNumericAndEvenDistributed(tc.minVal, tc.maxVal, tc.approxRowCount, tc.chunkSize, tc.dataType)
			if (bounds != nil) != tc.expected {
				t.Fatalf("expected bounds=%t, got %v", tc.expected, bounds)
			}
		})
	}
}

func TestIsStringSupportedPK(t *testing.T) {
	tests := []struct {
		name          string
		minVal        any
		maxVal        any
		dataMaxLength sql.NullInt64
		dataType      string
		expected      bool
	}{
		// accepts varchar primary keys whose min and max values are encodable
		{name: "supported varchar", minVal: "aa", maxVal: "az", dataMaxLength: validNullInt64(3), dataType: "varchar", expected: true},
		// accepts fixed-length char primary keys when the encoded range is increasing
		{name: "supported char", minVal: "a", maxVal: "z", dataMaxLength: validNullInt64(1), dataType: "char", expected: true},
		// skips string chunking for unsupported string-like column types
		{name: "unsupported type", minVal: "aa", maxVal: "az", dataMaxLength: validNullInt64(2), dataType: "text"},
		// skips string chunking when MySQL does not provide a valid max length
		{name: "missing max length", minVal: "aa", maxVal: "az", dataType: "varchar"},
		// skips string chunking when boundaries contain characters outside the supported charset
		{name: "unsupported character", minVal: "aa", maxVal: "az\n", dataMaxLength: validNullInt64(3), dataType: "varchar"},
		// skips string chunking when the encoded min value is not lower than the max value
		{name: "non increasing range", minVal: "az", maxVal: "aa", dataMaxLength: validNullInt64(2), dataType: "varchar"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bounds := isStringSupportedPK(tc.minVal, tc.maxVal, tc.dataMaxLength, tc.dataType)
			if (bounds != nil) != tc.expected {
				t.Fatalf("expected bounds=%t, got %v", tc.expected, bounds)
			}
		})
	}
}

func TestCharsetEncodingRoundTrip(t *testing.T) {
	// supported charset values should encode to big.Int and decode back without loss
	values := []string{"", "0", "Az9", "a z", "~!@"}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			encoded, err := encodeCharsetStringToBigInt(value)
			if err != nil {
				t.Fatalf("encode %q: %s", value, err)
			}
			if actual := decodeBigIntToCharsetString(encoded); actual != value {
				t.Fatalf("expected %q after round trip, got %q", value, actual)
			}
		})
	}

	// unsupported characters should return an encoding error
	if _, err := encodeCharsetStringToBigInt("\n"); err == nil {
		t.Fatal("expected unsupported character error")
	}
}

func TestPadRightWithZeroes(t *testing.T) {
	tests := []struct {
		value     string
		maxLength int
		expected  string
	}{
		// pads shorter values with zeroes until the requested max length
		{value: "ab", maxLength: 4, expected: "ab00"},
		// leaves values unchanged when they already match the requested max length
		{value: "abcd", maxLength: 4, expected: "abcd"},
		// leaves values unchanged when they are longer than the requested max length
		{value: "abcdef", maxLength: 4, expected: "abcdef"},
		// pads a single-character value to the requested max length
		{value: "a", maxLength: 2, expected: "a0"},
	}

	for _, tc := range tests {
		if actual := padRightWithZeroes(tc.value, tc.maxLength); actual != tc.expected {
			t.Fatalf("pad %q to %d: expected %q, got %q", tc.value, tc.maxLength, tc.expected, actual)
		}
	}
}

func TestCondenseStrings(t *testing.T) {
	tests := []struct {
		name       string
		candidates []string
		chunkCount int64
		expected   []string
	}{
		// returns all candidate boundaries when fewer boundaries are available than requested
		{name: "already small enough", candidates: []string{"a", "b"}, chunkCount: 3, expected: []string{"a", "b"}},
		// keeps the first boundary when only one output boundary is requested
		{name: "single boundary", candidates: []string{"a", "b", "c"}, chunkCount: 1, expected: []string{"a"}},
		// picks an evenly-spaced subset from a larger candidate boundary list
		{
			name:       "balanced subset",
			candidates: []string{"0", "1", "2", "3", "4", "5", "6"},
			chunkCount: 4,
			expected:   []string{"0", "2", "4", "6"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if actual := condenseStrings(tc.candidates, tc.chunkCount); !reflect.DeepEqual(actual, tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, actual)
			}
		})
	}
}

func assertChunksEqual(t *testing.T, chunks *types.Set[types.Chunk], expected []types.Chunk) {
	t.Helper()
	if chunks.Len() != len(expected) {
		t.Fatalf("expected %d chunks %v, got %d chunks %v", len(expected), expected, chunks.Len(), chunks.Array())
	}
	for _, chunk := range expected {
		if !chunks.Exists(chunk) {
			t.Fatalf("expected chunk %v in %v", chunk, chunks.Array())
		}
	}
}

func validNullInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: true}
}
