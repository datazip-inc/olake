package driver

import (
	"database/sql"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

func TestLimitOffsetChunks(t *testing.T) {
	tests := []struct {
		name           string
		approxRowCount int64
		chunkSize      int64
		expected       []types.Chunk
	}{
		{
			name:           "partial final chunk",
			approxRowCount: 250,
			chunkSize:      100,
			expected: []types.Chunk{
				{Min: nil, Max: "100"},
				{Min: "100", Max: "200"},
				{Min: "200", Max: "300"},
				{Min: "300", Max: nil},
			},
		},
		{
			name:           "table smaller than one chunk",
			approxRowCount: 50,
			chunkSize:      100,
			expected: []types.Chunk{
				{Min: nil, Max: "100"},
				{Min: "100", Max: nil},
			},
		},
		{
			name:           "exact chunk boundary",
			approxRowCount: 200,
			chunkSize:      100,
			expected: []types.Chunk{
				{Min: nil, Max: "100"},
				{Min: "100", Max: "200"},
				{Min: "200", Max: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertChunksEqual(t, limitOffsetChunks(tt.approxRowCount, tt.chunkSize), tt.expected)
		})
	}
}

func TestSplitEvenlyForInt(t *testing.T) {
	tests := []struct {
		name      string
		bounds    *NumericChunkBounds
		expected  []types.Chunk
		wantError bool
	}{
		{
			name: "even positive range",
			bounds: &NumericChunkBounds{
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
		{
			name: "negative range",
			bounds: &NumericChunkBounds{
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
		{
			name: "step larger than range",
			bounds: &NumericChunkBounds{
				MinBoundary: 10,
				MaxBoundary: 20,
				ChunkStep:   50,
			},
			expected: []types.Chunk{
				{Min: nil, Max: "10"},
				{Min: "10", Max: nil},
			},
		},
		{
			name: "zero step",
			bounds: &NumericChunkBounds{
				MinBoundary: 1,
				MaxBoundary: 2,
				ChunkStep:   0,
			},
			wantError: true,
		},
		{
			name: "overflow",
			bounds: &NumericChunkBounds{
				MinBoundary: math.MaxInt64 - 1,
				MaxBoundary: math.MaxInt64,
				ChunkStep:   2,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := splitEvenlyForInt(tt.bounds)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("split evenly for int: %s", err)
			}
			assertChunksEqual(t, chunks, tt.expected)
		})
	}
}

func TestPrimaryKeyChunkArgs(t *testing.T) {
	tests := []struct {
		name          string
		currentValue  any
		pkColumnCount int
		expected      []any
	}{
		{name: "single key", currentValue: 42, pkColumnCount: 1, expected: []any{"42"}},
		{
			name:          "composite key",
			currentValue:  "tenant_1,42,abc",
			pkColumnCount: 3,
			expected:      []any{"tenant_1", "tenant_1", "42", "tenant_1", "42", "abc"},
		},
		{
			name:          "fewer values than columns",
			currentValue:  "tenant_1,42",
			pkColumnCount: 3,
			expected:      []any{"tenant_1", "tenant_1", "42", "tenant_1", "42"},
		},
		{name: "no columns", currentValue: "tenant_1", pkColumnCount: 0, expected: []any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := primaryKeyChunkArgs(tt.currentValue, tt.pkColumnCount); !reflect.DeepEqual(actual, tt.expected) {
				t.Fatalf("expected args %v, got %v", tt.expected, actual)
			}
		})
	}
}

func TestExpectedStringChunkCount(t *testing.T) {
	tests := []struct {
		name            string
		approxTableSize int64
		expected        int64
	}{
		{name: "empty estimate", approxTableSize: 0, expected: 1},
		{name: "smaller than target file", approxTableSize: constants.EffectiveParquetSize - 1, expected: 1},
		{name: "exact target file", approxTableSize: constants.EffectiveParquetSize, expected: 1},
		{name: "multiple target files", approxTableSize: constants.EffectiveParquetSize*2 + 1, expected: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := expectedStringChunkCount(tt.approxTableSize); actual != tt.expected {
				t.Fatalf("expected %d chunks, got %d", tt.expected, actual)
			}
		})
	}
}

func TestStringChunkStepSizeUsesCeilDivision(t *testing.T) {
	bounds := &StringChunkBounds{
		minEncodedBigIntValue: big.NewInt(1),
		maxEncodedBigIntValue: big.NewInt(10),
	}

	if actual := stringChunkStepSize(bounds, 4); actual.Cmp(big.NewInt(3)) != 0 {
		t.Fatalf("expected step size 3, got %s", actual)
	}
}

func TestStringChunkCandidates(t *testing.T) {
	bounds := isStringSupportedPK("aa", "az", validNullInt64(2), "varchar")
	if bounds == nil {
		t.Fatal("expected supported string bounds")
	}
	step := stringChunkStepSize(bounds, 4)
	originalStep := new(big.Int).Set(step)

	candidates := stringChunkCandidates(bounds, step, 4, 1)
	if len(candidates) == 0 {
		t.Fatal("expected string chunk candidates")
	}
	if candidates[0] != bounds.MinPadded {
		t.Fatalf("expected first candidate %q, got %q", bounds.MinPadded, candidates[0])
	}
	if candidates[len(candidates)-1] != bounds.MaxPadded {
		t.Fatalf("expected final candidate %q, got %q", bounds.MaxPadded, candidates[len(candidates)-1])
	}
	if step.Cmp(originalStep) != 0 {
		t.Fatalf("candidate generation mutated step size: expected %s, got %s", originalStep, step)
	}
}

func TestChunksFromBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		boundaries []string
		expected   []types.Chunk
	}{
		{name: "empty", boundaries: nil, expected: nil},
		{
			name:       "single boundary",
			boundaries: []string{"m"},
			expected: []types.Chunk{
				{Min: nil, Max: "m"},
				{Min: "m", Max: nil},
			},
		},
		{
			name:       "multiple boundaries",
			boundaries: []string{"aa", "mm", "zz"},
			expected: []types.Chunk{
				{Min: nil, Max: "aa"},
				{Min: "aa", Max: "mm"},
				{Min: "mm", Max: "zz"},
				{Min: "zz", Max: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertChunksEqual(t, chunksFromBoundaries(tt.boundaries), tt.expected)
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
		wantBounds     bool
	}{
		{name: "supported bigint", minVal: 1, maxVal: 100, approxRowCount: 100, chunkSize: 10, dataType: "BIGINT", wantBounds: true},
		{name: "unsupported type", minVal: 1, maxVal: 100, approxRowCount: 100, chunkSize: 10, dataType: "varchar"},
		{name: "invalid minimum", minVal: "not-a-number", maxVal: 100, approxRowCount: 100, chunkSize: 10, dataType: "bigint"},
		{name: "invalid maximum", minVal: 1, maxVal: "not-a-number", approxRowCount: 100, chunkSize: 10, dataType: "bigint"},
		{name: "sparse distribution", minVal: 1, maxVal: 10000, approxRowCount: 10, chunkSize: 10, dataType: "bigint"},
		{name: "zero row estimate", minVal: 1, maxVal: 100, approxRowCount: 0, chunkSize: 10, dataType: "bigint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := isNumericAndEvenDistributed(tt.minVal, tt.maxVal, tt.approxRowCount, tt.chunkSize, tt.dataType)
			if (bounds != nil) != tt.wantBounds {
				t.Fatalf("expected bounds=%t, got %v", tt.wantBounds, bounds)
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
		wantBounds    bool
	}{
		{name: "supported varchar", minVal: "aa", maxVal: "az", dataMaxLength: validNullInt64(3), dataType: "varchar", wantBounds: true},
		{name: "supported char", minVal: "a", maxVal: "z", dataMaxLength: validNullInt64(1), dataType: "char", wantBounds: true},
		{name: "unsupported type", minVal: "aa", maxVal: "az", dataMaxLength: validNullInt64(2), dataType: "text"},
		{name: "missing max length", minVal: "aa", maxVal: "az", dataType: "varchar"},
		{name: "unsupported character", minVal: "aa", maxVal: "az\n", dataMaxLength: validNullInt64(3), dataType: "varchar"},
		{name: "non increasing range", minVal: "az", maxVal: "aa", dataMaxLength: validNullInt64(2), dataType: "varchar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := isStringSupportedPK(tt.minVal, tt.maxVal, tt.dataMaxLength, tt.dataType)
			if (bounds != nil) != tt.wantBounds {
				t.Fatalf("expected bounds=%t, got %v", tt.wantBounds, bounds)
			}
		})
	}
}

func TestCharsetEncodingRoundTrip(t *testing.T) {
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
		{value: "ab", maxLength: 4, expected: "ab00"},
		{value: "abcd", maxLength: 4, expected: "abcd"},
		{value: "abcdef", maxLength: 4, expected: "abcdef"},
		{value: "a", maxLength: 2, expected: "a0"},
	}

	for _, tt := range tests {
		if actual := padRightWithZeroes(tt.value, tt.maxLength); actual != tt.expected {
			t.Fatalf("pad %q to %d: expected %q, got %q", tt.value, tt.maxLength, tt.expected, actual)
		}
	}
}

func TestCondenseStrings(t *testing.T) {
	tests := []struct {
		name       string
		candidates []string
		expected   int64
		want       []string
	}{
		{name: "already small enough", candidates: []string{"a", "b"}, expected: 3, want: []string{"a", "b"}},
		{name: "single boundary", candidates: []string{"a", "b", "c"}, expected: 1, want: []string{"a"}},
		{
			name:       "balanced subset",
			candidates: []string{"0", "1", "2", "3", "4", "5", "6"},
			expected:   4,
			want:       []string{"0", "2", "4", "6"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := condenseStrings(tt.candidates, tt.expected); !reflect.DeepEqual(actual, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, actual)
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
