package driver

import (
	"database/sql"
	"math"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake/types"
)

func TestChunkSizeFromAvgRowSize(t *testing.T) {
	chunkSize, err := chunkSizeFromAvgRowSize([]uint8("128"))
	if err != nil {
		t.Fatalf("chunk size from avg row size: %s", err)
	}
	if chunkSize <= 0 {
		t.Fatalf("expected positive chunk size, got %d", chunkSize)
	}
}

func TestChunkColumnsUsesChunkColumnOverride(t *testing.T) {
	stream := &types.ConfiguredStream{Stream: types.NewStream("orders", "public", nil)}
	stream.GetStream().SourceDefinedPrimaryKey.Insert("id")
	stream.StreamMetadata.ChunkColumn = "created_at"

	columns := chunkColumns(stream)
	expected := []string{"created_at"}
	if !reflect.DeepEqual(columns, expected) {
		t.Fatalf("expected columns %v, got %v", expected, columns)
	}
}

func TestChunkColumnsSortsPrimaryKeys(t *testing.T) {
	stream := &types.ConfiguredStream{Stream: types.NewStream("orders", "public", nil)}
	stream.GetStream().SourceDefinedPrimaryKey.Insert("tenant_id")
	stream.GetStream().SourceDefinedPrimaryKey.Insert("id")

	columns := chunkColumns(stream)
	expected := []string{"id", "tenant_id"}
	if !reflect.DeepEqual(columns, expected) {
		t.Fatalf("expected columns %v, got %v", expected, columns)
	}
}

func TestChunkBoundsForColumnPrefersNumericStrategy(t *testing.T) {
	numeric, stringBounds := chunkBoundsForColumn(int64(1), int64(100), 100, 10, &mysqlColumnStats{
		dataType:      "bigint",
		dataMaxLength: sqlNullInt64(10),
	})

	if numeric == nil {
		t.Fatal("expected numeric chunk bounds")
	}
	if stringBounds != nil {
		t.Fatalf("expected no string bounds, got %v", stringBounds)
	}
}

func TestChunkBoundsForColumnFallsBackToStringStrategy(t *testing.T) {
	numeric, stringBounds := chunkBoundsForColumn("aa", "az", 100, 10, &mysqlColumnStats{
		dataType:      "varchar",
		dataMaxLength: sqlNullInt64(2),
	})

	if numeric != nil {
		t.Fatalf("expected no numeric bounds, got %v", numeric)
	}
	if stringBounds == nil {
		t.Fatal("expected string chunk bounds")
	}
}

func TestLimitOffsetChunks(t *testing.T) {
	expected := []types.Chunk{
		{Min: nil, Max: "100"},
		{Min: "100", Max: "200"},
		{Min: "200", Max: "300"},
		{Min: "300", Max: nil},
	}
	assertChunksEqual(t, limitOffsetChunks(250, 100), expected)
}

func TestSplitEvenlyForInt(t *testing.T) {
	chunks, err := splitEvenlyForInt(&NumericChunkBounds{
		MinBoundary: 0,
		MaxBoundary: 100,
		ChunkStep:   25,
	})
	if err != nil {
		t.Fatalf("split evenly for int: %s", err)
	}

	expected := []types.Chunk{
		{Min: nil, Max: "0"},
		{Min: "0", Max: "25"},
		{Min: "25", Max: "50"},
		{Min: "50", Max: "75"},
		{Min: "75", Max: "100"},
		{Min: "100", Max: nil},
	}
	assertChunksEqual(t, chunks, expected)
}

func TestSplitEvenlyForIntDetectsOverflow(t *testing.T) {
	_, err := splitEvenlyForInt(&NumericChunkBounds{
		MinBoundary: math.MaxInt64 - 1,
		MaxBoundary: math.MaxInt64,
		ChunkStep:   2,
	})
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestPrimaryKeyChunkArgs(t *testing.T) {
	args := primaryKeyChunkArgs("tenant_1,42,abc", 3)
	expected := []any{"tenant_1", "tenant_1", "42", "tenant_1", "42", "abc"}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("expected args %v, got %v", expected, args)
	}
}

func TestChunksFromBoundaries(t *testing.T) {
	expected := []types.Chunk{
		{Min: nil, Max: "aa"},
		{Min: "aa", Max: "mm"},
		{Min: "mm", Max: "zz"},
		{Min: "zz", Max: nil},
	}
	assertChunksEqual(t, chunksFromBoundaries([]string{"aa", "mm", "zz"}), expected)
}

func TestStringChunkCandidates(t *testing.T) {
	bounds := isStringSupportedPK("aa", "az", sqlNullInt64(2), "varchar")
	if bounds == nil {
		t.Fatal("expected supported string bounds")
	}

	candidates := stringChunkCandidates(bounds, stringChunkStepSize(bounds, 4), 4, 1)
	if len(candidates) == 0 {
		t.Fatal("expected string chunk candidates")
	}
	if candidates[0] != bounds.MinPadded {
		t.Fatalf("expected first candidate %q, got %q", bounds.MinPadded, candidates[0])
	}
	if candidates[len(candidates)-1] != bounds.MaxPadded {
		t.Fatalf("expected final candidate %q, got %q", bounds.MaxPadded, candidates[len(candidates)-1])
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

func sqlNullInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: true}
}
