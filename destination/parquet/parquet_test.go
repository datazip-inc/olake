package parquet

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	pqgo "github.com/parquet-go/parquet-go"
)

// ---- scaffolding -----------------------------------------------------------

func newStream(normalized bool, partitionRegex string, cols map[string]types.DataType) *types.ConfiguredStream {
	st := types.NewStream("users", "public", nil)
	for name, dt := range cols {
		st.UpsertField(name, dt, true, false)
	}
	return &types.ConfiguredStream{
		StreamMetadata: types.StreamMetadata{Normalization: &normalized, PartitionRegex: partitionRegex},
		Stream:         st,
	}
}

// newParquet builds a local-mode Parquet writer rooted at dir and runs Setup. When
// maxBytes > 0 it overrides the resolved roll threshold (in bytes) for deterministic
// rolling; when rollCheckInterval > 0 it overrides how often the on-disk size is checked
// (each check flushes a row group, so keep it well above 1 to avoid per-row row groups).
func newParquet(t *testing.T, dir string, stream *types.ConfiguredStream, maxBytes int64, rollCheckInterval int) *Parquet {
	t.Helper()
	p := &Parquet{}
	p.GetConfigRef() // initializes p.config
	p.config.Path = dir
	if _, _, err := p.Setup(context.Background(), stream, nil, &destination.Options{ThreadID: "test"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if maxBytes > 0 {
		p.maxFileBytes = maxBytes
	}
	if rollCheckInterval > 0 {
		p.checkIntervalForRoll = rollCheckInterval
	}
	return p
}

// writeBatch drives the writer the way WriterThread does: flatten, evolve, then write.
func writeBatch(t *testing.T, p *Parquet, records []types.RawRecord) {
	t.Helper()
	ctx := context.Background()
	evolution, buf, _, err := p.FlattenAndCleanData(ctx, records)
	if err != nil {
		t.Fatalf("FlattenAndCleanData: %v", err)
	}
	if evolution {
		if _, err := p.EvolveSchema(ctx, nil, nil); err != nil {
			t.Fatalf("EvolveSchema: %v", err)
		}
	}
	if err := p.Write(ctx, buf); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func rec(id int64, extra map[string]any) types.RawRecord {
	data := map[string]any{"id": id, "name": fmt.Sprintf("name-%d-padding-payload-to-grow-rows", id)}
	maps.Copy(data, extra)
	return types.RawRecord{
		Data: data,
		OlakeColumns: map[string]any{
			constants.OlakeID:        fmt.Sprintf("oid-%d", id),
			constants.OlakeTimestamp: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
			constants.OpType:         "i",
		},
	}
}

func recs(n int, extra func(i int) map[string]any) []types.RawRecord {
	out := make([]types.RawRecord, n)
	for i := range out {
		var e map[string]any
		if extra != nil {
			e = extra(i)
		}
		out[i] = rec(int64(i), e)
	}
	return out
}

// ---- parquet read helpers --------------------------------------------------

func parquetFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == "."+constants.ParquetFileExt {
			out = append(out, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return out
}

func parquetNumRows(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	pf, err := pqgo.OpenFile(f, info.Size())
	if err != nil {
		t.Fatalf("open parquet %s: %v", path, err)
	}
	return pf.NumRows()
}

func totalRows(t *testing.T, files []string) int64 {
	t.Helper()
	var total int64
	for _, f := range files {
		total += parquetNumRows(t, f)
	}
	return total
}

// ---- tests -----------------------------------------------------------------

// Rolling on a tiny threshold must produce several files whose rows sum to the input,
// for both the denormalized and normalized write paths.
func TestRollsIntoMultipleFiles(t *testing.T) {
	const batches, perBatch = 3, 2000
	for _, tc := range []struct {
		name       string
		normalized bool
	}{
		{"denormalized", false},
		{"normalized", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stream := newStream(tc.normalized, "", map[string]types.DataType{"id": types.Int64, "name": types.String})
			p := newParquet(t, dir, stream, 64*1024, 0) // 64KB rolling

			for range batches {
				writeBatch(t, p, recs(perBatch, nil))
			}
			if err := p.Close(context.Background(), nil); err != nil {
				t.Fatalf("Close: %v", err)
			}

			files := parquetFiles(t, dir)
			if len(files) < 2 {
				t.Fatalf("expected rolling into multiple files, got %d: %v", len(files), files)
			}

			if got, want := totalRows(t, files), int64(batches*perBatch); got != want {
				t.Fatalf("row count mismatch: got %d want %d across %d files", got, want, len(files))
			}
		})
	}
}

// Under the default (large) threshold a partition stays in a single file.
func TestNoRollSingleFile(t *testing.T) {
	dir := t.TempDir()
	stream := newStream(false, "", map[string]types.DataType{"id": types.Int64, "name": types.String})
	p := newParquet(t, dir, stream, 0, 0) // keep the resolved default (512MB)

	writeBatch(t, p, recs(500, nil))
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := parquetFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected exactly one file, got %d: %v", len(files), files)
	}
	if got := totalRows(t, files); got != 500 {
		t.Fatalf("row count mismatch: got %d want 500", got)
	}
}

// Each partition rolls independently; files land under per-partition directories and
// the total row count is preserved.
func TestRollsPerPartition(t *testing.T) {
	dir := t.TempDir()
	stream := newStream(false, "{region, default_region, }", map[string]types.DataType{
		"id": types.Int64, "name": types.String, "region": types.String,
	})
	p := newParquet(t, dir, stream, 16*1024, 0) // 16KB rolling

	region := func(i int) map[string]any {
		if i%2 == 0 {
			return map[string]any{"region": "us"}
		}
		return map[string]any{"region": "eu"}
	}
	const batches, perBatch = 2, 3000
	for range batches {
		writeBatch(t, p, recs(perBatch, region))
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := parquetFiles(t, dir)
	dirs := map[string]bool{}
	for _, f := range files {
		dirs[filepath.Base(filepath.Dir(f))] = true
	}
	if !dirs["us"] || !dirs["eu"] {
		t.Fatalf("expected us+eu partition dirs, got %v (files: %v)", dirs, files)
	}
	if got, want := totalRows(t, files), int64(batches*perBatch); got != want {
		t.Fatalf("row count mismatch: got %d want %d", got, want)
	}
}

// decimalRecords is one record whose price column holds text read as a decimal of dataType.
func decimalRecords(t *testing.T, dataType types.DataType, text string) []types.RawRecord {
	t.Helper()
	value, err := typeutils.ReformatDecimal(dataType, text)
	if err != nil {
		t.Fatalf("ReformatDecimal(%s): %v", text, err)
	}
	return []types.RawRecord{rec(1, map[string]any{"price": value})}
}

// TestWrite reads back the parquet column type and the value a reader such as spark decodes.
func TestWrite(t *testing.T) {
	testCases := []struct {
		name     string
		dataType types.DataType
		text     string
		node     pqgo.Node
		want     string
	}{
		{
			name:     "decimal up to 9 digits is an int32",
			dataType: types.DecimalOf(9, 2),
			text:     "-1234567.89",
			node:     pqgo.Decimal(2, 9, pqgo.Int32Type),
			want:     "-123456789",
		},
		{
			name:     "decimal up to 18 digits is an int64",
			dataType: types.DecimalOf(18, 4),
			text:     "12345678901234.5678",
			node:     pqgo.Decimal(4, 18, pqgo.Int64Type),
			want:     "123456789012345678",
		},
		{
			name:     "decimal past 18 digits is exact text",
			dataType: types.DecimalOf(20, 2),
			text:     "-123456789012345678.90",
			node:     pqgo.String(),
			want:     "-123456789012345678.90",
		},
		{
			name:     "the widest decimal is exact text",
			dataType: types.DecimalOf(38, 10),
			text:     "-1234567890123456789012345678.0123456789",
			node:     pqgo.String(),
			want:     "-1234567890123456789012345678.0123456789",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := newParquet(t, dir, newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String, "price": tc.dataType}), 0, 0)
			writeBatch(t, p, decimalRecords(t, tc.dataType, tc.text))
			if err := p.Close(context.Background(), nil); err != nil {
				t.Fatalf("Close: %v", err)
			}

			files := parquetFiles(t, dir)
			if len(files) != 1 {
				t.Fatalf("got %d files, want 1", len(files))
			}
			f, err := os.Open(files[0])
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			pf, err := pqgo.OpenFile(f, info.Size())
			if err != nil {
				t.Fatal(err)
			}
			leaf, found := pf.Schema().Lookup("price")
			if !found {
				t.Fatal("price column missing from the file")
			}
			if !pqgo.EqualNodes(pqgo.Optional(tc.node), leaf.Node) {
				t.Errorf("node %v, want %v", leaf.Node, tc.node)
			}
			rows := make([]pqgo.Row, 1)
			if n, _ := pf.RowGroups()[0].Rows().ReadRows(rows); n != 1 {
				t.Fatalf("read %d rows, want 1", n)
			}
			if got := rows[0][leaf.ColumnIndex].String(); got != tc.want {
				t.Errorf("value %s, want %s", got, tc.want)
			}
		})
	}
}

// TestFlattenAndCleanData checks when a decimal value evolves its column, after a first batch that
// adds the olake columns.
func TestFlattenAndCleanData(t *testing.T) {
	column := types.DecimalOf(10, 2)
	testCases := []struct {
		name      string
		valueType types.DataType
		text      string
		evolution bool
		want      types.DataType
	}{
		{
			name:      "a decimal of the column's type changes no schema",
			valueType: column,
			text:      "2.50",
			evolution: false,
			want:      column,
		},
		{
			name:      "a wider decimal widens the column",
			valueType: types.DecimalOf(12, 2),
			text:      "1234567890.12",
			evolution: true,
			want:      types.DecimalOf(12, 2),
		},
		{
			name:      "decimals of two scales meet at the decimal holding both",
			valueType: types.DecimalOf(10, 4),
			text:      "1.2345",
			evolution: true,
			want:      types.DecimalOf(12, 4),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := newParquet(t, t.TempDir(), newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String, "price": column}), 0, 0)
			if _, _, _, err := p.FlattenAndCleanData(context.Background(), decimalRecords(t, column, "1.50")); err != nil {
				t.Fatalf("first batch: %v", err)
			}
			evolution, _, _, err := p.FlattenAndCleanData(context.Background(), decimalRecords(t, tc.valueType, tc.text))
			if err != nil {
				t.Fatalf("FlattenAndCleanData: %v", err)
			}
			if evolution != tc.evolution || p.schema["price"].Type() != tc.want {
				t.Fatalf("evolution=%v type=%s, want evolution=%v type=%s", evolution, p.schema["price"].Type(), tc.evolution, tc.want)
			}
		})
	}
}

// TestWriteRejectsWrongFixedBinaryWidth writes a 16-byte value into a fixed_binary(8) column, which
// Write must reject with an error rather than let parquet-go panic.
func TestWriteRejectsWrongFixedBinaryWidth(t *testing.T) {
	stream := newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String, "code": types.FixedBinaryOf(8)})
	p := newParquet(t, t.TempDir(), stream, 0, 0)
	_, buf, _, err := p.FlattenAndCleanData(context.Background(), []types.RawRecord{rec(1, map[string]any{"code": make([]byte, 16)})})
	if err != nil {
		t.Fatalf("FlattenAndCleanData: %v", err)
	}
	want := "field[code]: fixed_binary(8) holds exactly 8 bytes, got 16"
	if err := p.Write(context.Background(), buf); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Write error = %v, want it to contain %q", err, want)
	}
}

// TestParquetGetPartitionedFilePath checks the directory each partition value renders to. Bytes
// render as escaped base64, the arrow writer's identity form, so a '/' in the encoding stays inside
// one directory.
func TestParquetGetPartitionedFilePath(t *testing.T) {
	testCases := []struct {
		name    string
		pattern string
		values  map[string]any
		want    string
	}{
		{
			name:    "bytes render as escaped base64",
			pattern: "/{pk, default, }",
			values:  map[string]any{"pk": []byte("k0000001")},
			want:    "azAwMDAwMDE%3D",
		},
		{
			name:    "a slash in the base64 stays in one directory",
			pattern: "/{pk, default, }",
			values:  map[string]any{"pk": []byte{0xff, 0xfe, 0xfb}},
			want:    "%2F%2F77",
		},
		{
			name:    "text renders as itself",
			pattern: "/{pk, default, }",
			values:  map[string]any{"pk": "k0000001"},
			want:    "k0000001",
		},
		{
			name:    "a missing value takes the default",
			pattern: "/{pk, default, }",
			values:  map[string]any{},
			want:    "default",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := newStream(false, tc.pattern, map[string]types.DataType{"pk": types.Binary})
			p := newParquet(t, t.TempDir(), stream, 0, 0)
			got := p.getPartitionedFilePath(tc.values, time.Unix(0, 0).UTC())
			if want := filepath.Join(p.basePath, tc.want); got != want {
				t.Fatalf("getPartitionedFilePath() = %q, want %q", got, want)
			}
		})
	}
}
