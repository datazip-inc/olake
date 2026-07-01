package parquet

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	pqgo "github.com/parquet-go/parquet-go"
)

// ---- scaffolding -----------------------------------------------------------

func newStream(normalized bool, partitionRegex string, cols map[string]types.DataType) *types.ConfiguredStream {
	st := types.NewStream("users", "public", nil)
	for name, dt := range cols {
		st.UpsertField(name, dt, true, false)
	}
	return &types.ConfiguredStream{
		StreamMetadata: types.StreamMetadata{Normalization: normalized, PartitionRegex: partitionRegex},
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
			p := newParquet(t, dir, stream, 64*1024, 500) // 64KB rolling

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
	p := newParquet(t, dir, stream, 16*1024, 500) // 16KB rolling

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
