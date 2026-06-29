package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/roller"
)

// ---- scaffolding -----------------------------------------------------------

func newStream(normalized bool, partitionRegex string, cols map[string]types.DataType) *types.ConfiguredStream {
	st := types.NewStream("users", "public", nil)
	for name, dt := range cols {
		st.UpsertField(name, dt, false, false)
	}
	return &types.ConfiguredStream{
		StreamMetadata: types.StreamMetadata{Normalization: normalized, PartitionRegex: partitionRegex},
		Stream:         st,
	}
}

// newParquet builds a Parquet writer rooted at dir and runs Setup.
func newParquet(t *testing.T, dir string, stream *types.ConfiguredStream, rollerCfg roller.RollerConfig) *Parquet {
	t.Helper()
	p := &Parquet{}
	p.GetConfigRef() // initialises p.config
	p.config.Path = dir
	p.rollerCfg = rollerCfg
	if _, _, err := p.Setup(context.Background(), stream, nil, &destination.Options{ThreadID: "test"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	return p
}

// writeBatch drives the writer the way the WriterThread does: flatten, evolve if
// needed, then write.
func writeBatch(t *testing.T, p *Parquet, records []types.RawRecord) error {
	t.Helper()
	ctx := context.Background()
	evolution, buf, _, err := p.FlattenAndCleanData(ctx, records)
	if err != nil {
		return err
	}
	if evolution {
		if _, err := p.EvolveSchema(ctx, nil, nil); err != nil {
			return err
		}
	}
	return p.Write(ctx, buf)
}

func rec(id int64, op string) types.RawRecord {
	return types.RawRecord{
		Data: map[string]any{"id": id, "name": fmt.Sprintf("name-%d", id)},
		OlakeColumns: map[string]any{
			constants.OlakeID:        fmt.Sprintf("oid-%d", id),
			constants.OlakeTimestamp: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
			constants.OpType:         op,
		},
	}
}

func recsAllInsert(n int) []types.RawRecord {
	out := make([]types.RawRecord, n)
	for i := range out {
		out[i] = rec(int64(i), "i")
	}
	return out
}

// ---- file/parquet helpers --------------------------------------------------

func parquetFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	walk(t, dir, func(path string) {
		if filepath.Ext(path) == ".parquet" {
			out = append(out, path)
		}
	})
	return out
}

func allFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	walk(t, dir, func(path string) { out = append(out, path) })
	return out
}

func partitionDirs(t *testing.T, dir string) map[string]bool {
	t.Helper()
	dirs := map[string]bool{}
	for _, f := range parquetFiles(t, dir) {
		dirs[filepath.Dir(f)] = true
	}
	return dirs
}

func walk(t *testing.T, dir string, fn func(path string)) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !de.IsDir() {
			fn(path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", dir, err)
	}
}

func openParquet(t *testing.T, path string) (*file.Reader, func()) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %q: %v", path, err)
	}
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()
		t.Fatalf("parquet reader %q: %v", path, err)
	}
	return rdr, func() { rdr.Close(); f.Close() }
}

func columnLogicalType(t *testing.T, path, col string) schema.LogicalType {
	t.Helper()
	rdr, done := openParquet(t, path)
	defer done()
	sc := rdr.MetaData().Schema
	for i := 0; i < sc.NumColumns(); i++ {
		if sc.Column(i).Name() == col {
			return sc.Column(i).LogicalType()
		}
	}
	t.Fatalf("column %q not found in %q", col, path)
	return nil
}

func columnNames(t *testing.T, path string) map[string]bool {
	t.Helper()
	rdr, done := openParquet(t, path)
	defer done()
	sc := rdr.MetaData().Schema
	names := map[string]bool{}
	for i := 0; i < sc.NumColumns(); i++ {
		names[sc.Column(i).Name()] = true
	}
	return names
}

// readStringColumn returns every value of a string column across all rolled files
// under dir, in file order.
func readStringColumn(t *testing.T, dir, col string) []string {
	t.Helper()
	var out []string
	for _, path := range parquetFiles(t, dir) {
		rdr, done := openParquet(t, path)
		ar, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			done()
			t.Fatalf("pqarrow reader: %v", err)
		}
		tbl, err := ar.ReadTable(context.Background())
		if err != nil {
			done()
			t.Fatalf("read table: %v", err)
		}
		idx := -1
		for i := 0; i < int(tbl.NumCols()); i++ {
			if tbl.Schema().Field(i).Name == col {
				idx = i
				break
			}
		}
		if idx >= 0 {
			for _, chunk := range tbl.Column(idx).Data().Chunks() {
				sa := chunk.(*array.String)
				for r := 0; r < sa.Len(); r++ {
					out = append(out, sa.Value(r))
				}
			}
		}
		tbl.Release()
		done()
	}
	return out
}

func totalRows(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	for _, path := range parquetFiles(t, dir) {
		rdr, done := openParquet(t, path)
		total += rdr.NumRows()
		done()
	}
	return total
}

// ---- tests -----------------------------------------------------------------

// Denormalized: rows land in a "data" column kept as the parquet JSON logical type
// (fidelity vs the legacy parquet.JSON()), op-type "i" is normalised to "c", and
// the system columns are present.
func TestParquet_Denormalized_LocalEndToEnd(t *testing.T) {
	dir := t.TempDir()
	p := newParquet(t, dir, newStream(false, "", nil), roller.RollerConfig{})

	if err := writeBatch(t, p, recsAllInsert(20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	files := parquetFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 parquet file, got %d (%v)", len(files), files)
	}
	if got := totalRows(t, dir); got != 20 {
		t.Fatalf("expected 20 rows, got %d", got)
	}

	names := columnNames(t, files[0])
	for _, want := range []string{constants.StringifiedData, constants.OlakeID, constants.OlakeTimestamp, constants.OpType} {
		if !names[want] {
			t.Fatalf("missing column %q; have %v", want, names)
		}
	}
	if lt := columnLogicalType(t, files[0], constants.StringifiedData); !lt.Equals(schema.JSONLogicalType{}) {
		t.Fatalf("data column logical type = %s, want JSON", lt)
	}

	for _, op := range readStringColumn(t, dir, constants.OpType) {
		if op != "c" {
			t.Fatalf("op-type %q not normalised to \"c\"", op)
		}
	}
}

// Setup must stand up the control-plane S3 sink when bucket+region are configured;
// it drives Check (S3 write test) and DropStreams. A refactor once dropped this, so
// S3 mode silently fell back to local.
func TestParquet_Setup_InitsS3SinkWhenConfigured(t *testing.T) {
	p := &Parquet{}
	p.GetConfigRef()
	p.config.Bucket = "test-bucket"
	p.config.Region = "us-east-1"
	p.config.Path = t.TempDir()

	if _, _, err := p.Setup(context.Background(), newStream(false, "", nil), nil, &destination.Options{ThreadID: "test"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if p.s3Sink == nil {
		t.Fatal("s3Sink was not initialized despite bucket+region being set")
	}
}

// Normalized: typed columns are written (no stringified "data" column).
func TestParquet_Normalized_LocalEndToEnd(t *testing.T) {
	dir := t.TempDir()
	stream := newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String})
	p := newParquet(t, dir, stream, roller.RollerConfig{})

	if err := writeBatch(t, p, recsAllInsert(15)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	files := parquetFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 parquet file, got %d", len(files))
	}
	if got := totalRows(t, dir); got != 15 {
		t.Fatalf("expected 15 rows, got %d", got)
	}
	names := columnNames(t, files[0])
	for _, want := range []string{"id", "name", constants.OlakeID, constants.OpType} {
		if !names[want] {
			t.Fatalf("missing column %q; have %v", want, names)
		}
	}
	if names[constants.StringifiedData] {
		t.Fatalf("normalized output must not contain a stringified %q column", constants.StringifiedData)
	}
}

// A tiny MaxFileBytes forces the partition to roll into several files.
func TestParquet_RollsIntoMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	p := newParquet(t, dir, newStream(false, "", nil), roller.RollerConfig{MaxFileBytes: 1, SizeCheckInterval: 5})

	if err := writeBatch(t, p, recsAllInsert(50)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	if files := parquetFiles(t, dir); len(files) < 2 {
		t.Fatalf("expected multiple rolled files, got %d (%v)", len(files), files)
	}
	if got := totalRows(t, dir); got != 50 {
		t.Fatalf("expected 50 rows across rolled files, got %d", got)
	}
}

// A partition regex routes records into separate partition directories.
func TestParquet_PartitionsByRegex(t *testing.T) {
	dir := t.TempDir()
	p := newParquet(t, dir, newStream(false, "/{region,unknown,}", nil), roller.RollerConfig{})

	records := make([]types.RawRecord, 10)
	for i := range records {
		r := rec(int64(i), "c")
		if i%2 == 0 {
			r.Data["region"] = "us"
		} else {
			r.Data["region"] = "eu"
		}
		records[i] = r
	}
	if err := writeBatch(t, p, records); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	if dirs := partitionDirs(t, dir); len(dirs) < 2 {
		t.Fatalf("expected >= 2 partition dirs, got %d (%v)", len(dirs), dirs)
	}
	if got := totalRows(t, dir); got != 10 {
		t.Fatalf("expected 10 rows total, got %d", got)
	}
}

// A cancelled context at Close must publish nothing: staged files are aborted.
func TestParquet_CancelledClose_PublishesNothing(t *testing.T) {
	dir := t.TempDir()
	p := newParquet(t, dir, newStream(false, "", nil), roller.RollerConfig{MaxFileBytes: 1, SizeCheckInterval: 5})

	if err := writeBatch(t, p, recsAllInsert(30)); err != nil {
		t.Fatalf("write: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Close(ctx, nil); err != nil {
		t.Fatalf("close (cancelled): %v", err)
	}
	if files := allFiles(t, dir); len(files) != 0 {
		t.Fatalf("expected no files after cancelled Close, found %d (%v)", len(files), files)
	}
}

// A write failure (an un-marshalable value in denormalized mode) makes Close abort
// instead of publish, so nothing is left behind.
func TestParquet_WriteFailure_AbortsOnClose(t *testing.T) {
	dir := t.TempDir()
	p := newParquet(t, dir, newStream(false, "", nil), roller.RollerConfig{})

	bad := types.RawRecord{
		Data: map[string]any{"oops": make(chan int)}, // json.Marshal fails
		OlakeColumns: map[string]any{
			constants.OlakeID:        "oid-bad",
			constants.OlakeTimestamp: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
			constants.OpType:         "c",
		},
	}
	if err := writeBatch(t, p, []types.RawRecord{bad}); err == nil {
		t.Fatalf("expected write to fail on un-marshalable data")
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}
	if files := allFiles(t, dir); len(files) != 0 {
		t.Fatalf("expected no files after a failed write, found %d (%v)", len(files), files)
	}
}

// Normalized schema evolution: a second batch with a new column seals the first
// generation and starts a new one; Close publishes files from both, with all rows.
func TestParquet_NormalizedSchemaEvolution(t *testing.T) {
	dir := t.TempDir()
	stream := newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String})
	p := newParquet(t, dir, stream, roller.RollerConfig{})

	if err := writeBatch(t, p, recsAllInsert(10)); err != nil {
		t.Fatalf("write batch 1: %v", err)
	}

	// second batch introduces a new column -> evolution
	batch2 := make([]types.RawRecord, 8)
	for i := range batch2 {
		r := rec(int64(100+i), "c")
		r.Data["extra"] = fmt.Sprintf("x%d", i)
		batch2[i] = r
	}
	if err := writeBatch(t, p, batch2); err != nil {
		t.Fatalf("write batch 2: %v", err)
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	if files := parquetFiles(t, dir); len(files) < 2 {
		t.Fatalf("expected files from two schema generations, got %d (%v)", len(files), files)
	}
	if got := totalRows(t, dir); got != 18 {
		t.Fatalf("expected 18 rows across generations, got %d", got)
	}
}
