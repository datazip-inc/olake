package writers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/encoder/parquetenc"
	"github.com/datazip-inc/olake/writers/roller"
	"github.com/datazip-inc/olake/writers/sink/localfs"
)

// ---- fakes -----------------------------------------------------------------

// plainWriter is a controllable RollingWriter (no committer). The PartitionedRollingWriter
// guarantees a single instance is only ever touched by one goroutine at a time,
// so its fields need no locking.
type plainWriter struct {
	key      string
	got      []*types.RawRecord // rows received, in order
	closed   bool
	writeErr error                           // returned from Write when set
	gate     func(ctx context.Context) error // optional hook to coordinate concurrency
}

func (w *plainWriter) Write(ctx context.Context, rows []*types.RawRecord) error {
	if w.gate != nil {
		if err := w.gate(ctx); err != nil {
			return err
		}
	}
	w.got = append(w.got, rows...)
	return w.writeErr
}

func (w *plainWriter) Close(context.Context) error {
	w.closed = true
	return nil
}

// committerWriter is a plainWriter that also implements committer.
type committerWriter struct {
	plainWriter
	commitErr error
	abortErr   error
	committed  bool
	aborted    bool
}

func (w *committerWriter) Commit(context.Context) error {
	w.committed = true
	return w.commitErr
}

func (w *committerWriter) Abort(context.Context) error {
	w.aborted = true
	return w.abortErr
}

// keyPartitioner routes by the record's Data["k"] field.
func keyPartitioner(_ string, rec *types.RawRecord) (Partition, error) {
	return Partition{Key: rec.Data["k"].(string)}, nil
}

// rows builds RawRecords; keys[i] becomes Data["k"], i becomes Data["i"].
func rows(keys ...string) []types.RawRecord {
	out := make([]types.RawRecord, len(keys))
	for i, k := range keys {
		out[i] = types.RawRecord{Data: map[string]any{"k": k, "i": i}, OlakeColumns: map[string]any{}}
	}
	return out
}

// registry collects the plainWriters a factory hands out so tests can inspect them.
type registry struct {
	mu      sync.Mutex
	created map[string]*plainWriter
	creates int
}

func newRegistry() *registry { return &registry{created: map[string]*plainWriter{}} }

func (r *registry) factory(p Partition) (RollingWriter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.creates++
	w := &plainWriter{key: p.Key}
	r.created[p.Key] = w
	return w, nil
}

// ---- routing ---------------------------------------------------------------

func TestWrite_RoutesByKeyPreservingOrder(t *testing.T) {
	reg := newRegistry()
	prw := New("", keyPartitioner, reg.factory, Config{})

	if err := prw.Write(context.Background(), rows("a", "b", "a", "b", "a")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	want := map[string][]int{"a": {0, 2, 4}, "b": {1, 3}}
	for key, idxs := range want {
		w, ok := reg.created[key]
		if !ok {
			t.Fatalf("no writer created for key %q", key)
		}
		if len(w.got) != len(idxs) {
			t.Fatalf("key %q: got %d rows, want %d", key, len(w.got), len(idxs))
		}
		for j, rec := range w.got {
			if rec.Data["i"].(int) != idxs[j] {
				t.Fatalf("key %q row %d: got record %v, want index %d", key, j, rec.Data["i"], idxs[j])
			}
		}
	}
}

func TestWrite_CreatesWriterOncePerKeyAcrossBatches(t *testing.T) {
	reg := newRegistry()
	d := New("", keyPartitioner, reg.factory, Config{})

	ctx := context.Background()
	if err := d.Write(ctx, rows("a", "b")); err != nil {
		t.Fatalf("Write batch 1: %v", err)
	}
	firstA := reg.created["a"]
	if err := d.Write(ctx, rows("a", "c", "a")); err != nil {
		t.Fatalf("Write batch 2: %v", err)
	}

	if reg.creates != 3 { // a, b, c — each created exactly once
		t.Fatalf("factory called %d times, want 3", reg.creates)
	}
	if reg.created["a"] != firstA {
		t.Fatalf("writer for key %q was recreated across batches", "a")
	}
	// "a" received its rows from both batches (indices 0 then 0,2).
	if len(firstA.got) != 3 {
		t.Fatalf("key %q: got %d rows across batches, want 3", "a", len(firstA.got))
	}
}

func TestWrite_Empty_NoWritersCreated(t *testing.T) {
	reg := newRegistry()
	d := New("", keyPartitioner, reg.factory, Config{})

	if err := d.Write(context.Background(), nil); err != nil {
		t.Fatalf("Write(nil): %v", err)
	}
	if err := d.Write(context.Background(), []types.RawRecord{}); err != nil {
		t.Fatalf("Write([]): %v", err)
	}
	if reg.creates != 0 {
		t.Fatalf("factory called %d times for empty input, want 0", reg.creates)
	}
}

// New must thread basePath to the Partitioner on every record. It was once dropped
// (New ignored its basePath arg), sending "" to every partitioner and collapsing all
// streams into the same root directory.
func TestWrite_ThreadsBasePathToPartitioner(t *testing.T) {
	reg := newRegistry()
	part := func(basePath string, rec *types.RawRecord) (Partition, error) {
		return Partition{Key: filepath.Join(basePath, rec.Data["k"].(string))}, nil
	}
	d := New("ns/tbl", part, reg.factory, Config{})

	if err := d.Write(context.Background(), rows("a")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := filepath.Join("ns/tbl", "a")
	if _, ok := reg.created[want]; !ok {
		got := make([]string, 0, len(reg.created))
		for k := range reg.created {
			got = append(got, k)
		}
		t.Fatalf("partition key %q not found (basePath not threaded); created keys: %v", want, got)
	}
}

// ---- concurrency -----------------------------------------------------------

// barrier blocks each arriving goroutine until n have arrived. If the PartitionedRollingWriter
// did not fan out, fewer than n goroutines arrive and callers block until the
// context deadline — turning a serial regression into a clean test failure.
type barrier struct {
	n     int
	mu    sync.Mutex
	count int
	ch    chan struct{}
}

func newBarrier(n int) *barrier { return &barrier{n: n, ch: make(chan struct{})} }

func (b *barrier) wait(ctx context.Context) error {
	b.mu.Lock()
	b.count++
	if b.count == b.n {
		close(b.ch)
	}
	b.mu.Unlock()
	select {
	case <-b.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWrite_FanOutIsConcurrent(t *testing.T) {
	const n = 4
	bar := newBarrier(n)
	factory := func(p Partition) (RollingWriter, error) {
		return &plainWriter{key: p.Key, gate: bar.wait}, nil
	}
	d := New("", keyPartitioner, factory, Config{Concurrency: n})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// One record per distinct key => n partitions. They must run concurrently to
	// all clear the barrier; otherwise this returns context.DeadlineExceeded.
	if err := d.Write(ctx, rows("a", "b", "c", "d")); err != nil {
		t.Fatalf("Write did not fan out (partitions ran serially): %v", err)
	}
}

// ---- error propagation -----------------------------------------------------

func TestWrite_PartitionErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	factory := func(p Partition) (RollingWriter, error) {
		w := &plainWriter{key: p.Key}
		if p.Key == "b" {
			w.writeErr = sentinel
		}
		return w, nil
	}
	d := New("", keyPartitioner, factory, Config{})

	err := d.Write(context.Background(), rows("a", "b"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Write error = %v, want it to wrap %v", err, sentinel)
	}
}

func TestWrite_PartitionerErrorPropagates(t *testing.T) {
	sentinel := errors.New("bad key")
	part := func(_ string, rec *types.RawRecord) (Partition, error) {
		if rec.Data["k"].(string) == "b" {
			return Partition{}, sentinel
		}
		return Partition{Key: rec.Data["k"].(string)}, nil
	}
	reg := newRegistry()
	d := New("", part, reg.factory, Config{})

	err := d.Write(context.Background(), rows("a", "b"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Write error = %v, want it to wrap %v", err, sentinel)
	}
}

func TestWrite_FactoryErrorPropagates(t *testing.T) {
	sentinel := errors.New("no writer")
	factory := func(p Partition) (RollingWriter, error) {
		return nil, sentinel
	}
	d := New("", keyPartitioner, factory, Config{})

	err := d.Write(context.Background(), rows("a"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Write error = %v, want it to wrap %v", err, sentinel)
	}
}

// ---- lifecycle: close / publish / abort ------------------------------------

func TestClose_FlushesAllPartitions(t *testing.T) {
	reg := newRegistry()
	d := New("", keyPartitioner, reg.factory, Config{})

	ctx := context.Background()
	if err := d.Write(ctx, rows("a", "b", "c")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if !reg.created[key].closed {
			t.Fatalf("partition %q was not closed", key)
		}
	}
}

// mixedFactory returns a committerWriter for keys in committers, else a plainWriter.
// It records both kinds so the test can assert which got Commit/Abort.
type mixedFactory struct {
	committers map[string]bool
	fin        map[string]*committerWriter
	plain      map[string]*plainWriter
	abortErrs  map[string]error
}

func (m *mixedFactory) make(p Partition) (RollingWriter, error) {
	if m.committers[p.Key] {
		w := &committerWriter{plainWriter: plainWriter{key: p.Key}, abortErr: m.abortErrs[p.Key]}
		m.fin[p.Key] = w
		return w, nil
	}
	w := &plainWriter{key: p.Key}
	m.plain[p.Key] = w
	return w, nil
}

func TestCommit_OnlyCommittersAreCommitted(t *testing.T) {
	mf := &mixedFactory{
		committers: map[string]bool{"a": true}, // "b" is a plain writer
		fin:        map[string]*committerWriter{},
		plain:      map[string]*plainWriter{},
		abortErrs:  map[string]error{},
	}
	d := New("", keyPartitioner, mf.make, Config{})

	ctx := context.Background()
	if err := d.Write(ctx, rows("a", "b")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := d.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !mf.fin["a"].committed {
		t.Fatalf("committer partition %q was not committed", "a")
	}
	// "b" (plain) is simply skipped — nothing to assert beyond no panic/error.
}

func TestAbort_BestEffortAttemptsAllAndJoinsErrors(t *testing.T) {
	errA := errors.New("abort a failed")
	errB := errors.New("abort b failed")
	mf := &mixedFactory{
		committers: map[string]bool{"a": true, "b": true},
		fin:        map[string]*committerWriter{},
		plain:      map[string]*plainWriter{},
		abortErrs:  map[string]error{"a": errA, "b": errB},
	}
	d := New("", keyPartitioner, mf.make, Config{})

	ctx := context.Background()
	if err := d.Write(ctx, rows("a", "b")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	err := d.Abort(ctx)
	// Both partitions must be attempted despite both failing...
	if !mf.fin["a"].aborted || !mf.fin["b"].aborted {
		t.Fatalf("abort was not attempted for every partition: a=%v b=%v", mf.fin["a"].aborted, mf.fin["b"].aborted)
	}
	// ...and both errors must be reported.
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("Abort error = %v, want it to join %v and %v", err, errA, errB)
	}
}

// ---- integration: SingleRoller + real Roller/Sink/Encoder ------------------

// TestSingleRoller_EndToEnd wires the PartitionedRollingWriter with the package's own
// SingleRoller over a real parquet encoder and a localfs sink, proving the layer
// composes with the lower seams and stays transactional across partitions.
func TestSingleRoller_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	partition := func(_ string, rec *types.RawRecord) (Partition, error) {
		return Partition{Key: rec.Data["region"].(string)}, nil
	}
	newWriter := func(p Partition) (RollingWriter, error) {
		sink := localfs.NewLocalFSSink(func(i int) string {
			return filepath.Join(dir, p.Key, fmt.Sprintf("part-%05d.parquet", i))
		})
		enc := parquetenc.New(schema, []parquet.WriterProperty{parquet.WithCompression(compress.Codecs.Snappy)}, nil)
		alloc := memory.NewGoAllocator()
		conv := func(rs []*types.RawRecord) (arrow.Record, error) {
			return roller.CreateArrowRecord(rs, alloc, schema)
		}
		return SingleRoller{
			Roller: roller.NewRoller(sink, enc, conv, roller.RollerConfig{}),
			Sink:   sink,
		}, nil
	}
	d := New("", partition, newWriter, Config{})

	ctx := context.Background()
	recs := make([]types.RawRecord, 6)
	for i := range recs {
		region := "us"
		if i%2 == 0 {
			region = "eu"
		}
		recs[i] = types.RawRecord{Data: map[string]any{"id": int64(i), "region": region}, OlakeColumns: map[string]any{}}
	}

	if err := d.Write(ctx, recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Transactional: nothing is committed yet, so no final .parquet exists.
	if got := finalFiles(t, dir); len(got) != 0 {
		t.Fatalf("expected 0 committed files before Commit, got %v", got)
	}

	if err := d.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got := finalFiles(t, dir)
	if len(got) != 2 { // one file per partition (us, eu), small data => no roll
		t.Fatalf("expected 2 committed files, got %d (%v)", len(got), got)
	}
	for _, f := range got {
		assertParquet(t, f)
	}
	// staging is clean: only the two final files remain.
	if total := allFiles(t, dir); len(total) != 2 {
		t.Fatalf("expected stage dir clean (2 files), found %d (%v)", len(total), total)
	}
}

func TestSingleRoller_AbortLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	partition := func(_ string, rec *types.RawRecord) (Partition, error) {
		return Partition{Key: rec.Data["region"].(string)}, nil
	}
	newWriter := func(p Partition) (RollingWriter, error) {
		sink := localfs.NewLocalFSSink(func(i int) string {
			return filepath.Join(dir, p.Key, fmt.Sprintf("part-%05d.parquet", i))
		})
		enc := parquetenc.New(schema, nil, nil)
		alloc := memory.NewGoAllocator()
		conv := func(rs []*types.RawRecord) (arrow.Record, error) {
			return roller.CreateArrowRecord(rs, alloc, schema)
		}
		return SingleRoller{Roller: roller.NewRoller(sink, enc, conv, roller.RollerConfig{}), Sink: sink}, nil
	}
	d := New("", partition, newWriter, Config{})

	ctx := context.Background()
	recs := []types.RawRecord{
		{Data: map[string]any{"id": int64(1), "region": "us"}, OlakeColumns: map[string]any{}},
		{Data: map[string]any{"id": int64(2), "region": "eu"}, OlakeColumns: map[string]any{}},
	}
	if err := d.Write(ctx, recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Abort(ctx); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if total := allFiles(t, dir); len(total) != 0 {
		t.Fatalf("expected no files after Abort, found %d (%v)", len(total), total)
	}
}

// ---- helpers ---------------------------------------------------------------

func finalFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	for _, f := range allFiles(t, dir) {
		if filepath.Ext(f) == ".parquet" {
			out = append(out, f)
		}
	}
	return out
}

func allFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !de.IsDir() {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", dir, err)
	}
	return out
}

func assertParquet(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if len(b) < 4 || string(b[:4]) != "PAR1" {
		t.Fatalf("%q is not a parquet file", path)
	}
}
