package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/encoder/parquetenc"
	"github.com/datazip-inc/olake/writers/roller"
)

// fakeUploader captures uploaded objects in memory so tests can assert on them.
type fakeUploader struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeUploader() *fakeUploader {
	return &fakeUploader{objects: map[string][]byte{}}
}

func (f *fakeUploader) Upload(_ context.Context, key string, body io.Reader) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = data
	return nil
}

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func testRows(n int) []*types.RawRecord {
	rows := make([]*types.RawRecord, n)
	for i := range n {
		rows[i] = &types.RawRecord{
			Data:         map[string]any{"id": int64(i), "name": fmt.Sprintf("row-%d", i)},
			OlakeColumns: map[string]any{},
		}
	}
	return rows
}

func newTestS3Setup(t *testing.T, upload func(context.Context, string, io.Reader) error, cfg roller.RollerConfig) (*roller.Roller[*types.RawRecord], *S3Sink) {
	t.Helper()
	schema := testSchema()
	alloc := memory.NewGoAllocator()

	// Build the sink directly (NewS3Sink would stand up a real AWS client) and inject
	// the in-memory uploader through the uploadFn seam.
	s := &S3Sink{
		stageDir: t.TempDir(),
		key: func(i int) string {
			return fmt.Sprintf("table/part-%05d.parquet", i)
		},
		uploadFn: upload,
	}
	enc := parquetenc.New(schema, []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
	}, nil)
	convert := func(rows []*types.RawRecord) (arrow.Record, error) {
		return roller.CreateArrowRecord(rows, alloc, schema)
	}
	return roller.NewRoller(s, enc, convert, cfg), s
}

// Default rolling (512MB threshold) with a small input: nothing is uploaded until
// Commit (transactional), then exactly one object appears and staging is clean.
func TestS3Sink_DefaultRolling_CommitUploadsSingleFile(t *testing.T) {
	up := newFakeUploader()
	stageDir := t.TempDir()
	schema := testSchema()
	alloc := memory.NewGoAllocator()

	s := &S3Sink{
		stageDir: stageDir,
		key: func(i int) string {
			return fmt.Sprintf("table/part-%05d.parquet", i)
		},
		uploadFn: up.Upload,
	}
	enc := parquetenc.New(schema, nil, nil)
	convert := func(rows []*types.RawRecord) (arrow.Record, error) {
		return roller.CreateArrowRecord(rows, alloc, schema)
	}
	r := roller.NewRoller(s, enc, convert, roller.RollerConfig{}) // defaults: 512MB / 10k

	ctx := context.Background()
	if err := r.Write(ctx, testRows(1000)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Transactional: closing the roller stages but uploads nothing.
	if len(up.objects) != 0 {
		t.Fatalf("expected 0 uploads before Commit, got %d", len(up.objects))
	}

	if err := s.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(up.objects) != 1 {
		t.Fatalf("expected 1 uploaded object, got %d (%v)", len(up.objects), keys(up.objects))
	}
	if data := up.objects["table/part-00000.parquet"]; !bytes.HasPrefix(data, []byte("PAR1")) {
		t.Fatalf("uploaded object is not a parquet file (magic=%q)", first4(data))
	}
	assertStageDirEmpty(t, stageDir)
}

// A tiny MaxFileBytes forces a roll between size checks; Commit uploads them all.
func TestS3Sink_RollsIntoMultipleFiles(t *testing.T) {
	up := newFakeUploader()
	r, s := newTestS3Setup(t, up.Upload, roller.RollerConfig{
		MaxFileBytes:      1, // roll after every chunk
		SizeCheckInterval: 100,
	})

	ctx := context.Background()
	if err := r.Write(ctx, testRows(500)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(up.objects) != 0 {
		t.Fatalf("expected 0 uploads before Commit, got %d", len(up.objects))
	}
	if err := s.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 500 rows / 100 per chunk, rolling after each chunk => 5 files.
	if len(up.objects) != 5 {
		t.Fatalf("expected 5 uploaded objects, got %d (%v)", len(up.objects), keys(up.objects))
	}
	for k, data := range up.objects {
		if !bytes.HasPrefix(data, []byte("PAR1")) {
			t.Fatalf("object %q is not a parquet file", k)
		}
	}
}

// A file that spans several chunks holds one row group per chunk; reading it back
// must recover every row group and row. This is the normal (non-rolling) path and
// exercises the encoder's arrow write context being reused across row groups.
func TestS3Sink_MultipleRowGroupsSingleFile(t *testing.T) {
	up := newFakeUploader()
	r, s := newTestS3Setup(t, up.Upload, roller.RollerConfig{
		MaxFileBytes:      1 << 30, // 1 GiB: effectively never roll
		SizeCheckInterval: 100,     // one row group per 100-row chunk
	})

	ctx := context.Background()
	if err := r.Write(ctx, testRows(500)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if len(up.objects) != 1 {
		t.Fatalf("expected 1 object, got %d (%v)", len(up.objects), keys(up.objects))
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(up.objects["table/part-00000.parquet"]))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer rdr.Close()

	if got := rdr.NumRowGroups(); got != 5 { // 500 rows / 100 per chunk
		t.Fatalf("expected 5 row groups, got %d", got)
	}
	if got := rdr.NumRows(); got != 500 {
		t.Fatalf("expected 500 rows total, got %d", got)
	}
}

// Abort after writing must upload nothing and remove all staged files.
func TestS3Sink_AbortUploadsNothing(t *testing.T) {
	up := newFakeUploader()
	r, s := newTestS3Setup(t, up.Upload, roller.RollerConfig{MaxFileBytes: 1, SizeCheckInterval: 100})

	ctx := context.Background()
	if err := r.Write(ctx, testRows(300)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Abort(ctx); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if len(up.objects) != 0 {
		t.Fatalf("expected 0 uploads after Abort, got %d", len(up.objects))
	}
	assertStageDirEmpty(t, s.stageDir)
}

// An empty input must open no files and upload nothing (lazy open).
func TestS3Sink_NoData_NoFiles(t *testing.T) {
	up := newFakeUploader()
	r, s := newTestS3Setup(t, up.Upload, roller.RollerConfig{})

	ctx := context.Background()
	if err := r.Write(ctx, nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(up.objects) != 0 {
		t.Fatalf("expected 0 uploaded objects, got %d", len(up.objects))
	}
}

// failingUploader fails on one specific key and accepts the rest.
type failingUploader struct {
	failOnKey string
	uploaded  []string
}

func (f *failingUploader) Upload(_ context.Context, key string, body io.Reader) error {
	if key == f.failOnKey {
		return fmt.Errorf("simulated upload failure for %q", key)
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		return err
	}
	f.uploaded = append(f.uploaded, key)
	return nil
}

// A Commit that fails partway must leave the unpublished staged files reclaimable:
// a follow-up Abort removes them, so nothing leaks under the stage dir.
func TestS3Sink_CommitPartialFailure_AbortCleansRemaining(t *testing.T) {
	up := &failingUploader{failOnKey: "table/part-00001.parquet"}
	r, s := newTestS3Setup(t, up.Upload, roller.RollerConfig{MaxFileBytes: 1, SizeCheckInterval: 100})

	ctx := context.Background()
	if err := r.Write(ctx, testRows(300)); err != nil { // 300/100 => 3 rolled files
		t.Fatalf("Write: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := s.Commit(ctx); err == nil {
		t.Fatalf("expected Commit to fail on the 2nd object")
	}
	// Only the first object was uploaded before the failure.
	if len(up.uploaded) != 1 || up.uploaded[0] != "table/part-00000.parquet" {
		t.Fatalf("expected only part-00000 uploaded, got %v", up.uploaded)
	}
	// Abort reclaims the remaining staged temps (the failed one + the un-attempted one).
	if err := s.Abort(ctx); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	assertStageDirEmpty(t, s.stageDir)
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func first4(b []byte) string {
	if len(b) < 4 {
		return string(b)
	}
	return string(b[:4])
}

// assertStageDirEmpty verifies the S3 sink removed its staging files
// after upload.
func assertStageDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", dir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected stage dir %q to be empty, found %d entries", dir, len(entries))
	}
}
