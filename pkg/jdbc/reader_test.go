package jdbc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	sql.Register("olake_jdbc_test", mockDriver{})
}

type mockDriver struct{}

func (mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{rows: parseMockDSN(name)}, nil
}

type mockConn struct {
	rows *mockDriverRows
}

func (c *mockConn) Prepare(_ string) (driver.Stmt, error) {
	return &mockStmt{rows: c.rows}, nil
}

func (c *mockConn) Close() error { return nil }

func (c *mockConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not supported in mock driver")
}

type mockStmt struct {
	rows *mockDriverRows
}

func (s *mockStmt) Close() error { return nil }

func (s *mockStmt) NumInput() int { return 0 }

func (s *mockStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("exec not supported in mock driver")
}

func (s *mockStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.rows.clone(), nil
}

type mockDriverRows struct {
	mu         sync.Mutex
	cols       []string
	data       [][]driver.Value
	idx        int
	closed     bool
	rowsErr    error
	closeCount *atomic.Int32
}

func (r *mockDriverRows) Columns() []string { return r.cols }

func (r *mockDriverRows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.closeCount != nil {
		r.closeCount.Add(1)
	}
	return nil
}

func (r *mockDriverRows) Next(dest []driver.Value) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rowsErr != nil && r.idx > 0 {
		return r.rowsErr
	}
	if r.idx >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.idx])
	r.idx++
	return nil
}

func (r *mockDriverRows) clone() *mockDriverRows {
	r.mu.Lock()
	defer r.mu.Unlock()
	return &mockDriverRows{
		cols:       append([]string(nil), r.cols...),
		data:       append([][]driver.Value(nil), r.data...),
		rowsErr:    r.rowsErr,
		closeCount: r.closeCount,
	}
}

type mockDSN struct {
	cols       []string
	data       [][]driver.Value
	rowsErr    error
	closeCount *atomic.Int32
}

var mockDSNRegistry sync.Map

func registerMockDSN(dsn string, cfg mockDSN) {
	mockDSNRegistry.Store(dsn, cfg)
}

func parseMockDSN(dsn string) *mockDriverRows {
	if raw, ok := mockDSNRegistry.Load(dsn); ok {
		cfg := raw.(mockDSN)
		return &mockDriverRows{cols: cfg.cols, data: cfg.data, rowsErr: cfg.rowsErr, closeCount: cfg.closeCount}
	}
	return &mockDriverRows{}
}

func openMockDB(t *testing.T, dsn string, cfg mockDSN) (*sql.DB, *atomic.Int32) {
	t.Helper()
	closeCount := &atomic.Int32{}
	cfg.closeCount = closeCount
	registerMockDSN(dsn, cfg)
	db, err := sql.Open("olake_jdbc_test", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, closeCount
}

func usersMockDSN(t *testing.T) (string, mockDSN) {
	t.Helper()
	return t.Name() + "_users", mockDSN{
		cols: []string{"id", "name", "active"},
		data: [][]driver.Value{
			{int64(1), "alice", int64(1)},
			{int64(2), "bob", int64(0)},
			{int64(3), "carol", int64(1)},
		},
	}
}

func testColumnSizer(colType *sql.ColumnType) func(v any) int64 {
	_ = colType
	return func(v any) int64 {
		if v == nil {
			return 0
		}
		return int64(len(fmt.Sprint(v)))
	}
}

func identityConverter(value interface{}, _ string) (interface{}, error) {
	return value, nil
}

// fakeIter is an in-memory Iterable that does not auto-close on exhaustion,
// unlike database/sql.Rows. Used to assert Capture always calls Close().
type fakeIter struct {
	remaining int
	iterErr   error
	closed    atomic.Int32
}

func newFakeIter(count int, iterErr error) *fakeIter {
	return &fakeIter{remaining: count, iterErr: iterErr}
}

func (f *fakeIter) Next() bool {
	if f.remaining <= 0 {
		return false
	}
	f.remaining--
	return true
}

func (f *fakeIter) Err() error {
	return f.iterErr
}

func (f *fakeIter) Close() error {
	f.closed.Add(1)
	return nil
}

func newFakeReader(iter *fakeIter) *Reader[*fakeIter] {
	return NewReader(context.Background(), "SELECT 1", func(_ context.Context, _ string, _ ...any) (*fakeIter, error) {
		return iter, nil
	})
}

func TestReaderCapture_closesOnSuccess(t *testing.T) {
	iter := newFakeIter(3, nil)
	setter := newFakeReader(iter)

	var rowCount int
	err := setter.Capture(func(_ *fakeIter) error {
		rowCount++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, rowCount)
	assert.Equal(t, int32(1), iter.closed.Load(), "Capture must close the iterable on success")
}

func TestReaderCapture_closesOnCaptureError(t *testing.T) {
	iter := newFakeIter(3, nil)
	setter := newFakeReader(iter)

	captureErr := errors.New("capture failed")
	err := setter.Capture(func(_ *fakeIter) error {
		return captureErr
	})
	require.ErrorIs(t, err, captureErr)
	assert.Equal(t, int32(1), iter.closed.Load(), "Capture must close the iterable on early return")
}

func TestReaderCapture_closesOnIterableErr(t *testing.T) {
	iterErr := errors.New("iteration failed")
	iter := newFakeIter(1, iterErr)
	setter := newFakeReader(iter)

	err := setter.Capture(func(_ *fakeIter) error { return nil })
	require.ErrorIs(t, err, iterErr)
	assert.Equal(t, int32(1), iter.closed.Load(), "Capture must close the iterable when Err() fails")
}

func TestReaderCapture_doesNotLeakIterator(t *testing.T) {
	ctx := context.Background()
	var open atomic.Int32
	for i := 0; i < 5; i++ {
		iter := newFakeIter(1, nil)
		setter := NewReader(ctx, "SELECT 1", func(_ context.Context, _ string, _ ...any) (*fakeIter, error) {
			if open.Load() > 0 {
				return nil, errors.New("previous iterator not closed")
			}
			open.Store(1)
			return iter, nil
		})
		err := setter.Capture(func(_ *fakeIter) error { return nil })
		require.NoError(t, err, "iteration %d should not leak the only open iterator", i)
		assert.Equal(t, int32(1), iter.closed.Load())
		open.Store(0)
	}
}

func TestReaderCapture_rejectsQueryWithSemicolon(t *testing.T) {
	ctx := context.Background()
	setter := NewReader(ctx, "SELECT 1;", func(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
		return nil, errors.New("exec must not be called for invalid query")
	})

	err := setter.Capture(func(_ *sql.Rows) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ends with ';'")
}

func TestMapScanConcurrent_emitsAllRows(t *testing.T) {
	dsn, cfg := usersMockDSN(t)
	db, _ := openMockDB(t, dsn, cfg)

	ctx := context.Background()
	setter := NewReader(ctx, "SELECT id, name, active FROM users", func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return db.QueryContext(ctx, query, args...)
	})

	var records []map[string]any
	err := MapScanConcurrent(setter, identityConverter, func(_ context.Context, record map[string]any, sourceBytes int64) error {
		records = append(records, record)
		assert.Positive(t, sourceBytes)
		return nil
	}, testColumnSizer)
	require.NoError(t, err)

	require.Len(t, records, 3)
	assert.Equal(t, int64(1), records[0]["id"])
	assert.Equal(t, "alice", records[0]["name"])
	assert.Equal(t, int64(2), records[1]["id"])
	assert.Equal(t, "bob", records[1]["name"])
}

func TestMapScanConcurrent_propagatesCallbackError(t *testing.T) {
	dsn, cfg := usersMockDSN(t)
	db, _ := openMockDB(t, dsn, cfg)

	ctx := context.Background()
	setter := NewReader(ctx, "SELECT id FROM users", func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return db.QueryContext(ctx, query, args...)
	})

	callbackErr := errors.New("downstream failure")
	err := MapScanConcurrent(setter, identityConverter, func(_ context.Context, _ map[string]any, _ int64) error {
		return callbackErr
	}, testColumnSizer)
	require.ErrorIs(t, err, callbackErr)
}

func TestMapScanConcurrent_skipsRowsViaNilCallbackReturn(t *testing.T) {
	dsn, cfg := usersMockDSN(t)
	db, _ := openMockDB(t, dsn, cfg)

	ctx := context.Background()
	setter := NewReader(ctx, "SELECT id, name FROM users", func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return db.QueryContext(ctx, query, args...)
	})

	var emitted []int64
	err := MapScanConcurrent(setter, identityConverter, func(_ context.Context, record map[string]any, _ int64) error {
		id := record["id"].(int64)
		if id == 2 {
			return nil // mirrors MSSQL CDC before-image skip
		}
		emitted = append(emitted, id)
		return nil
	}, testColumnSizer)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 3}, emitted)
}

func TestMapScanConcurrent_cdcMetadataColumnSizer(t *testing.T) {
	dsn := t.Name() + "_cdc"
	cfg := mockDSN{
		cols: []string{"__$operation", "__$start_lsn", "id", "status"},
		data: [][]driver.Value{{int64(4), []byte{1, 2}, int64(1), "shipped"}},
	}
	db, _ := openMockDB(t, dsn, cfg)

	cdcColumnSizer := func(colType *sql.ColumnType) func(v any) int64 {
		if colType.Name() == "__$operation" || colType.Name() == "__$start_lsn" {
			return func(any) int64 { return 0 }
		}
		return testColumnSizer(colType)
	}

	ctx := context.Background()
	setter := NewReader(ctx, "SELECT __$operation, __$start_lsn, id, status FROM cdc_ct", func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return db.QueryContext(ctx, query, args...)
	})

	var rowBytes int64
	err := MapScanConcurrent(setter, identityConverter, func(_ context.Context, record map[string]any, bytes int64) error {
		rowBytes = bytes
		assert.Equal(t, int64(4), record["__$operation"])
		assert.Equal(t, int64(1), record["id"])
		return nil
	}, cdcColumnSizer)
	require.NoError(t, err)
	assert.Positive(t, rowBytes, "only data columns should contribute to byte count")
}

func TestMapScanConcurrent_stopsProducerOnConsumerError(t *testing.T) {
	dsn := t.Name() + "_cancel"
	rows := make([][]driver.Value, 100)
	for i := range rows {
		rows[i] = []driver.Value{int64(i + 1)}
	}
	cfg := mockDSN{
		cols: []string{"id"},
		data: rows,
	}
	db, closeCount := openMockDB(t, dsn, cfg)

	ctx := context.Background()
	setter := NewReader(ctx, "SELECT id FROM users", func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return db.QueryContext(ctx, query, args...)
	})

	callbackErr := errors.New("downstream failure")
	var emitted int
	err := MapScanConcurrent(setter, identityConverter, func(_ context.Context, _ map[string]any, _ int64) error {
		emitted++
		return callbackErr
	}, testColumnSizer)
	require.ErrorIs(t, err, callbackErr)
	assert.Equal(t, 1, emitted, "consumer should stop after first row")
	assert.Equal(t, int32(1), closeCount.Load(), "rows must be closed when producer is canceled")
}
