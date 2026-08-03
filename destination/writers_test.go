package destination

import (
	"context"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubWriter records the Options it was set up with so a test can assert on what
// the pool handed to each writer thread.
type stubWriter struct {
	deleteMode types.DeleteMode

	mu       sync.Mutex
	setupOpt []*Options
}

func (s *stubWriter) Spec() any                                      { return nil }
func (s *stubWriter) Type() string                                   { return "STUB" }
func (s *stubWriter) Check(context.Context) error                    { return nil }
func (s *stubWriter) Write(context.Context, []types.RawRecord) error { return nil }
func (s *stubWriter) Close(context.Context, any) error               { return nil }

func (s *stubWriter) Setup(_ context.Context, _ types.StreamInterface, _ any, opts *Options) (any, *types.MetadataState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setupOpt = append(s.setupOpt, opts)
	return map[string]string{"id": "long"}, &types.MetadataState{}, nil
}

func (s *stubWriter) FlattenAndCleanData(_ context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	return false, records, nil, nil
}

func (s *stubWriter) EvolveSchema(_ context.Context, globalSchema, _ any) (any, error) {
	return globalSchema, nil
}

func (s *stubWriter) DropStreams(context.Context, []types.StreamInterface) error { return nil }

func (s *stubWriter) options() []*Options {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.setupOpt
}

// registerStubWriter installs a stub destination for the duration of the test.
func registerStubWriter(t *testing.T, deleteMode types.DeleteMode) *stubWriter {
	t.Helper()

	const stubType types.DestinationType = "STUB"
	writer := &stubWriter{deleteMode: deleteMode}

	previous, existed := RegisteredWriters[stubType]
	RegisteredWriters[stubType] = func(*types.WriterConfig, types.DeleteMode) (Writer, func(context.Context), error) {
		return writer, func(context.Context) {}, nil
	}
	t.Cleanup(func() {
		if existed {
			RegisteredWriters[stubType] = previous
			return
		}
		delete(RegisteredWriters, stubType)
	})

	return writer
}

func stubWriterConfig() *types.WriterConfig {
	return &types.WriterConfig{Type: "STUB", WriterConfig: map[string]interface{}{}}
}

func TestNewWriterPoolSkipsRowIndexForEqualityDeletes(t *testing.T) {
	t.Setenv(indexdb.EnvDir, t.TempDir())
	registerStubWriter(t, types.DeleteModeEquality)

	pool, err := NewWriterPool(context.Background(), stubWriterConfig(), types.DeleteModeEquality, []string{"public.orders"}, 100)
	require.NoError(t, err)
	defer pool.Shutdown(context.Background())

	assert.Nil(t, pool.indexStore, "equality-delete syncs must not pay for a row index")

	artifact, ok := pool.writerSchema.Load("public.orders")
	require.True(t, ok)
	assert.Nil(t, artifact.(*writerSchema).rowIndex)
}

func TestNewWriterPoolOpensOneRowIndexPerStream(t *testing.T) {
	t.Setenv(indexdb.EnvDir, t.TempDir())
	registerStubWriter(t, types.DeleteModePosition)

	streams := []string{"public.orders", "public.customers"}
	pool, err := NewWriterPool(context.Background(), stubWriterConfig(), types.DeleteModePosition, streams, 100)
	require.NoError(t, err)
	defer pool.Shutdown(context.Background())

	require.NotNil(t, pool.indexStore)

	indexes := make([]types.TableIndex, 0, len(streams))
	for _, stream := range streams {
		artifact, ok := pool.writerSchema.Load(stream)
		require.True(t, ok, "stream %s", stream)

		rowIndex := artifact.(*writerSchema).rowIndex
		require.NotNil(t, rowIndex, "stream %s", stream)
		indexes = append(indexes, rowIndex)
	}

	assert.NotSame(t, indexes[0], indexes[1], "each stream must get its own database")

	// A fresh index reports no snapshot, which is the signal that the table's
	// rows have not been indexed yet.
	_, ok, err := indexes[0].IndexedSnapshot()
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestNewWriterPoolHandsRowIndexToWriterThreads(t *testing.T) {
	t.Setenv(indexdb.EnvDir, t.TempDir())
	writer := registerStubWriter(t, types.DeleteModePosition)

	pool, err := NewWriterPool(context.Background(), stubWriterConfig(), types.DeleteModePosition, []string{"public.orders"}, 100)
	require.NoError(t, err)
	defer pool.Shutdown(context.Background())

	sourceDatabase := "postgres"
	stream := &types.ConfiguredStream{Stream: types.NewStream("orders", "public", &sourceDatabase)}
	for _, threadID := range []string{"thread-1", "thread-2"} {
		_, _, err := pool.NewWriter(context.Background(), stream, WithThreadID(threadID), WithBackfill(true))
		require.NoError(t, err)
	}

	artifact, ok := pool.writerSchema.Load(stream.ID())
	require.True(t, ok)
	expected := artifact.(*writerSchema).rowIndex

	setupOptions := writer.options()
	require.Len(t, setupOptions, 2)
	for _, opts := range setupOptions {
		// Threads of one stream share the stream's single index.
		assert.Same(t, expected, opts.RowIndex, "thread[%s]", opts.ThreadID)
	}
}

func TestWriterPoolShutdownClosesRowIndexStore(t *testing.T) {
	t.Setenv(indexdb.EnvDir, t.TempDir())
	registerStubWriter(t, types.DeleteModePosition)

	pool, err := NewWriterPool(context.Background(), stubWriterConfig(), types.DeleteModePosition, []string{"public.orders"}, 100)
	require.NoError(t, err)

	pool.Shutdown(context.Background())

	// The store rejects further use once closed, which is how we know Shutdown
	// released the pebble databases rather than leaking them.
	_, err = pool.indexStore.Open(context.Background(), "public.orders")
	require.Error(t, err)
}
