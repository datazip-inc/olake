package abstract

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDriver implements DriverInterface entirely in memory so we don't need a real
// database connection to test AbstractDriver's own logic. Each field controls what
// the corresponding method returns, so individual tests can tune behavior as needed.
type mockDriver struct {
	driverType     string
	cdcSupported   bool
	maxConnections int
	maxRetries     int

	setupErr        error
	streamNames     []types.StreamID
	streamNamesErr  error
	produceSchemaFn func(stream types.StreamID) (*types.Stream, error)
	setupStateFn    func(state *types.State)
}

func (m *mockDriver) GetConfigRef() Config { return &mockConfig{} }
func (m *mockDriver) Spec() any            { return nil }
func (m *mockDriver) Type() string         { return m.driverType }
func (m *mockDriver) MaxConnections() int  { return m.maxConnections }
func (m *mockDriver) MaxRetries() int      { return m.maxRetries }
func (m *mockDriver) CDCSupported() bool   { return m.cdcSupported }

func (m *mockDriver) Setup(_ context.Context) error { return m.setupErr }

func (m *mockDriver) SetupState(state *types.State) {
	if m.setupStateFn != nil {
		m.setupStateFn(state)
	}
}

func (m *mockDriver) GetStreamNames(_ context.Context) ([]types.StreamID, error) {
	return m.streamNames, m.streamNamesErr
}

func (m *mockDriver) ProduceSchema(_ context.Context, stream types.StreamID) (*types.Stream, error) {
	if m.produceSchemaFn != nil {
		return m.produceSchemaFn(stream)
	}
	return types.NewStream(stream.Name, "public", nil), nil
}

func (m *mockDriver) GetOrSplitChunks(_ context.Context, _ *destination.WriterPool, _ types.StreamInterface) (*types.Set[types.Chunk], error) {
	return types.NewSet[types.Chunk](), nil
}
func (m *mockDriver) ChunkIterator(_ context.Context, _ types.StreamInterface, _ types.Chunk, _ BackfillMsgFn) error {
	return nil
}
func (m *mockDriver) FetchMaxCursorValues(_ context.Context, _ types.StreamInterface) (any, any, error) {
	return nil, nil, nil
}
func (m *mockDriver) StreamIncrementalChanges(_ context.Context, _ types.StreamInterface, _ BackfillMsgFn) error {
	return nil
}
func (m *mockDriver) ChangeStreamConfig() (bool, bool, bool)                    { return true, false, false }
func (m *mockDriver) PreCDC(_ context.Context, _ []types.StreamInterface) error { return nil }
func (m *mockDriver) StreamChanges(_ context.Context, _ int, _ map[string]any, _ CDCMsgFn) (any, error) {
	return nil, nil //nolint:nilnil // mock stub, there's genuinely nothing to return here
}
func (m *mockDriver) PostCDC(_ context.Context, _ int) error { return nil }

// mockConfig just satisfies the Config interface, nothing to validate here.
type mockConfig struct{}

func (c *mockConfig) Validate() error { return nil }

// mockConfiguredStream is a minimal types.StreamInterface stand-in for tests.
type mockConfiguredStream struct {
	name      string
	namespace string
}

func (m *mockConfiguredStream) Name() string      { return m.name }
func (m *mockConfiguredStream) Namespace() string { return m.namespace }
func (m *mockConfiguredStream) ID() string {
	if m.namespace != "" {
		return m.namespace + "." + m.name
	}
	return m.name
}
func (m *mockConfiguredStream) Self() *types.ConfiguredStream { return nil }
func (m *mockConfiguredStream) Schema() *types.TypeSchema     { return nil }
func (m *mockConfiguredStream) GetStream() *types.Stream      { return nil }
func (m *mockConfiguredStream) GetSyncMode() types.SyncMode   { return "" }
func (m *mockConfiguredStream) GetFilter() (types.FilterConfig, bool, error) {
	return types.FilterConfig{}, false, nil
}
func (m *mockConfiguredStream) SupportedSyncModes() *types.Set[types.SyncMode] { return nil }
func (m *mockConfiguredStream) Cursor() (string, string)                       { return "", "" }
func (m *mockConfiguredStream) Validate(_ *types.Stream) error                 { return nil }
func (m *mockConfiguredStream) NormalizationEnabled() bool                     { return false }
func (m *mockConfiguredStream) GetDestinationDatabase(_ *string) string        { return "" }
func (m *mockConfiguredStream) GetDestinationTable() string                    { return "" }
func (m *mockConfiguredStream) GetPartitionRegex() string                      { return "" }
func (m *mockConfiguredStream) RetainSelectedColumns() func(map[string]interface{}) map[string]interface{} {
	return func(r map[string]interface{}) map[string]interface{} { return r }
}
func (m *mockConfiguredStream) IsSelectedColumn() func(string) bool {
	return func(_ string) bool { return true }
}
func (m *mockConfiguredStream) ResolveColumnName(key string) string { return key }

// --- helpers ---

func newTestDriver(driverType string, cdcSupported bool) (*AbstractDriver, *mockDriver) {
	mock := &mockDriver{
		driverType:   driverType,
		cdcSupported: cdcSupported,
		maxRetries:   1,
	}
	ad := NewAbstractDriver(context.Background(), mock)
	return ad, mock
}

// newState builds a types.State with its embedded RWMutex initialized.
// Skipping this made anything that called state.Lock() panic on a nil pointer,
// took a while to figure out why.
func newState() *types.State {
	s := &types.State{}
	s.RWMutex = &sync.RWMutex{}
	return s
}

var _ DriverInterface = (*mockDriver)(nil)
var _ types.StreamInterface = (*mockConfiguredStream)(nil)

func TestDefaultColumns(t *testing.T) {
	expected := map[string]types.DataType{
		constants.OlakeID:        types.String,
		constants.OlakeTimestamp: types.TimestampMicro,
		constants.OpType:         types.String,
		constants.CdcTimestamp:   types.TimestampMicro,
	}
	assert.Len(t, DefaultColumns, len(expected))
	for col, dt := range expected {
		assert.Equal(t, dt, DefaultColumns[col], "DefaultColumns[%s] type mismatch", col)
	}
}

func TestNewAbstractDriver(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	require.NotNil(t, ad)
	require.NotNil(t, ad.GlobalCtxGroup)
	require.NotNil(t, ad.GlobalConnGroup)
	assert.Equal(t, "postgres", ad.Type())
	assert.Equal(t, mock, ad.driver)
}

func TestSetupState(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	state := newState()

	var received *types.State
	mock.setupStateFn = func(s *types.State) { received = s }

	ad.SetupState(state)

	assert.Equal(t, state, ad.state)
	assert.Equal(t, state, received, "driver.SetupState must be forwarded with the same pointer")
}

func TestType(t *testing.T) {
	for _, dt := range []string{"postgres", "mysql", "mongodb", "kafka", "oracle"} {
		t.Run(dt, func(t *testing.T) {
			ad, _ := newTestDriver(dt, false)
			assert.Equal(t, dt, ad.Type())
		})
	}
}

func TestSetup_Success(t *testing.T) {
	ad, _ := newTestDriver("postgres", true)
	assert.NoError(t, ad.Setup(context.Background()))
}

func TestSetup_Error(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.setupErr = errors.New("connection refused")
	assert.EqualError(t, ad.Setup(context.Background()), "connection refused")
}

func TestSupportsCdcColumn(t *testing.T) {
	tests := []struct {
		name         string
		driverType   string
		cdcSupported bool
		want         bool
	}{
		{"postgres CDC", "postgres", true, true},
		{"mysql CDC", "mysql", true, true},
		// Kafka supports CDC but is intentionally excluded from the cdc_timestamp column
		{"kafka CDC", string(constants.Kafka), true, false},
		{"postgres non-CDC", "postgres", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ad, _ := newTestDriver(tc.driverType, tc.cdcSupported)
			assert.Equal(t, tc.want, ad.supportsCdcColumn())
		})
	}
}

// Discover has a lot of branching (sync modes, CDC vs non-CDC, default columns,
// per-driver-type defaults), so it's tested as one table instead of a pile of
// near-duplicate functions.
func TestDiscover(t *testing.T) {
	tests := []struct {
		name           string
		driverType     string
		cdcSupported   bool
		isSync         bool
		maxThreads     int
		streamNames    []types.StreamID
		streamNamesErr error
		produceSchema  func(id types.StreamID) (*types.Stream, error)
		check          func(t *testing.T, streams []*types.Stream, err error)
	}{
		// isSync
		{
			name:         "isSync returns nil",
			driverType:   "postgres",
			cdcSupported: true,
			isSync:       true,
			streamNames:  []types.StreamID{{Name: "orders"}, {Name: "users"}},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				assert.NoError(t, err)
				assert.Nil(t, streams, "isSync=true should return nil so classifyStreams trusts the catalog")
			},
		},
		{
			name:           "GetStreamNames error",
			driverType:     "postgres",
			cdcSupported:   true,
			streamNamesErr: errors.New("no connection"),
			check: func(t *testing.T, _ []*types.Stream, err error) {
				assert.ErrorContains(t, err, "failed to get stream names")
			},
		},
		{
			name:         "empty stream list",
			driverType:   "postgres",
			cdcSupported: true,
			streamNames:  []types.StreamID{},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				assert.NoError(t, err)
				assert.Empty(t, streams)
			},
		},
		{
			name:         "produceSchema error",
			driverType:   "postgres",
			cdcSupported: true,
			streamNames:  []types.StreamID{{Name: "orders"}},
			produceSchema: func(_ types.StreamID) (*types.Stream, error) {
				return nil, fmt.Errorf("schema error")
			},
			check: func(t *testing.T, _ []*types.Stream, err error) {
				assert.ErrorContains(t, err, "error occurred while waiting for connection group")
			},
		},

		// default columns
		{
			name:         "default columns added for CDC driver",
			driverType:   "postgres",
			cdcSupported: true,
			streamNames:  []types.StreamID{{Name: "orders"}},
			produceSchema: func(id types.StreamID) (*types.Stream, error) {
				s := types.NewStream(id.Name, "public", nil)
				s.SupportedSyncModes = types.NewSet(types.CDC)
				return s, nil
			},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				for col := range DefaultColumns {
					_, ok := streams[0].Schema.Properties.Load(col)
					assert.True(t, ok, "expected default column %q in schema", col)
				}
			},
		},
		{
			name:        "CdcTimestamp not added for non-CDC driver",
			driverType:  "postgres",
			streamNames: []types.StreamID{{Name: "users"}},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				_, hasCdcTS := streams[0].Schema.Properties.Load(constants.CdcTimestamp)
				assert.False(t, hasCdcTS, "non-CDC driver must not get CdcTimestamp column")
			},
		},
		{
			name:         "CdcTimestamp not added for Kafka driver",
			driverType:   string(constants.Kafka),
			cdcSupported: true,
			streamNames:  []types.StreamID{{Name: "topic1"}},
			produceSchema: func(id types.StreamID) (*types.Stream, error) {
				return types.NewStream(id.Name, "kafka", nil), nil
			},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				_, hasCdcTS := streams[0].Schema.Properties.Load(constants.CdcTimestamp)
				assert.False(t, hasCdcTS, "Kafka driver must not get CdcTimestamp column")
			},
		},

		// sync mode selection
		{
			name:         "sync mode CDC",
			driverType:   "postgres",
			cdcSupported: true,
			streamNames:  []types.StreamID{{Name: "orders"}},
			produceSchema: func(id types.StreamID) (*types.Stream, error) {
				s := types.NewStream(id.Name, "public", nil)
				s.SupportedSyncModes = types.NewSet(types.CDC, types.INCREMENTAL, types.FULLREFRESH)
				return s, nil
			},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				assert.Equal(t, types.CDC, streams[0].SyncMode)
			},
		},
		{
			// driver supports CDC globally, but this particular stream only supports INCREMENTAL
			name:         "sync mode incremental",
			driverType:   "postgres",
			cdcSupported: true,
			streamNames:  []types.StreamID{{Name: "logs"}},
			produceSchema: func(id types.StreamID) (*types.Stream, error) {
				s := types.NewStream(id.Name, "public", nil)
				s.SupportedSyncModes = types.NewSet(types.INCREMENTAL, types.FULLREFRESH)
				return s, nil
			},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				assert.Equal(t, types.INCREMENTAL, streams[0].SyncMode)
			},
		},
		{
			name:        "sync mode full refresh fallback",
			driverType:  "postgres",
			streamNames: []types.StreamID{{Name: "archive"}},
			produceSchema: func(id types.StreamID) (*types.Stream, error) {
				s := types.NewStream(id.Name, "public", nil)
				s.SupportedSyncModes = types.NewSet(types.FULLREFRESH)
				return s, nil
			},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				assert.Equal(t, types.FULLREFRESH, streams[0].SyncMode)
			},
		},
		{
			// STRICTCDC sits between INCREMENTAL and FULLREFRESH in priority
			name:        "sync mode strict CDC",
			driverType:  "postgres",
			streamNames: []types.StreamID{{Name: "events"}},
			produceSchema: func(id types.StreamID) (*types.Stream, error) {
				s := types.NewStream(id.Name, "public", nil)
				s.SupportedSyncModes = types.NewSet(types.STRICTCDC, types.FULLREFRESH)
				return s, nil
			},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				assert.Equal(t, types.STRICTCDC, streams[0].SyncMode)
			},
		},

		// default stream properties
		{
			name:        "default properties for relational driver",
			driverType:  "postgres",
			streamNames: []types.StreamID{{Name: "users"}},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				props := streams[0].DefaultStreamProperties
				require.NotNil(t, props)
				assert.True(t, props.Normalization)
				assert.False(t, props.AppendMode)
			},
		},
		{
			name:         "default properties for Kafka driver",
			driverType:   string(constants.Kafka),
			cdcSupported: true,
			streamNames:  []types.StreamID{{Name: "topic1"}},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				require.NoError(t, err)
				require.Len(t, streams, 1)
				props := streams[0].DefaultStreamProperties
				require.NotNil(t, props)
				assert.True(t, props.AppendMode)
				assert.False(t, props.Normalization)
			},
		},

		// concurrency
		{
			name:        "max discover threads respected",
			driverType:  "postgres",
			maxThreads:  2,
			streamNames: []types.StreamID{{Name: "s1"}, {Name: "s2"}, {Name: "s3"}},
			check: func(t *testing.T, streams []*types.Stream, err error) {
				assert.NoError(t, err)
				assert.Len(t, streams, 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ad, mock := newTestDriver(tc.driverType, tc.cdcSupported)
			mock.streamNames = tc.streamNames
			mock.streamNamesErr = tc.streamNamesErr
			if tc.produceSchema != nil {
				mock.produceSchemaFn = tc.produceSchema
			}

			streams, err := ad.Discover(context.Background(), tc.maxThreads, tc.isSync)
			tc.check(t, streams, err)
		})
	}
}

func TestClearState_NilState_ReturnsEmptyState(t *testing.T) {
	ad, _ := newTestDriver("postgres", true)
	// nil state shouldn't panic, it should just come back empty
	result, err := ad.ClearState(nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestClearState_RemovesGlobalStreamState(t *testing.T) {
	ad, _ := newTestDriver("postgres", true)

	state := newState()
	state.Global = &types.GlobalState{
		Streams: types.NewSet("public.orders", "public.users"),
	}
	ad.SetupState(state)

	_, err := ad.ClearState([]types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	})
	require.NoError(t, err)

	assert.False(t, ad.state.Global.Streams.Exists("public.orders"), "orders should be removed")
	assert.True(t, ad.state.Global.Streams.Exists("public.users"), "users should remain")
}

func TestClearState_EmptyStreamList_NoOp(t *testing.T) {
	ad, _ := newTestDriver("postgres", true)

	state := newState()
	state.Global = &types.GlobalState{
		Streams: types.NewSet("public.orders"),
	}
	ad.SetupState(state)

	_, err := ad.ClearState([]types.StreamInterface{})
	require.NoError(t, err)
	assert.True(t, ad.state.Global.Streams.Exists("public.orders"))
}

func TestClearState_ResetsPerStreamState(t *testing.T) {
	// clearing a stream should reset HoldsValue and wipe its sync.Map entries
	ad, _ := newTestDriver("postgres", true)

	ss := &types.StreamState{Namespace: "public", Stream: "events"}
	ss.HoldsValue.Store(true)
	ss.State.Store("cursor_value", "abc123")

	state := newState()
	state.Streams = []*types.StreamState{ss}
	ad.SetupState(state)

	_, err := ad.ClearState([]types.StreamInterface{
		&mockConfiguredStream{name: "events", namespace: "public"},
	})
	require.NoError(t, err)

	assert.False(t, ss.HoldsValue.Load(), "HoldsValue should be reset to false")
	_, cursorStillHere := ss.State.Load("cursor_value")
	assert.False(t, cursorStillHere, "State entries should be wiped")
}

func TestClearState_DoesNotTouchUnrelatedStream(t *testing.T) {
	ad, _ := newTestDriver("postgres", true)

	keep := &types.StreamState{Namespace: "public", Stream: "users"}
	keep.HoldsValue.Store(true)
	keep.State.Store("cursor_value", "xyz")

	toClear := &types.StreamState{Namespace: "public", Stream: "logs"}
	toClear.HoldsValue.Store(true)

	state := newState()
	state.Streams = []*types.StreamState{keep, toClear}
	ad.SetupState(state)

	_, err := ad.ClearState([]types.StreamInterface{
		&mockConfiguredStream{name: "logs", namespace: "public"},
	})
	require.NoError(t, err)

	assert.True(t, keep.HoldsValue.Load(), "unrelated stream should be untouched")
	_, cursorExists := keep.State.Load("cursor_value")
	assert.True(t, cursorExists)
}

func TestGenerateThreadID_WithHash(t *testing.T) {
	assert.Equal(t, "public.orders_abc123", generateThreadID("public.orders", "abc123"))
}

func TestGenerateThreadID_EmptyHash_UsesULID(t *testing.T) {
	id := generateThreadID("public.orders", "")
	assert.Contains(t, id, "public.orders_")
	assert.Greater(t, len(id), len("public.orders_"))
}

func TestGenerateThreadID_EmptyHash_IsUnique(t *testing.T) {
	// two calls with no hash should still yield different IDs, via ULID
	id1 := generateThreadID("public.orders", "")
	id2 := generateThreadID("public.orders", "")
	assert.NotEqual(t, id1, id2)
}

func TestGenerateThreadID_EmptyStreamID(t *testing.T) {
	assert.Equal(t, "_hash", generateThreadID("", "hash"))
}

func TestGenerateThreadID_EmptyBoth(t *testing.T) {
	id := generateThreadID("", "")
	assert.Contains(t, id, "_")
	assert.Greater(t, len(id), 1)
}

func TestWaitForBackfillCompletion_Success(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
		&mockConfiguredStream{name: "users", namespace: "public"},
	}
	ch := make(chan string, 2)
	ch <- "public.orders"
	ch <- "public.users"

	var processed []string
	err := ad.waitForBackfillCompletion(context.Background(), ch, streams, func(id string) error {
		processed = append(processed, id)
		return nil
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"public.orders", "public.users"}, processed)
}

func TestWaitForBackfillCompletion_NilProcessFn(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	}
	ch := make(chan string, 1)
	ch <- "public.orders"

	assert.NoError(t, ad.waitForBackfillCompletion(context.Background(), ch, streams, nil))
}

func TestWaitForBackfillCompletion_ContextCancelled(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	}
	ch := make(chan string, 1)

	err := ad.waitForBackfillCompletion(ctx, ch, streams, nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWaitForBackfillCompletion_ClosedChannel(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	}
	ch := make(chan string)
	close(ch)

	err := ad.waitForBackfillCompletion(context.Background(), ch, streams, nil)
	assert.ErrorContains(t, err, "backfill channel closed unexpectedly")
}

func TestWaitForBackfillCompletion_ProcessFnError(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	}
	ch := make(chan string, 1)
	ch <- "public.orders"

	err := ad.waitForBackfillCompletion(context.Background(), ch, streams, func(_ string) error {
		return fmt.Errorf("processing failed")
	})
	assert.ErrorContains(t, err, "processing failed")
}

func TestHandleWriterCleanup_UnsupportedWriterType_SetsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	var state any

	handleWriterCleanup(ctx, cancel, &err, "not-a-writer", "thread-1", &state, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported writer type")
}

func TestHandleWriterCleanup_ThreadIDAppearsInError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	var state any

	handleWriterCleanup(ctx, cancel, &err, "bad", "stream-42", &state, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "thread[stream-42]")
}

func TestHandleWriterCleanup_EmptyThreadID_NoPrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	var state any

	handleWriterCleanup(ctx, cancel, &err, "bad", "", &state, nil)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "thread[")
}

func TestHandleWriterCleanup_ExistingError_CancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	existingErr := fmt.Errorf("upstream broke")
	var state any

	handleWriterCleanup(ctx, cancel, &existingErr, "bad", "t1", &state, nil)

	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context should have been canceled when an error exists")
	}
}

func TestHandleWriterCleanup_MapWriter_EmptyMap_NoPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	var state any
	writers := map[string]*destination.WriterThread{}

	assert.NotPanics(t, func() {
		handleWriterCleanup(ctx, cancel, &err, writers, "t1", &state, nil)
	})
	assert.NoError(t, err)
}

func TestRead_NoStreams_NoError(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)
	// Read(ctx, pool, backfillStreams, cdcStreams, incrementalStreams)
	assert.NoError(t, ad.Read(context.Background(), nil, nil, nil, nil))
}

func TestRead_CDCStreams_DriverNotSupported(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)
	stream := &mockConfiguredStream{name: "orders", namespace: "public"}

	// passing a stream as cdcStreams when the driver doesn't support CDC should error out
	err := ad.Read(context.Background(), nil, nil, []types.StreamInterface{stream}, nil)
	assert.ErrorContains(t, err, "cdc configuration not provided")
}

func TestCDCChange_ZeroValue(t *testing.T) {
	var c CDCChange
	assert.Nil(t, c.Stream)
	assert.True(t, c.Timestamp.IsZero())
	assert.Empty(t, c.Kind)
	assert.Nil(t, c.Data)
	assert.Nil(t, c.ExtraColumns)
}

func TestCDCChange_Populated(t *testing.T) {
	now := time.Now()
	stream := &mockConfiguredStream{name: "orders", namespace: "db"}
	c := CDCChange{
		Stream:       stream,
		Timestamp:    now,
		Kind:         "insert",
		Data:         map[string]any{"id": 1},
		ExtraColumns: map[string]any{"lsn": "0/ABCD"},
	}
	assert.Equal(t, stream, c.Stream)
	assert.Equal(t, now, c.Timestamp)
	assert.Equal(t, "insert", c.Kind)
	assert.Equal(t, 1, c.Data["id"])
	assert.Equal(t, "0/ABCD", c.ExtraColumns["lsn"])
}

func TestCDCChange_AllKinds(t *testing.T) {
	for _, kind := range []string{"insert", "update", "delete"} {
		c := CDCChange{Kind: kind}
		assert.Equal(t, kind, c.Kind)
	}
}

func TestRead_MaxConnections_Applied(t *testing.T) {
	// a positive MaxConnections should update GlobalConnGroup before any sync runs
	mock := &mockDriver{driverType: "postgres", maxConnections: 5, maxRetries: 1}
	ad := NewAbstractDriver(context.Background(), mock)

	assert.NoError(t, ad.Read(context.Background(), nil, nil, nil, nil))
}

func TestWaitForBackfillCompletion_GlobalConnGroupCancelled(t *testing.T) {
	// if the driver's connection group gets canceled mid-backfill, we should
	// get ErrGlobalContextGroup back, not a plain context.Canceled
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	mock := &mockDriver{driverType: "postgres", maxRetries: 1}
	ad := NewAbstractDriver(rootCtx, mock)

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	}
	ch := make(chan string) // deliberately empty so the select blocks

	rootCancel()

	err := ad.waitForBackfillCompletion(context.Background(), ch, streams, nil)
	assert.Equal(t, constants.ErrGlobalContextGroup, err)
}

func TestHandleWriterCleanup_PanicRecovery(t *testing.T) {
	// recover() only works inside a deferred call, so it's wrapped properly here
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	var state any
	writers := map[string]*destination.WriterThread{}

	func() {
		defer handleWriterCleanup(ctx, cancel, &err, writers, "t1", &state, nil)
		panic("something went wrong")
	}()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic recovered")
	assert.Contains(t, err.Error(), "something went wrong")
}

func TestHandleWriterCleanup_MtState_NonNil(t *testing.T) {
	// plain strings go through SetMetadataState without any JSON marshaling issues
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	var state any = "some_cursor_value"
	writers := map[string]*destination.WriterThread{}

	handleWriterCleanup(ctx, cancel, &err, writers, "t1", &state, nil)
	assert.NoError(t, err)
}

func TestHandleWriterCleanup_MtState_UnmarshalableValue(t *testing.T) {
	// channels can't be JSON-marshaled, so SetMetadataState should error here
	ctx, cancel := context.WithCancel(context.Background())

	var err error
	var state any = make(chan int)
	writers := map[string]*destination.WriterThread{}

	handleWriterCleanup(ctx, cancel, &err, writers, "", &state, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set metadata state")

	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected context to be canceled after metadata state error")
	}
}
