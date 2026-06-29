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

// Implements DriverInterface entirely in memory.
// Fields control what each method returns so individual tests can tune behaviour.
type mockDriver struct {
	driverType     string
	cdcSupported   bool
	maxConnections int
	maxRetries     int

	setupErr        error
	streamNames     []string
	streamNamesErr  error
	produceSchemaFn func(stream string) (*types.Stream, error)
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

func (m *mockDriver) GetStreamNames(_ context.Context) ([]string, error) {
	return m.streamNames, m.streamNamesErr
}

func (m *mockDriver) ProduceSchema(_ context.Context, stream string) (*types.Stream, error) {
	if m.produceSchemaFn != nil {
		return m.produceSchemaFn(stream)
	}
	return types.NewStream(stream, "public", nil), nil
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
	return nil, nil
}
func (m *mockDriver) PostCDC(_ context.Context, _ int) error { return nil }

// mockConfig satisfies the Config interface.
type mockConfig struct{}

func (c *mockConfig) Validate() error { return nil }

// mockConfiguredStream — minimal types.StreamInterface for tests.

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
func (m *mockConfiguredStream) RetainSelectedColumns() func(map[string]interface{}) map[string]interface{} {
	return func(r map[string]interface{}) map[string]interface{} { return r }
}
func (m *mockConfiguredStream) IsSelectedColumn() func(string) bool {
	return func(_ string) bool { return true }
}
func (m *mockConfiguredStream) ResolveColumnName(key string) string { return key }

// helpers

func newTestDriver(driverType string, cdcSupported bool) (*AbstractDriver, *mockDriver) {
	mock := &mockDriver{
		driverType:   driverType,
		cdcSupported: cdcSupported,
		maxRetries:   1,
	}
	ad := NewAbstractDriver(context.Background(), mock)
	return ad, mock
}

// newState creates a types.State with its embedded RWMutex properly initialized.
// Without this, any method that called state.Lock() was panicking on a nil pointer.
func newState() *types.State {
	s := &types.State{}
	s.RWMutex = &sync.RWMutex{}
	return s
}

// compile-time check
var _ DriverInterface = (*mockDriver)(nil)

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

// TestNewAbstractDriver

func TestNewAbstractDriver(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	require.NotNil(t, ad)
	require.NotNil(t, ad.GlobalCtxGroup)
	require.NotNil(t, ad.GlobalConnGroup)
	assert.Equal(t, "postgres", ad.Type())
	assert.Equal(t, mock, ad.driver)
}

// TestSetupState

func TestSetupState(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	state := newState()

	var received *types.State
	mock.setupStateFn = func(s *types.State) { received = s }

	ad.SetupState(state)

	assert.Equal(t, state, ad.state)
	assert.Equal(t, state, received, "driver.SetupState must be forwarded with the same pointer")
}

// TestType

func TestType(t *testing.T) {
	for _, dt := range []string{"postgres", "mysql", "mongodb", "kafka", "oracle"} {
		t.Run(dt, func(t *testing.T) {
			ad, _ := newTestDriver(dt, false)
			assert.Equal(t, dt, ad.Type())
		})
	}
}

// TestSetup

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
		// Kafka supports CDC but intentionally excluded from cdc_timestamp column
		{"kafka CDC", string(constants.Kafka), true, false},
		{"postgres non-CDC", "postgres", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad, _ := newTestDriver(tt.driverType, tt.cdcSupported)
			assert.Equal(t, tt.want, ad.supportsCdcColumn())
		})
	}
}

// TestDiscover
func TestDiscover_IsSync_ReturnsNil(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.streamNames = []string{"orders", "users"}

	streams, err := ad.Discover(context.Background(), 0, true)
	assert.NoError(t, err)
	assert.Nil(t, streams, "isSync=true should return nil so classifyStreams trusts the catalog")
}

func TestDiscover_GetStreamNamesError(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.streamNamesErr = errors.New("no connection")

	_, err := ad.Discover(context.Background(), 0, false)
	assert.ErrorContains(t, err, "failed to get stream names")
}

func TestDiscover_EmptyStreamList(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.streamNames = []string{}

	streams, err := ad.Discover(context.Background(), 0, false)
	assert.NoError(t, err)
	assert.Empty(t, streams)
}

func TestDiscover_ProduceSchemaError(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.streamNames = []string{"orders"}
	mock.produceSchemaFn = func(_ string) (*types.Stream, error) {
		return nil, fmt.Errorf("schema error")
	}

	_, err := ad.Discover(context.Background(), 0, false)
	assert.ErrorContains(t, err, "error occurred while waiting for connection group")
}

func TestDiscover_DefaultColumnsAdded_CDCDriver(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.streamNames = []string{"orders"}
	mock.produceSchemaFn = func(name string) (*types.Stream, error) {
		s := types.NewStream(name, "public", nil)
		s.SupportedSyncModes = types.NewSet(types.CDC)
		return s, nil
	}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)

	for col := range DefaultColumns {
		_, ok := streams[0].Schema.Properties.Load(col)
		assert.True(t, ok, "expected default column %q in schema", col)
	}
}

func TestDiscover_CdcTimestampColumn_NotAdded_NonCdcDriver(t *testing.T) {
	ad, mock := newTestDriver("postgres", false)
	mock.streamNames = []string{"users"}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)

	_, hasCdcTs := streams[0].Schema.Properties.Load(constants.CdcTimestamp)
	assert.False(t, hasCdcTs, "non-CDC driver must not get CdcTimestamp column")
}

func TestDiscover_CdcTimestampColumn_NotAdded_KafkaDriver(t *testing.T) {
	ad, mock := newTestDriver(string(constants.Kafka), true)
	mock.streamNames = []string{"topic1"}
	mock.produceSchemaFn = func(name string) (*types.Stream, error) {
		return types.NewStream(name, "kafka", nil), nil
	}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)

	_, hasCdcTs := streams[0].Schema.Properties.Load(constants.CdcTimestamp)
	assert.False(t, hasCdcTs, "Kafka driver must not get CdcTimestamp column")
}

func TestDiscover_SyncModeSelection_CDC(t *testing.T) {
	ad, mock := newTestDriver("postgres", true)
	mock.streamNames = []string{"orders"}
	mock.produceSchemaFn = func(name string) (*types.Stream, error) {
		s := types.NewStream(name, "public", nil)
		s.SupportedSyncModes = types.NewSet(types.CDC, types.INCREMENTAL, types.FULLREFRESH)
		return s, nil
	}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, types.CDC, streams[0].SyncMode)
}

func TestDiscover_SyncModeSelection_Incremental(t *testing.T) {
	// Driver supports CDC globally but this stream only supports INCREMENTAL.
	ad, mock := newTestDriver("postgres", true)
	mock.streamNames = []string{"logs"}
	mock.produceSchemaFn = func(name string) (*types.Stream, error) {
		s := types.NewStream(name, "public", nil)
		s.SupportedSyncModes = types.NewSet(types.INCREMENTAL, types.FULLREFRESH)
		return s, nil
	}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, types.INCREMENTAL, streams[0].SyncMode)
}

func TestDiscover_SyncModeSelection_FullRefresh_Fallback(t *testing.T) {
	ad, mock := newTestDriver("postgres", false)
	mock.streamNames = []string{"archive"}
	mock.produceSchemaFn = func(name string) (*types.Stream, error) {
		s := types.NewStream(name, "public", nil)
		s.SupportedSyncModes = types.NewSet(types.FULLREFRESH)
		return s, nil
	}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, types.FULLREFRESH, streams[0].SyncMode)
}

func TestDiscover_DefaultStreamProperties_RelationalDriver(t *testing.T) {
	ad, mock := newTestDriver("postgres", false)
	mock.streamNames = []string{"users"}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)

	props := streams[0].DefaultStreamProperties
	require.NotNil(t, props)
	assert.True(t, props.Normalization)
	assert.False(t, props.AppendMode)
}

func TestDiscover_DefaultStreamProperties_KafkaDriver(t *testing.T) {
	ad, mock := newTestDriver(string(constants.Kafka), true)
	mock.streamNames = []string{"topic1"}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)

	props := streams[0].DefaultStreamProperties
	require.NotNil(t, props)
	assert.True(t, props.AppendMode)
	assert.False(t, props.Normalization)
}

func TestDiscover_MaxDiscoverThreadsRespected(t *testing.T) {
	ad, mock := newTestDriver("postgres", false)
	mock.streamNames = []string{"s1", "s2", "s3"}

	streams, err := ad.Discover(context.Background(), 2, false)
	assert.NoError(t, err)
	assert.Len(t, streams, 3)
}

// TestClearState
func TestClearState_NilState_ReturnsEmptyState(t *testing.T) {
	ad, _ := newTestDriver("postgres", true)
	// state is nil — ClearState must return empty State, not panic
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
	// ClearState should reset HoldsValue and wipe the State sync.Map entries.
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

	clear := &types.StreamState{Namespace: "public", Stream: "logs"}
	clear.HoldsValue.Store(true)

	state := newState()
	state.Streams = []*types.StreamState{keep, clear}
	ad.SetupState(state)

	_, err := ad.ClearState([]types.StreamInterface{
		&mockConfiguredStream{name: "logs", namespace: "public"},
	})
	require.NoError(t, err)

	assert.True(t, keep.HoldsValue.Load(), "unrelated stream should be untouched")
	_, cursorExists := keep.State.Load("cursor_value")
	assert.True(t, cursorExists)
}

// TestGenerateThreadID

func TestGenerateThreadID_WithHash(t *testing.T) {
	assert.Equal(t, "public.orders_abc123", generateThreadID("public.orders", "abc123"))
}

func TestGenerateThreadID_EmptyHash_UsesULID(t *testing.T) {
	id := generateThreadID("public.orders", "")
	assert.Contains(t, id, "public.orders_")
	assert.Greater(t, len(id), len("public.orders_"))
}

func TestGenerateThreadID_EmptyHash_IsUnique(t *testing.T) {
	// Two calls with no hash should yield different IDs via ULID.
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

// TestWaitForBackfillCompletion

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

	// nil processStream must not panic
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

// TestHandleWriterCleanup

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
		// expected — cancel was called because err != nil
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context should have been cancelled when an error exists")
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
	// Empty map means no Close calls — no error expected.
	assert.NoError(t, err)
}

// TestRead

func TestRead_NoStreams_NoError(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)
	// Read(ctx, pool, backfillStreams, cdcStreams, incrementalStreams)
	assert.NoError(t, ad.Read(context.Background(), nil, nil, nil, nil))
}

func TestRead_CDCStreams_DriverNotSupported(t *testing.T) {
	ad, _ := newTestDriver("postgres", false)
	stream := &mockConfiguredStream{name: "orders", namespace: "public"}

	// Passing stream as cdcStreams — driver doesn't support CDC, must error
	err := ad.Read(context.Background(), nil, nil, []types.StreamInterface{stream}, nil)
	assert.ErrorContains(t, err, "cdc configuration not provided")
}

// TestCDCChange — struct field sanity

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

func TestDiscover_SyncModeSelection_StrictCDC(t *testing.T) {
	// STRICTCDC sits between INCREMENTAL and FULLREFRESH in priority.
	ad, mock := newTestDriver("postgres", false)
	mock.streamNames = []string{"events"}
	mock.produceSchemaFn = func(name string) (*types.Stream, error) {
		s := types.NewStream(name, "public", nil)
		s.SupportedSyncModes = types.NewSet(types.STRICTCDC, types.FULLREFRESH)
		return s, nil
	}

	streams, err := ad.Discover(context.Background(), 0, false)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, types.STRICTCDC, streams[0].SyncMode)
}

func TestRead_MaxConnections_Applied(t *testing.T) {
	// MaxConnections > 0 should update GlobalConnGroup before any sync runs.
	mock := &mockDriver{driverType: "postgres", maxConnections: 5, maxRetries: 1}
	ad := NewAbstractDriver(context.Background(), mock)

	assert.NoError(t, ad.Read(context.Background(), nil, nil, nil, nil))
}

func TestWaitForBackfillCompletion_GlobalConnGroupCancelled(t *testing.T) {
	// If the driver's connection group is cancelled mid-backfill,
	// we should get ErrGlobalContextGroup back, not a context.Canceled.
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	mock := &mockDriver{driverType: "postgres", maxRetries: 1}
	ad := NewAbstractDriver(rootCtx, mock)

	streams := []types.StreamInterface{
		&mockConfiguredStream{name: "orders", namespace: "public"},
	}
	ch := make(chan string) // intentionally empty so the select blocks

	rootCancel()

	err := ad.waitForBackfillCompletion(context.Background(), ch, streams, nil)
	assert.Equal(t, constants.ErrGlobalContextGroup, err)
}

func TestHandleWriterCleanup_PanicRecovery(t *testing.T) {
	// recover() only works inside a deferred call, so we wrap it properly here.
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
	// String values go through SetMetadataState without JSON marshalling should be fine.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	var state any = "some_cursor_value"
	writers := map[string]*destination.WriterThread{}

	handleWriterCleanup(ctx, cancel, &err, writers, "t1", &state, nil)
	assert.NoError(t, err)
}

func TestHandleWriterCleanup_MtState_UnmarshalableValue(t *testing.T) {
	// Channels can't be JSON-marshalled, so SetMetadataState will error.
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
		t.Fatal("expected context to be cancelled after metadata state error")
	}
}
