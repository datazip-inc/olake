package types

import (
	"runtime"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	json "github.com/goccy/go-json"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// prevent LogState() from writing logs during tests
	if runtime.GOOS == "windows" {
		viper.Set(constants.StatePath, "NUL")
	} else {
		viper.Set(constants.StatePath, "/dev/null")
	}
}

func newState() *State {
	return &State{RWMutex: &sync.RWMutex{}, Streams: []*StreamState{}}
}

func newConfiguredStream(name, namespace, cursor string, mode SyncMode) *ConfiguredStream {
	s := NewStream(name, namespace, nil)
	s.CursorField = cursor
	s.SyncMode = mode
	return s.Wrap(0)
}

func TestIsZeroAndSetType_ResetStreams(t *testing.T) {
	s := newState()
	assert.True(t, s.isZero(), "new state without streams/global should be zero")

	s.SetType(GlobalType)
	assert.Equal(t, GlobalType, s.Type, "SetType should set the state type")

	// add a stream then reset
	cfg := newConfiguredStream("s1", "ns1", "id", SyncMode("incremental"))
	s.SetCursor(cfg, "id", 123)
	require.False(t, s.isZero(), "state should not be zero after adding cursor")
	s.ResetStreams()
	assert.Equal(t, 0, len(s.Streams), "ResetStreams should clear stream slice")
}

func TestCursorSetAndGet_ResetCursor(t *testing.T) {
	s := newState()
	cfg := newConfiguredStream("users", "public", "id:sub", SyncMode("incremental"))

	// empty key should be ignored
	s.SetCursor(cfg, "", 10)
	assert.Nil(t, s.GetCursor(cfg, ""), "GetCursor with empty key should return nil")

	// set cursor (creates stream)
	s.SetCursor(cfg, "id", 42)
	got := s.GetCursor(cfg, "id")
	require.NotNil(t, got)
	assert.Equal(t, 42, got.(int))

	// ResetCursor should remove both primary and secondary
	primary, secondary := cfg.Cursor()
	assert.Equal(t, "id", primary)
	assert.Equal(t, "sub", secondary)

	// set secondary too
	s.SetCursor(cfg, "sub", 100)
	require.Equal(t, 100, s.GetCursor(cfg, "sub").(int))

	s.ResetCursor(cfg)
	assert.Nil(t, s.GetCursor(cfg, "id"))
	assert.Nil(t, s.GetCursor(cfg, "sub"))
}

func TestGlobalState_SetGet_HasCompletedBackfill(t *testing.T) {
	s := newState()

	cfg := newConfiguredStream("orders", "db1", "id", SyncMode("incremental"))
	// set global state and attach stream
	s.SetGlobal(map[string]any{"k": "v"}, cfg.ID())
	got := s.GetGlobal()
	require.NotNil(t, got)
	assert.Equal(t, "v", got.State.(map[string]any)["k"])
	// HasCompletedBackfill should be true for GLOBAL when stream exists
	s.SetType(GlobalType)
	assert.True(t, s.HasCompletedBackfill(cfg))

	// switch to stream type and use chunks
	s.SetType(StreamType)
	// no chunks -> HasCompletedBackfill true (0-length)
	emptyChunks := NewSet[Chunk]()
	s.SetChunks(cfg, emptyChunks)
	assert.True(t, s.HasCompletedBackfill(cfg))
	// non-empty chunks -> false
	c := NewSet[Chunk](Chunk{Min: 1, Max: 2})
	s.SetChunks(cfg, c)
	assert.False(t, s.HasCompletedBackfill(cfg))
}

func TestChunks_SetGet_RemoveChunk(t *testing.T) {
	s := newState()
	cfg := newConfiguredStream("products", "ns", "id", SyncMode("incremental"))

	// SetChunks should create stream and store chunks
	chunks := NewSet[Chunk](Chunk{Min: 1, Max: 10}, Chunk{Min: 11, Max: 20})
	s.SetChunks(cfg, chunks)

	got := s.GetChunks(cfg)
	require.NotNil(t, got)
	assert.Equal(t, 2, got.Len())

	// Remove one chunk
	remaining := s.RemoveChunk(cfg, Chunk{Min: 1, Max: 10})
	assert.Equal(t, 1, remaining)

	// remove a non-existing chunk -> chunk set unchanged length (or -1 if missing)
	rem := s.RemoveChunk(&ConfiguredStream{Stream: &Stream{Name: "missing", Namespace: "missing"}}, Chunk{Min: 0, Max: 0})
	assert.Equal(t, -1, rem)
}

func TestSetChunks_SkipsOnFullRefresh(t *testing.T) {
	s := newState()
	cfg := newConfiguredStream("table", "db", "id", FULLREFRESH)

	chunks := NewSet[Chunk](Chunk{Min: 1, Max: 2})
	s.SetChunks(cfg, chunks)

	assert.Nil(t, s.GetChunks(cfg), "SetChunks should be no-op for FULLREFRESH mode")
}

func TestState_MarshalJSON_PopulatedStreamsOnly(t *testing.T) {
	s := newState()
	cfg1 := newConfiguredStream("a", "n", "id", SyncMode("incremental"))
	cfg2 := newConfiguredStream("b", "n", "id", SyncMode("incremental"))

	// stream a: set cursor -> holds value
	s.SetCursor(cfg1, "id", 1)
	// stream b: create streamstate but not hold any value
	// create internal streamstate without HoldsValue
	ss := s.initStreamState(cfg2)
	s.Streams = append(s.Streams, ss)

	// marshal
	b, err := json.Marshal(s)
	require.NoError(t, err)

	// unmarshal to generic map and assert only one stream present
	var root map[string]any
	require.NoError(t, json.Unmarshal(b, &root))
	streams, ok := root["streams"].([]any)
	require.True(t, ok)
	assert.Equal(t, 1, len(streams), "Only populated streams must be serialized")
}

func TestStreamState_MarshalUnmarshalJSON_WithChunks(t *testing.T) {
	// prepare stream state
	ss := &StreamState{
		Stream:    "s",
		Namespace: "ns",
		State:     sync.Map{},
	}
	// store a cursor and chunks
	ss.State.Store("cursor", 55)
	ss.State.Store(ChunksKey, NewSet[Chunk](Chunk{Min: 1, Max: 2}))
	ss.HoldsValue.Store(true)

	// marshal
	b, err := json.Marshal(ss)
	require.NoError(t, err)

	// unmarshal back
	var out StreamState
	require.NoError(t, json.Unmarshal(b, &out))

	// check holds value set
	assert.True(t, out.HoldsValue.Load())

	// check cursor restored
	val, _ := out.State.Load("cursor")
	assert.Equal(t, float64(55), val.(float64)) // json unmarshals numbers as float64

	// chunks present and is *Set[Chunk]
	chunksAny, ok := out.State.Load(ChunksKey)
	require.True(t, ok)
	// type assert to *Set[Chunk]
	gotChunks, ok := chunksAny.(*Set[Chunk])
	require.True(t, ok)
	assert.Equal(t, 1, gotChunks.Len())
}
