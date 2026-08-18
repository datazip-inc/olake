package types

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/goccy/go-json"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestState builds a usable State fixture.
//
// Two things make a bare &State{} unusable in tests:
//   - State embeds *sync.RWMutex, so every locking method nil-panics unless it is set.
//   - Every locking mutator (ResetStreams, ResetCursor, SetGlobal, SetCursor, SetChunks
//     and RemoveChunk — SetType is the exception, it neither locks nor logs) ends in
//     LogState(), which calls logger.Fatalf (os.Exit(1)) when constants.StatePath is
//     unset. That kills the test binary without any failure output, so the path is
//     pointed at a temp file here.
func newTestState(t *testing.T, typ StateType) *State {
	t.Helper()
	old := viper.GetString(constants.StatePath)
	t.Cleanup(func() { viper.Set(constants.StatePath, old) })
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	return &State{
		RWMutex: &sync.RWMutex{},
		Type:    typ,
	}
}

func testStream(namespace, name string, opts ...func(*Stream)) *ConfiguredStream {
	stream := &Stream{
		Name:      name,
		Namespace: namespace,
		Schema:    NewTypeSchema(),
		SyncMode:  INCREMENTAL,
	}
	for _, opt := range opts {
		opt(stream)
	}
	return &ConfiguredStream{Stream: stream}
}

func usersStream(opts ...func(*Stream)) *ConfiguredStream {
	return testStream("public", "users", opts...)
}
func ordersStream(opts ...func(*Stream)) *ConfiguredStream {
	return testStream("public", "orders", opts...)
}

func withCursorField(field string) func(*Stream) {
	return func(s *Stream) { s.CursorField = field }
}

func withSyncMode(mode SyncMode) func(*Stream) {
	return func(s *Stream) { s.SyncMode = mode }
}

// TestStateSetType tests the SetType function
func TestStateSetType(t *testing.T) {
	tests := []struct {
		name     string
		typ      StateType
		expected StateType
	}{
		// ===== supported types =====
		{
			name:     "global type",
			typ:      GlobalType,
			expected: GlobalType,
		},
		{
			name:     "stream type",
			typ:      StreamType,
			expected: StreamType,
		},
		{
			name:     "mixed type",
			typ:      MixedType,
			expected: MixedType,
		},

		// ===== unset =====
		{
			name:     "empty type accepted as-is",
			typ:      StateType(""),
			expected: StateType(""),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StateType(""))
			state.SetType(tc.typ)

			assert.Equal(t, tc.expected, state.Type)
		})
	}
}

// TestStateIsZero tests the isZero function
func TestStateIsZero(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(s *State)
		expected bool
	}{
		// ===== empty =====
		{
			name:     "fresh state",
			setup:    func(_ *State) {},
			expected: true,
		},
		{
			name:     "type set but no data",
			setup:    func(s *State) { s.SetType(GlobalType) },
			expected: true,
		},

		// ===== populated =====
		{
			name:     "global state populated",
			setup:    func(s *State) { s.SetGlobal(map[string]any{"lsn": "0/16B3748"}) },
			expected: false,
		},
		{
			name:     "cursor populated",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			expected: false,
		},
		{
			name:     "chunks populated",
			setup:    func(s *State) { s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10})) },
			expected: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)

			assert.Equal(t, tc.expected, state.isZero())
		})
	}
}

// TestStateResetStreams tests the ResetStreams function
func TestStateResetStreams(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(s *State)
		expectedGlobal any
	}{
		// ===== stream state cleared =====
		{
			name:           "empty state is a no-op",
			setup:          func(_ *State) {},
			expectedGlobal: nil,
		},
		{
			name: "clears every stream state",
			setup: func(s *State) {
				s.SetCursor(usersStream(), "updated_at", "2024-01-01")
				s.SetCursor(ordersStream(), "updated_at", "2024-01-02")
			},
			expectedGlobal: nil,
		},
		{
			name: "clears chunks along with cursors",
			setup: func(s *State) {
				s.SetCursor(usersStream(), "updated_at", "2024-01-01")
				s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10}))
			},
			expectedGlobal: nil,
		},

		// ===== global state survives =====
		{
			name: "leaves global state untouched",
			setup: func(s *State) {
				s.SetGlobal("resume-token", usersStream().ID())
				s.SetCursor(usersStream(), "updated_at", "2024-01-01")
			},
			expectedGlobal: "resume-token",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)

			state.ResetStreams()

			assert.Empty(t, state.Streams)
			assert.NotNil(t, state.Streams, "streams should be reset to an empty slice, not nil")
			assert.Nil(t, state.GetCursor(usersStream(), "updated_at"))
			assert.Nil(t, state.GetChunks(usersStream()))

			if tc.expectedGlobal != nil {
				require.NotNil(t, state.GetGlobal())
				assert.Equal(t, tc.expectedGlobal, state.GetGlobal().State)
			} else {
				assert.Nil(t, state.GetGlobal())
			}
		})
	}
}

// TestStateGetGlobal tests the GetGlobal function
func TestStateGetGlobal(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(s *State)
		expectedNil     bool
		expectedState   any
		expectedStreams []string
	}{
		// ===== unset =====
		{
			name:        "nil until set",
			setup:       func(_ *State) {},
			expectedNil: true,
		},

		// ===== set =====
		{
			name:            "returns the stored value and streams",
			setup:           func(s *State) { s.SetGlobal("resume-token", "public.users") },
			expectedState:   "resume-token",
			expectedStreams: []string{"public.users"},
		},
		{
			name:            "returns a structured value unchanged",
			setup:           func(s *State) { s.SetGlobal(map[string]any{"lsn": "0/16B3748"}) },
			expectedState:   map[string]any{"lsn": "0/16B3748"},
			expectedStreams: []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, GlobalType)
			tc.setup(state)

			global := state.GetGlobal()

			if tc.expectedNil {
				assert.Nil(t, global)
				return
			}
			require.NotNil(t, global)
			assert.Equal(t, tc.expectedState, global.State)
			require.NotNil(t, global.Streams)
			assert.Equal(t, len(tc.expectedStreams), global.Streams.Len())
			for _, id := range tc.expectedStreams {
				assert.True(t, global.Streams.Exists(id))
			}
		})
	}
}

// TestStateSetGlobal tests the SetGlobal function
func TestStateSetGlobal(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(s *State)
		state           any
		streams         []string
		expectedState   any
		expectedStreams []string
	}{
		// ===== first write =====
		{
			name:            "creates global state with streams",
			setup:           func(_ *State) {},
			state:           "resume-token",
			streams:         []string{"public.users", "public.orders"},
			expectedState:   "resume-token",
			expectedStreams: []string{"public.users", "public.orders"},
		},
		{
			name:            "creates an empty stream set when no stream is passed",
			setup:           func(_ *State) {},
			state:           "resume-token",
			streams:         nil,
			expectedState:   "resume-token",
			expectedStreams: []string{},
		},

		// ===== subsequent writes =====
		{
			name:            "overwrites the value and merges streams",
			setup:           func(s *State) { s.SetGlobal("token-v1", "public.users") },
			state:           "token-v2",
			streams:         []string{"public.orders"},
			expectedState:   "token-v2",
			expectedStreams: []string{"public.users", "public.orders"},
		},
		{
			name:            "a nil value preserves the stored state",
			setup:           func(s *State) { s.SetGlobal("token-v1", "public.users") },
			state:           nil,
			streams:         []string{"public.orders"},
			expectedState:   "token-v1",
			expectedStreams: []string{"public.users", "public.orders"},
		},
		{
			name:            "re-inserting a known stream is idempotent",
			setup:           func(s *State) { s.SetGlobal("token", "public.users") },
			state:           "token",
			streams:         []string{"public.users"},
			expectedState:   "token",
			expectedStreams: []string{"public.users"},
		},
		{
			name:            "no streams passed leaves the existing set alone",
			setup:           func(s *State) { s.SetGlobal("token-v1", "public.users") },
			state:           "token-v2",
			streams:         nil,
			expectedState:   "token-v2",
			expectedStreams: []string{"public.users"},
		},

		// ===== decoded state with a null stream set =====
		{
			name:            "creates the stream set when the existing global has none",
			setup:           func(s *State) { s.Global = &GlobalState{State: "token"} },
			state:           nil,
			streams:         []string{"public.users"},
			expectedState:   "token",
			expectedStreams: []string{"public.users"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, GlobalType)
			tc.setup(state)

			state.SetGlobal(tc.state, tc.streams...)

			global := state.GetGlobal()
			require.NotNil(t, global)
			assert.Equal(t, tc.expectedState, global.State)
			require.NotNil(t, global.Streams)
			assert.Equal(t, len(tc.expectedStreams), global.Streams.Len())
			for _, id := range tc.expectedStreams {
				assert.True(t, global.Streams.Exists(id), "stream %s should be tracked", id)
			}
		})
	}
}

// TestStateGetCursor tests the GetCursor function
func TestStateGetCursor(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(s *State)
		stream   *ConfiguredStream
		key      string
		expected any
	}{
		// ===== empty key =====
		{
			name:     "empty key reads as nil",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:   usersStream(),
			key:      "",
			expected: nil,
		},

		// ===== missing entries =====
		{
			name:     "unknown stream reads as nil",
			setup:    func(_ *State) {},
			stream:   usersStream(),
			key:      "updated_at",
			expected: nil,
		},
		{
			name:     "known stream with unknown key reads as nil",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:   usersStream(),
			key:      "created_at",
			expected: nil,
		},
		{
			name:     "a different stream reads as nil",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:   ordersStream(),
			key:      "updated_at",
			expected: nil,
		},

		// ===== stored values =====
		{
			name:     "returns the stored value",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:   usersStream(),
			key:      "updated_at",
			expected: "2024-01-01",
		},
		{
			name:     "returns a non-string value unchanged",
			setup:    func(s *State) { s.SetCursor(usersStream(), "id", 42) },
			stream:   usersStream(),
			key:      "id",
			expected: 42,
		},
		{
			name:     "a stored nil reads as nil",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", nil) },
			stream:   usersStream(),
			key:      "updated_at",
			expected: nil,
		},

		// ===== namespace scoping =====
		{
			name: "streams are keyed on namespace and name together",
			setup: func(s *State) {
				s.SetCursor(testStream("public", "users"), "updated_at", "public-value")
				s.SetCursor(testStream("sales", "users"), "updated_at", "sales-value")
			},
			stream:   testStream("sales", "users"),
			key:      "updated_at",
			expected: "sales-value",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)

			assert.Equal(t, tc.expected, state.GetCursor(tc.stream, tc.key))
		})
	}
}

// TestStateSetCursor tests the SetCursor function
func TestStateSetCursor(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(s *State)
		stream        *ConfiguredStream
		key           string
		value         any
		expectedCount int
		expected      any
	}{
		// ===== empty key =====
		{
			name:          "empty key does not create a stream state",
			setup:         func(_ *State) {},
			stream:        usersStream(),
			key:           "",
			value:         "2024-01-01",
			expectedCount: 0,
			expected:      nil,
		},

		// ===== stream state creation =====
		{
			name:          "creates the stream state on first write",
			setup:         func(_ *State) {},
			stream:        usersStream(),
			key:           "updated_at",
			value:         "2024-01-01",
			expectedCount: 1,
			expected:      "2024-01-01",
		},
		{
			name:          "appends a stream state for a new stream",
			setup:         func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:        ordersStream(),
			key:           "updated_at",
			value:         "2024-01-02",
			expectedCount: 2,
			expected:      "2024-01-02",
		},

		// ===== reuse of an existing stream state =====
		{
			name:          "reuses the stream state for a second key",
			setup:         func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:        usersStream(),
			key:           "id",
			value:         42,
			expectedCount: 1,
			expected:      42,
		},
		{
			name:          "overwrites an existing value",
			setup:         func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			stream:        usersStream(),
			key:           "updated_at",
			value:         "2024-02-01",
			expectedCount: 1,
			expected:      "2024-02-01",
		},
		{
			name:          "reuses a stream state created by SetChunks",
			setup:         func(s *State) { s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10})) },
			stream:        usersStream(),
			key:           "updated_at",
			value:         "2024-01-01",
			expectedCount: 1,
			expected:      "2024-01-01",
		},

		// ===== nil values =====
		{
			name:          "stores a nil value",
			setup:         func(_ *State) {},
			stream:        usersStream(),
			key:           "updated_at",
			value:         nil,
			expectedCount: 1,
			expected:      nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)

			state.SetCursor(tc.stream, tc.key, tc.value)

			require.Len(t, state.Streams, tc.expectedCount)
			assert.Equal(t, tc.expected, state.GetCursor(tc.stream, tc.key))
			if tc.expectedCount > 0 {
				written := state.Streams[len(state.Streams)-1]
				assert.Equal(t, tc.stream.Name(), written.Stream)
				assert.Equal(t, tc.stream.Namespace(), written.Namespace)
				assert.True(t, written.HoldsValue.Load(), "a written stream must be marked for persistence")
				assert.Empty(t, written.SyncMode, "initStreamState does not populate SyncMode")
			}
		})
	}
}

// TestStateResetCursor tests the ResetCursor function
func TestStateResetCursor(t *testing.T) {
	tests := []struct {
		name          string
		cursorField   string
		setup         func(s *State, stream *ConfiguredStream)
		expectedKeys  map[string]any
		expectedChunk int // -1 when no chunk set is expected
	}{
		// ===== primary cursor only =====
		{
			name:        "clears the primary cursor and leaves other keys",
			cursorField: "updated_at",
			setup: func(s *State, stream *ConfiguredStream) {
				s.SetCursor(stream, "updated_at", "2024-01-01")
				s.SetCursor(stream, "id", 42)
			},
			expectedKeys:  map[string]any{"updated_at": nil, "id": 42},
			expectedChunk: -1,
		},

		// ===== primary and secondary cursor =====
		{
			name:        "clears both primary and secondary cursors",
			cursorField: "updated_at:id",
			setup: func(s *State, stream *ConfiguredStream) {
				s.SetCursor(stream, "updated_at", "2024-01-01")
				s.SetCursor(stream, "id", 42)
			},
			expectedKeys:  map[string]any{"updated_at": nil, "id": nil},
			expectedChunk: -1,
		},

		// ===== chunks are not cursors =====
		{
			name:        "leaves chunks intact",
			cursorField: "updated_at",
			setup: func(s *State, stream *ConfiguredStream) {
				s.SetCursor(stream, "updated_at", "2024-01-01")
				s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 10}))
			},
			expectedKeys:  map[string]any{"updated_at": nil},
			expectedChunk: 1,
		},

		// ===== unknown stream =====
		{
			name:          "unknown stream is a no-op",
			cursorField:   "updated_at",
			setup:         func(_ *State, _ *ConfiguredStream) {},
			expectedKeys:  map[string]any{"updated_at": nil},
			expectedChunk: -1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			stream := usersStream(withCursorField(tc.cursorField))
			tc.setup(state, stream)

			assert.NotPanics(t, func() { state.ResetCursor(stream) })

			for key, expected := range tc.expectedKeys {
				assert.Equal(t, expected, state.GetCursor(stream, key), "cursor key %s", key)
			}
			if tc.expectedChunk < 0 {
				assert.Nil(t, state.GetChunks(stream))
			} else {
				require.NotNil(t, state.GetChunks(stream))
				assert.Equal(t, tc.expectedChunk, state.GetChunks(stream).Len())
			}
		})
	}
}

// TestStateGetChunks tests the GetChunks function
func TestStateGetChunks(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(s *State)
		expectedNil bool
		expectedLen int
	}{
		// ===== nothing recorded =====
		{
			name:        "unknown stream has no chunks",
			setup:       func(_ *State) {},
			expectedNil: true,
		},
		{
			name:        "known stream that was never chunked",
			setup:       func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			expectedNil: true,
		},
		{
			name:        "another stream's chunks are not visible",
			setup:       func(s *State) { s.SetChunks(ordersStream(), NewSet(Chunk{Min: 1, Max: 10})) },
			expectedNil: true,
		},

		// ===== chunks recorded =====
		{
			name:        "returns the stored set",
			setup:       func(s *State) { s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10}, Chunk{Min: 10, Max: 20})) },
			expectedLen: 2,
		},
		{
			name:        "an empty set is not the same as no chunks recorded",
			setup:       func(s *State) { s.SetChunks(usersStream(), NewSet[Chunk]()) },
			expectedLen: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)

			chunks := state.GetChunks(usersStream())

			if tc.expectedNil {
				assert.Nil(t, chunks)
				return
			}
			require.NotNil(t, chunks)
			assert.Equal(t, tc.expectedLen, chunks.Len())
		})
	}
}

// TestStateSetChunks tests the SetChunks function
func TestStateSetChunks(t *testing.T) {
	tests := []struct {
		name          string
		syncMode      SyncMode
		setup         func(s *State)
		chunks        *Set[Chunk]
		expectedCount int
		expectedNil   bool
		expectedLen   int
	}{
		// ===== modes that persist chunks =====
		{
			name:          "incremental persists chunks",
			syncMode:      INCREMENTAL,
			setup:         func(_ *State) {},
			chunks:        NewSet(Chunk{Min: 1, Max: 10}, Chunk{Min: 10, Max: 20}),
			expectedCount: 1,
			expectedLen:   2,
		},
		{
			name:          "cdc persists chunks",
			syncMode:      CDC,
			setup:         func(_ *State) {},
			chunks:        NewSet(Chunk{Min: 1, Max: 10}),
			expectedCount: 1,
			expectedLen:   1,
		},
		{
			name:          "strict cdc persists chunks",
			syncMode:      STRICTCDC,
			setup:         func(_ *State) {},
			chunks:        NewSet(Chunk{Min: 1, Max: 10}),
			expectedCount: 1,
			expectedLen:   1,
		},

		// ===== full refresh re-reads everything, so chunks are meaningless =====
		{
			name:          "full refresh is a no-op",
			syncMode:      FULLREFRESH,
			setup:         func(_ *State) {},
			chunks:        NewSet(Chunk{Min: 1, Max: 10}),
			expectedCount: 0,
			expectedNil:   true,
		},

		// ===== reuse of an existing stream state =====
		{
			name:          "reuses a stream state created by SetCursor",
			syncMode:      INCREMENTAL,
			setup:         func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			chunks:        NewSet(Chunk{Min: 1, Max: 10}),
			expectedCount: 1,
			expectedLen:   1,
		},
		{
			name:          "overwrites a previously stored set",
			syncMode:      INCREMENTAL,
			setup:         func(s *State) { s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10}, Chunk{Min: 10, Max: 20})) },
			chunks:        NewSet(Chunk{Min: 100, Max: 200}),
			expectedCount: 1,
			expectedLen:   1,
		},

		// ===== edge case sets =====
		{
			name:          "stores an empty set",
			syncMode:      INCREMENTAL,
			setup:         func(_ *State) {},
			chunks:        NewSet[Chunk](),
			expectedCount: 1,
			expectedLen:   0,
		},
		{
			name:          "stores chunks with nil bounds",
			syncMode:      INCREMENTAL,
			setup:         func(_ *State) {},
			chunks:        NewSet(Chunk{Min: nil, Max: 10}, Chunk{Min: 10, Max: nil}),
			expectedCount: 1,
			expectedLen:   2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)
			stream := usersStream(withSyncMode(tc.syncMode))

			state.SetChunks(stream, tc.chunks)

			require.Len(t, state.Streams, tc.expectedCount)
			chunks := state.GetChunks(stream)
			if tc.expectedNil {
				assert.Nil(t, chunks)
				return
			}
			require.NotNil(t, chunks)
			assert.Equal(t, tc.expectedLen, chunks.Len())
			assert.True(t, state.Streams[0].HoldsValue.Load(), "a chunked stream must be marked for persistence")
		})
	}
}

// TestStateRemoveChunk tests the RemoveChunk function
func TestStateRemoveChunk(t *testing.T) {
	first := Chunk{Min: 1, Max: 10}
	second := Chunk{Min: 10, Max: 20}

	tests := []struct {
		name              string
		setup             func(s *State)
		chunk             Chunk
		expected          int
		checkOrders       bool
		expectedOrdersLen int
	}{
		// ===== remaining count =====
		{
			name:     "returns the number of chunks left",
			setup:    func(s *State) { s.SetChunks(usersStream(), NewSet(first, second)) },
			chunk:    first,
			expected: 1,
		},
		{
			name:     "returns zero once the last chunk is removed",
			setup:    func(s *State) { s.SetChunks(usersStream(), NewSet(first)) },
			chunk:    first,
			expected: 0,
		},
		{
			name:     "removing an unknown chunk keeps the count",
			setup:    func(s *State) { s.SetChunks(usersStream(), NewSet(first)) },
			chunk:    Chunk{Min: 100, Max: 200},
			expected: 1,
		},
		{
			name:     "removing from an empty set stays at zero",
			setup:    func(s *State) { s.SetChunks(usersStream(), NewSet[Chunk]()) },
			chunk:    first,
			expected: 0,
		},

		// ===== -1 means "never chunked", which is not the same as "zero remaining" =====
		{
			name:     "unknown stream returns -1",
			setup:    func(_ *State) {},
			chunk:    first,
			expected: -1,
		},
		{
			name:     "known stream with no chunks recorded returns -1",
			setup:    func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			chunk:    first,
			expected: -1,
		},

		// ===== scoping =====
		{
			name: "removal does not touch other streams",
			setup: func(s *State) {
				s.SetChunks(usersStream(), NewSet(first, second))
				s.SetChunks(ordersStream(), NewSet(first))
			},
			chunk:             first,
			expected:          1,
			checkOrders:       true,
			expectedOrdersLen: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, StreamType)
			tc.setup(state)

			assert.Equal(t, tc.expected, state.RemoveChunk(usersStream(), tc.chunk))

			if tc.expected >= 0 {
				require.NotNil(t, state.GetChunks(usersStream()))
				assert.Equal(t, tc.expected, state.GetChunks(usersStream()).Len())
			}
			if tc.checkOrders {
				require.NotNil(t, state.GetChunks(ordersStream()))
				assert.Equal(t, tc.expectedOrdersLen, state.GetChunks(ordersStream()).Len())
				assert.True(t, state.GetChunks(ordersStream()).Exists(tc.chunk))
			}
		})
	}
}

// TestStateHasCompletedBackfill tests the HasCompletedBackfill function
func TestStateHasCompletedBackfill(t *testing.T) {
	tests := []struct {
		name      string
		stateType StateType
		setup     func(s *State)
		expected  bool
	}{
		// ===== GLOBAL: decided by membership of the global stream set =====
		{
			name:      "global - no global state",
			stateType: GlobalType,
			setup:     func(_ *State) {},
			expected:  false,
		},
		{
			name:      "global - global state without streams",
			stateType: GlobalType,
			setup:     func(s *State) { s.Global = &GlobalState{State: "token"} },
			expected:  false,
		},
		{
			name:      "global - stream not tracked",
			stateType: GlobalType,
			setup:     func(s *State) { s.SetGlobal("token", ordersStream().ID()) },
			expected:  false,
		},
		{
			name:      "global - stream tracked",
			stateType: GlobalType,
			setup:     func(s *State) { s.SetGlobal("token", usersStream().ID(), ordersStream().ID()) },
			expected:  true,
		},
		{
			name:      "global - chunks pending are irrelevant",
			stateType: GlobalType,
			setup: func(s *State) {
				s.SetGlobal("token", usersStream().ID())
				s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10}))
			},
			expected: true,
		},

		// ===== STREAM: decided by chunks remaining =====
		{
			name:      "stream - nothing recorded",
			stateType: StreamType,
			setup:     func(_ *State) {},
			expected:  false,
		},
		{
			name:      "stream - known but never chunked",
			stateType: StreamType,
			setup:     func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			expected:  false,
		},
		{
			name:      "stream - chunks still pending",
			stateType: StreamType,
			setup:     func(s *State) { s.SetChunks(usersStream(), NewSet(Chunk{Min: 1, Max: 10})) },
			expected:  false,
		},
		{
			name:      "stream - all chunks consumed",
			stateType: StreamType,
			setup: func(s *State) {
				chunk := Chunk{Min: 1, Max: 10}
				s.SetChunks(usersStream(), NewSet(chunk))
				s.RemoveChunk(usersStream(), chunk)
			},
			expected: true,
		},
		{
			name:      "stream - chunk set explicitly emptied",
			stateType: StreamType,
			setup:     func(s *State) { s.SetChunks(usersStream(), NewSet[Chunk]()) },
			expected:  true,
		},
		{
			name:      "stream - global membership is ignored",
			stateType: StreamType,
			setup:     func(s *State) { s.SetGlobal("token", usersStream().ID()) },
			expected:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, tc.stateType)
			tc.setup(state)

			assert.Equal(t, tc.expected, state.HasCompletedBackfill(usersStream()))
		})
	}
}

// TestStateMarshalJSON tests the State MarshalJSON function
func TestStateMarshalJSON(t *testing.T) {
	tests := []struct {
		name             string
		stateType        StateType
		setup            func(s *State)
		expectedAbsent   []string
		expectedStreams  []string
		expectedGlobal   any
		expectedVersion  int
		expectedCursors  map[string]any
		expectedChunkLen int // -1 when no chunk set is expected
	}{
		// ===== empty state =====
		{
			name:             "zero state omits streams and global",
			stateType:        StreamType,
			setup:            func(_ *State) {},
			expectedAbsent:   []string{"streams", "global"},
			expectedChunkLen: -1,
		},

		// ===== streams that never received a value are dropped =====
		{
			name:      "streams without a value are not persisted",
			stateType: StreamType,
			setup: func(s *State) {
				s.SetCursor(usersStream(), "updated_at", "2024-01-01")
				// a stream registered during discovery that never received a value
				s.Streams = append(s.Streams, s.initStreamState(ordersStream()))
			},
			expectedStreams:  []string{"users"},
			expectedCursors:  map[string]any{"updated_at": "2024-01-01"},
			expectedChunkLen: -1,
		},
		{
			name:      "cursors and chunks both round-trip",
			stateType: StreamType,
			setup: func(s *State) {
				s.SetCursor(usersStream(), "updated_at", "2024-01-01")
				s.SetChunks(usersStream(), NewSet(Chunk{Min: "a", Max: "m"}))
			},
			expectedStreams:  []string{"users"},
			expectedCursors:  map[string]any{"updated_at": "2024-01-01"},
			expectedChunkLen: 1,
		},

		// ===== global state =====
		{
			name:      "global state and version round-trip",
			stateType: GlobalType,
			setup: func(s *State) {
				s.Version = 2
				s.SetGlobal(map[string]any{"lsn": "0/16B3748"}, usersStream().ID())
			},
			expectedGlobal:   map[string]any{"lsn": "0/16B3748"},
			expectedVersion:  2,
			expectedChunkLen: -1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t, tc.stateType)
			tc.setup(state)

			raw, err := json.Marshal(state)
			require.NoError(t, err)

			var asMap map[string]any
			require.NoError(t, json.Unmarshal(raw, &asMap))
			for _, key := range tc.expectedAbsent {
				assert.NotContains(t, asMap, key)
			}

			decoded := &State{RWMutex: &sync.RWMutex{}}
			require.NoError(t, json.Unmarshal(raw, decoded))
			assert.Equal(t, tc.stateType, decoded.Type)
			assert.Equal(t, tc.expectedVersion, decoded.Version)

			require.Len(t, decoded.Streams, len(tc.expectedStreams))
			for i, name := range tc.expectedStreams {
				assert.Equal(t, name, decoded.Streams[i].Stream)
			}
			for key, expected := range tc.expectedCursors {
				assert.Equal(t, expected, decoded.GetCursor(usersStream(), key))
			}

			if tc.expectedChunkLen < 0 {
				assert.Nil(t, decoded.GetChunks(usersStream()))
			} else {
				require.NotNil(t, decoded.GetChunks(usersStream()))
				assert.Equal(t, tc.expectedChunkLen, decoded.GetChunks(usersStream()).Len())
			}

			if tc.expectedGlobal != nil {
				require.NotNil(t, decoded.Global)
				assert.Equal(t, tc.expectedGlobal, decoded.Global.State)
				assert.True(t, decoded.Global.Streams.Exists(usersStream().ID()))
			}
		})
	}
}

// TestStreamStateMarshalJSON tests the StreamState MarshalJSON function
func TestStreamStateMarshalJSON(t *testing.T) {
	tests := []struct {
		name          string
		build         func() *StreamState
		expectedState map[string]any
	}{
		// ===== cursor values =====
		{
			name: "cursor values are flattened out of the sync.Map",
			build: func() *StreamState {
				s := &StreamState{Stream: "users", Namespace: "public", SyncMode: string(INCREMENTAL)}
				s.State.Store("updated_at", "2024-01-01")
				s.State.Store("id", float64(42))
				return s
			},
			expectedState: map[string]any{"updated_at": "2024-01-01", "id": float64(42)},
		},
		{
			name: "an empty state encodes as an empty object",
			build: func() *StreamState {
				return &StreamState{Stream: "users", Namespace: "public"}
			},
			expectedState: map[string]any{},
		},

		// ===== chunks =====
		{
			name: "chunks encode as an array",
			build: func() *StreamState {
				s := &StreamState{Stream: "users", Namespace: "public"}
				s.State.Store(ChunksKey, NewSet(Chunk{Min: "a", Max: "m"}))
				return s
			},
			expectedState: map[string]any{
				ChunksKey: []any{map[string]any{"min": "a", "max": "m"}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.build()

			raw, err := json.Marshal(original)
			require.NoError(t, err)

			var decoded struct {
				Stream    string         `json:"stream"`
				Namespace string         `json:"namespace"`
				SyncMode  string         `json:"sync_mode"`
				State     map[string]any `json:"state"`
			}
			require.NoError(t, json.Unmarshal(raw, &decoded))
			assert.Equal(t, original.Stream, decoded.Stream)
			assert.Equal(t, original.Namespace, decoded.Namespace)
			assert.Equal(t, original.SyncMode, decoded.SyncMode)
			assert.Equal(t, tc.expectedState, decoded.State)
		})
	}
}

// TestStreamStateUnmarshalJSON tests the StreamState UnmarshalJSON function
func TestStreamStateUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		expectedErr     bool
		expectedStream  string
		expectedHolds   bool
		expectedCursors map[string]any
		expectedChunks  []Chunk // nil when no *Set[Chunk] is expected
		expectedChunkOK bool
	}{
		// ===== cursor values =====
		{
			name:            "cursor values populate the sync.Map",
			input:           `{"stream":"users","namespace":"public","sync_mode":"incremental","state":{"updated_at":"2024-01-01"}}`,
			expectedStream:  "users",
			expectedHolds:   true,
			expectedCursors: map[string]any{"updated_at": "2024-01-01"},
		},
		{
			name:           "an empty state does not mark the stream as holding a value",
			input:          `{"stream":"users","namespace":"public","state":{}}`,
			expectedStream: "users",
			expectedHolds:  false,
		},
		{
			name:           "a missing state key is tolerated",
			input:          `{"stream":"users","namespace":"public"}`,
			expectedStream: "users",
			expectedHolds:  false,
		},

		// ===== chunks =====
		{
			name:            "a chunk array is rehydrated into a chunk set",
			input:           `{"stream":"users","namespace":"public","state":{"chunks":[{"min":"a","max":"m"},{"min":"m","max":"z"}]}}`,
			expectedStream:  "users",
			expectedHolds:   true,
			expectedChunks:  []Chunk{{Min: "a", Max: "m"}, {Min: "m", Max: "z"}},
			expectedChunkOK: true,
		},
		{
			name:            "an empty chunk array is rehydrated into an empty set",
			input:           `{"stream":"users","namespace":"public","state":{"chunks":[]}}`,
			expectedStream:  "users",
			expectedHolds:   true,
			expectedChunks:  []Chunk{},
			expectedChunkOK: true,
		},
		{
			// A malformed state file leaves a raw map behind rather than a Set. GetChunks
			// handles this safely and returns nil.
			name:            "a chunk object is not converted into a chunk set",
			input:           `{"stream":"users","namespace":"public","state":{"chunks":{"min":1,"max":10}}}`,
			expectedStream:  "users",
			expectedHolds:   true,
			expectedChunkOK: false,
		},

		// ===== malformed input =====
		{
			name:        "invalid json returns an error",
			input:       `{"stream":"users",`,
			expectedErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded := &StreamState{}
			err := json.Unmarshal([]byte(tc.input), decoded)

			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedStream, decoded.Stream)
			assert.Equal(t, tc.expectedHolds, decoded.HoldsValue.Load())

			for key, expected := range tc.expectedCursors {
				value, ok := decoded.State.Load(key)
				require.True(t, ok, "cursor key %s should be present", key)
				assert.Equal(t, expected, value)
			}

			stored, loaded := decoded.State.Load(ChunksKey)
			if !tc.expectedChunkOK {
				if loaded {
					_, isSet := stored.(*Set[Chunk])
					assert.False(t, isSet, "a non-array chunks value must not become a chunk set")
				}
				return
			}
			require.True(t, loaded)
			chunks, isSet := stored.(*Set[Chunk])
			require.True(t, isSet, "chunks must decode back into *Set[Chunk], not a raw slice")
			assert.Equal(t, len(tc.expectedChunks), chunks.Len())
			for _, chunk := range tc.expectedChunks {
				assert.True(t, chunks.Exists(chunk), "chunk %v should be present", chunk)
			}
		})
	}
}

// TestStateLogState tests the LogState and LogWithLock functions
func TestStateLogState(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(s *State)
		logExplicitly   bool
		expectedFile    bool
		expectedCursors map[string]any
	}{
		// ===== nothing to persist =====
		{
			name:          "an empty state is not written to disk",
			setup:         func(_ *State) {},
			logExplicitly: true,
			expectedFile:  false,
		},

		// ===== mutations flush automatically =====
		{
			name:            "a cursor write flushes the state file",
			setup:           func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			expectedFile:    true,
			expectedCursors: map[string]any{"updated_at": "2024-01-01"},
		},
		{
			name: "the newest value wins after repeated writes",
			setup: func(s *State) {
				s.SetCursor(usersStream(), "updated_at", "2024-01-01")
				s.SetCursor(usersStream(), "updated_at", "2024-02-01")
			},
			expectedFile:    true,
			expectedCursors: map[string]any{"updated_at": "2024-02-01"},
		},

		// ===== explicit logging =====
		{
			name:            "explicit logging rewrites the same file",
			setup:           func(s *State) { s.SetCursor(usersStream(), "updated_at", "2024-01-01") },
			logExplicitly:   true,
			expectedFile:    true,
			expectedCursors: map[string]any{"updated_at": "2024-01-01"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			old := viper.GetString(constants.StatePath)
			t.Cleanup(func() { viper.Set(constants.StatePath, old) })
			viper.Set(constants.StatePath, path)

			state := &State{RWMutex: &sync.RWMutex{}, Type: StreamType}
			tc.setup(state)
			if tc.logExplicitly {
				state.LogWithLock()
			}

			raw, err := os.ReadFile(path)
			if !tc.expectedFile {
				assert.True(t, os.IsNotExist(err), "no state file should be created for an empty state")
				return
			}
			require.NoError(t, err)

			decoded := &State{RWMutex: &sync.RWMutex{}}
			require.NoError(t, json.Unmarshal(raw, decoded))
			for key, expected := range tc.expectedCursors {
				assert.Equal(t, expected, decoded.GetCursor(usersStream(), key))
			}
		})
	}
}

// TestStateConcurrentAccess tests that the read/write locking holds under concurrent
// readers and writers. Meaningful under -race.
func TestStateConcurrentAccess(t *testing.T) {
	state := newTestState(t, StreamType)
	streams := []*ConfiguredStream{
		testStream("public", "users"),
		testStream("public", "orders"),
		testStream("sales", "users"),
	}
	// pre-create the stream states so readers have something to hit
	for _, stream := range streams {
		state.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 10}))
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		for _, stream := range streams {
			wg.Add(4)
			go func(stream *ConfiguredStream, value int) {
				defer wg.Done()
				state.SetCursor(stream, "updated_at", value)
			}(stream, i)
			go func(stream *ConfiguredStream) {
				defer wg.Done()
				state.GetCursor(stream, "updated_at")
			}(stream)
			go func(stream *ConfiguredStream) {
				defer wg.Done()
				state.GetChunks(stream)
			}(stream)
			go func() {
				defer wg.Done()
				state.SetGlobal("token", "public.users")
			}()
		}
	}
	wg.Wait()

	assert.Len(t, state.Streams, len(streams), "concurrent writers must not duplicate stream states")
	for _, stream := range streams {
		assert.NotNil(t, state.GetCursor(stream, "updated_at"))
		require.NotNil(t, state.GetChunks(stream))
		assert.Equal(t, 1, state.GetChunks(stream).Len())
	}
}
