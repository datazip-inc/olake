package types

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// ─── Test Helpers ─────────────────────────────────────────────────────────────

// newTestState creates a State with the mutex initialized and LogState wired to
// a throwaway temp file so tests never touch the real state path.
func newTestState(t *testing.T, typ StateType) *State {
	t.Helper()
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	return &State{
		RWMutex: &sync.RWMutex{},
		Type:    typ,
		Streams: []*StreamState{},
	}
}

// newTestStream builds a minimal ConfiguredStream. cursor is the CursorField
// string (e.g. "updated_at").
func newTestStream(name, namespace string, mode SyncMode, cursor string) *ConfiguredStream {
	return &ConfiguredStream{
		Stream: &Stream{
			Name:        name,
			Namespace:   namespace,
			SyncMode:    mode,
			CursorField: cursor,
		},
	}
}

// ─── Group 1: Basic State ─────────────────────────────────────────────────────

func TestStateIsZero(t *testing.T) {
	t.Run("bare state is zero", func(t *testing.T) {
		s := &State{RWMutex: &sync.RWMutex{}}
		assert.True(t, s.isZero())
	})

	t.Run("state with a stream entry is not zero", func(t *testing.T) {
		s := &State{
			RWMutex: &sync.RWMutex{},
			Streams: []*StreamState{{}},
		}
		assert.False(t, s.isZero())
	})

	t.Run("state with global is not zero", func(t *testing.T) {
		s := &State{
			RWMutex: &sync.RWMutex{},
			Global:  &GlobalState{},
		}
		assert.False(t, s.isZero())
	})
}

func TestStateSetType(t *testing.T) {
	s := newTestState(t, StreamType)
	assert.Equal(t, StreamType, s.Type)

	s.SetType(GlobalType)
	assert.Equal(t, GlobalType, s.Type)

	s.SetType(MixedType)
	assert.Equal(t, MixedType, s.Type)
}

func TestStateGetGlobal(t *testing.T) {
	t.Run("returns nil on fresh state", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		assert.Nil(t, s.GetGlobal())
	})

	t.Run("returns value after SetGlobal", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal(42)
		assert.NotNil(t, s.GetGlobal())
	})
}

func TestStateSetGlobal(t *testing.T) {
	t.Run("initializes global when nil", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal("cdc-offset-1", "stream_a", "stream_b")

		g := s.GetGlobal()
		assert.NotNil(t, g)
		assert.Equal(t, "cdc-offset-1", g.State)
		assert.True(t, g.Streams.Exists("stream_a"))
		assert.True(t, g.Streams.Exists("stream_b"))
	})

	t.Run("updates state and merges streams when global exists", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal("pos-1", "stream_a")
		s.SetGlobal("pos-2", "stream_b")

		g := s.GetGlobal()
		assert.Equal(t, "pos-2", g.State)
		assert.True(t, g.Streams.Exists("stream_a"), "original stream must be kept")
		assert.True(t, g.Streams.Exists("stream_b"), "new stream must be added")
	})

	t.Run("nil state does not overwrite existing value", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal("pos-1", "stream_a")
		s.SetGlobal(nil) // no state, no streams — should be a no-op on the value

		assert.Equal(t, "pos-1", s.GetGlobal().State)
	})

	t.Run("no streams arg does not reset existing streams", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal("pos-1", "stream_a")
		s.SetGlobal("pos-2") // update value but pass no streams

		g := s.GetGlobal()
		assert.Equal(t, "pos-2", g.State)
		assert.True(t, g.Streams.Exists("stream_a"), "existing stream must survive")
	})
}

func TestStateResetStreams(t *testing.T) {
	s := newTestState(t, StreamType)
	stream := newTestStream("users", "public", INCREMENTAL, "updated_at")

	s.SetCursor(stream, "updated_at", "2025-01-01")
	assert.NotEmpty(t, s.Streams, "streams should be populated after SetCursor")

	s.ResetStreams()
	assert.Empty(t, s.Streams, "ResetStreams should empty the slice")
}

// ─── Group 2: Cursor Management ───────────────────────────────────────────────

func TestStateSetCursor(t *testing.T) {
	t.Run("empty key is a no-op", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "", "value")
		assert.Empty(t, s.Streams, "empty key should not create a stream entry")
	})

	t.Run("creates new stream entry when stream not yet in state", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")

		assert.Len(t, s.Streams, 1)
		assert.Equal(t, "users", s.Streams[0].Stream)
		assert.Equal(t, "public", s.Streams[0].Namespace)
		assert.True(t, s.Streams[0].HoldsValue.Load(), "HoldsValue should be true after SetCursor")
	})

	t.Run("updates value on existing stream without duplicating", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		s.SetCursor(stream, "updated_at", "2025-06-01")

		assert.Len(t, s.Streams, 1, "second SetCursor must not create a duplicate entry")
		assert.Equal(t, "2025-06-01", s.GetCursor(stream, "updated_at"))
	})

	t.Run("multiple keys stored on same stream", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		s.SetCursor(stream, "id", int64(999))

		assert.Len(t, s.Streams, 1)
		assert.Equal(t, "2025-01-01", s.GetCursor(stream, "updated_at"))
		assert.Equal(t, int64(999), s.GetCursor(stream, "id"))
	})

	t.Run("different streams create separate entries", func(t *testing.T) {
		s := newTestState(t, StreamType)
		users := newTestStream("users", "public", INCREMENTAL, "updated_at")
		orders := newTestStream("orders", "public", INCREMENTAL, "order_date")
		s.SetCursor(users, "updated_at", "2025-01-01")
		s.SetCursor(orders, "order_date", "2025-02-01")

		assert.Len(t, s.Streams, 2)
	})
}

func TestStateGetCursor(t *testing.T) {
	t.Run("empty key returns nil", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		assert.Nil(t, s.GetCursor(stream, ""))
	})

	t.Run("missing stream returns nil", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		assert.Nil(t, s.GetCursor(stream, "updated_at"))
	})

	t.Run("missing key on existing stream returns nil", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		assert.Nil(t, s.GetCursor(stream, "nonexistent_key"))
	})

	t.Run("returns stored value", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-06-19")
		assert.Equal(t, "2025-06-19", s.GetCursor(stream, "updated_at"))
	})

	t.Run("same stream name in different namespace is isolated", func(t *testing.T) {
		s := newTestState(t, StreamType)
		streamA := newTestStream("users", "schema_a", INCREMENTAL, "updated_at")
		streamB := newTestStream("users", "schema_b", INCREMENTAL, "updated_at")
		s.SetCursor(streamA, "updated_at", "2025-01-01")

		assert.Nil(t, s.GetCursor(streamB, "updated_at"), "different namespace must not bleed into each other")
	})
}

func TestStateResetCursor(t *testing.T) {
	t.Run("removes cursor key from existing stream", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		assert.NotNil(t, s.GetCursor(stream, "updated_at"))

		s.ResetCursor(stream)
		assert.Nil(t, s.GetCursor(stream, "updated_at"), "cursor should be gone after reset")
	})

	t.Run("no-op and no panic on missing stream", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		assert.NotPanics(t, func() { s.ResetCursor(stream) })
	})

	t.Run("only cursor fields are removed, other keys survive", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		s.SetCursor(stream, "extra_key", "keep_me")

		s.ResetCursor(stream)
		assert.Nil(t, s.GetCursor(stream, "updated_at"))
		assert.Equal(t, "keep_me", s.GetCursor(stream, "extra_key"), "non-cursor keys must survive reset")
	})
}

// ─── Group 3: Chunk Management ────────────────────────────────────────────────

func TestStateSetChunks(t *testing.T) {
	t.Run("full refresh mode is a no-op", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", FULLREFRESH, "")
		chunks := NewSet(Chunk{Min: 1, Max: 100})
		s.SetChunks(stream, chunks)
		assert.Empty(t, s.Streams, "SetChunks must be skipped for FULLREFRESH streams")
	})

	t.Run("creates stream entry with chunks", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		chunks := NewSet(Chunk{Min: 1, Max: 100}, Chunk{Min: 101, Max: 200})
		s.SetChunks(stream, chunks)

		assert.Len(t, s.Streams, 1)
		got := s.GetChunks(stream)
		assert.NotNil(t, got)
		assert.Equal(t, 2, got.Len())
	})

	t.Run("replaces chunks on existing stream", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")

		s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 100}))
		s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 50}, Chunk{Min: 51, Max: 100}, Chunk{Min: 101, Max: 200}))

		assert.Len(t, s.Streams, 1, "must not create a duplicate stream entry")
		assert.Equal(t, 3, s.GetChunks(stream).Len())
	})

	t.Run("marks stream as HoldsValue true", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 100}))
		assert.True(t, s.Streams[0].HoldsValue.Load())
	})
}

func TestStateGetChunks(t *testing.T) {
	t.Run("returns nil when stream not in state", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		assert.Nil(t, s.GetChunks(stream))
	})

	t.Run("returns nil when stream exists but has no chunks key", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01") // stream exists, but no chunks stored
		assert.Nil(t, s.GetChunks(stream))
	})

	t.Run("returns stored chunk set", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 100}, Chunk{Min: 101, Max: 200}))

		got := s.GetChunks(stream)
		assert.NotNil(t, got)
		assert.Equal(t, 2, got.Len())
	})
}

func TestStateRemoveChunk(t *testing.T) {
	t.Run("returns -1 when stream not in state", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		assert.Equal(t, -1, s.RemoveChunk(stream, Chunk{Min: 1, Max: 100}))
	})

	t.Run("returns -1 when stream exists but has no chunks key", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		assert.Equal(t, -1, s.RemoveChunk(stream, Chunk{Min: 1, Max: 100}))
	})

	t.Run("removes a chunk and returns remaining count", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		chunk1 := Chunk{Min: 1, Max: 100}
		chunk2 := Chunk{Min: 101, Max: 200}
		s.SetChunks(stream, NewSet(chunk1, chunk2))

		remaining := s.RemoveChunk(stream, chunk1)
		assert.Equal(t, 1, remaining)
		assert.Equal(t, 1, s.GetChunks(stream).Len())
	})

	t.Run("removing the last chunk returns zero and leaves empty set", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		chunk := Chunk{Min: 1, Max: 100}
		s.SetChunks(stream, NewSet(chunk))

		remaining := s.RemoveChunk(stream, chunk)
		assert.Equal(t, 0, remaining)

		got := s.GetChunks(stream)
		assert.NotNil(t, got, "chunk set must still exist after last removal")
		assert.Equal(t, 0, got.Len())
	})

	t.Run("removing absent chunk leaves count unchanged", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		chunk1 := Chunk{Min: 1, Max: 100}
		chunk2 := Chunk{Min: 101, Max: 200}
		s.SetChunks(stream, NewSet(chunk1))

		remaining := s.RemoveChunk(stream, chunk2) // chunk2 was never added
		assert.Equal(t, 1, remaining, "count must not change when chunk was not present")
	})
}

// ─── Group 4: Backfill Logic ──────────────────────────────────────────────────

func TestStateHasCompletedBackfill(t *testing.T) {
	// ── GlobalType path ───────────────────────────────────────────────────────

	t.Run("global: returns false when Global is nil", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		assert.False(t, s.HasCompletedBackfill(stream))
	})

	t.Run("global: returns false when stream ID not present in Global.Streams", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		users := newTestStream("users", "public", INCREMENTAL, "updated_at")
		orders := newTestStream("orders", "public", INCREMENTAL, "order_date")

		s.SetGlobal("cdc-offset-1", orders.ID()) // only orders is registered
		assert.False(t, s.HasCompletedBackfill(users))
	})

	t.Run("global: returns true when stream ID is present in Global.Streams", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")

		s.SetGlobal("cdc-offset-1", stream.ID())
		assert.True(t, s.HasCompletedBackfill(stream))
	})

	t.Run("global: returns true only for the registered stream, not others", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		users := newTestStream("users", "public", INCREMENTAL, "updated_at")
		orders := newTestStream("orders", "public", INCREMENTAL, "order_date")

		s.SetGlobal("cdc-offset-1", users.ID())
		assert.True(t, s.HasCompletedBackfill(users))
		assert.False(t, s.HasCompletedBackfill(orders))
	})

	// ── StreamType path ───────────────────────────────────────────────────────

	t.Run("stream: returns false when stream has no chunks stored at all", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		assert.False(t, s.HasCompletedBackfill(stream))
	})

	t.Run("stream: returns false when chunks remain", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 100}, Chunk{Min: 101, Max: 200}))
		assert.False(t, s.HasCompletedBackfill(stream))
	})

	t.Run("stream: returns true after all chunks are removed one by one", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		chunk1 := Chunk{Min: 1, Max: 100}
		chunk2 := Chunk{Min: 101, Max: 200}
		s.SetChunks(stream, NewSet(chunk1, chunk2))

		s.RemoveChunk(stream, chunk1)
		assert.False(t, s.HasCompletedBackfill(stream), "one chunk still remaining")

		s.RemoveChunk(stream, chunk2)
		assert.True(t, s.HasCompletedBackfill(stream), "all chunks gone — backfill complete")
	})

	t.Run("stream: returns true when an empty chunk set is stored directly", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		s.SetChunks(stream, NewSet[Chunk]()) // explicit type arg needed — no args to infer from
		assert.True(t, s.HasCompletedBackfill(stream))
	})

	t.Run("stream: cursor-only stream (no chunks) is not considered complete", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01") // stream in state but no chunks key
		assert.False(t, s.HasCompletedBackfill(stream))
	})
}

// ─── Group 5: Serialization ───────────────────────────────────────────────────

func TestStateMarshalJSON(t *testing.T) {
	t.Run("empty state marshals without error", func(t *testing.T) {
		s := newTestState(t, StreamType)
		data, err := json.Marshal(s)
		assert.NoError(t, err)
		assert.NotEmpty(t, data)
	})

	t.Run("stream with HoldsValue false is excluded from JSON", func(t *testing.T) {
		s := newTestState(t, StreamType)
		// bypass SetCursor so HoldsValue stays false
		s.Streams = append(s.Streams, &StreamState{
			Stream:    "ghost",
			Namespace: "public",
		})

		data, err := json.Marshal(s)
		assert.NoError(t, err)

		var out struct {
			Streams []json.RawMessage `json:"streams"`
		}
		assert.NoError(t, json.Unmarshal(data, &out))
		assert.Empty(t, out.Streams, "HoldsValue=false stream must not appear in JSON")
	})

	t.Run("stream with HoldsValue true is included in JSON", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")

		data, err := json.Marshal(s)
		assert.NoError(t, err)

		var out struct {
			Streams []json.RawMessage `json:"streams"`
		}
		assert.NoError(t, json.Unmarshal(data, &out))
		assert.Len(t, out.Streams, 1)
	})

	t.Run("only HoldsValue=true streams survive when both types are present", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		// ghost stream added manually — HoldsValue stays false
		s.Streams = append(s.Streams, &StreamState{Stream: "ghost", Namespace: "public"})

		data, err := json.Marshal(s)
		assert.NoError(t, err)

		var out struct {
			Streams []json.RawMessage `json:"streams"`
		}
		assert.NoError(t, json.Unmarshal(data, &out))
		assert.Len(t, out.Streams, 1, "only the real stream should survive filtering")
	})

	t.Run("global state and type are preserved in JSON", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal("cdc-position-1", "stream_a")

		data, err := json.Marshal(s)
		assert.NoError(t, err)

		var out struct {
			Type   string          `json:"type"`
			Global json.RawMessage `json:"global"`
		}
		assert.NoError(t, json.Unmarshal(data, &out))
		assert.Equal(t, string(GlobalType), out.Type)
		assert.NotEmpty(t, out.Global)
	})
}

func TestStreamStateRoundTrip(t *testing.T) {
	t.Run("cursor value survives marshal then unmarshal", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-06-19")

		data, err := json.Marshal(s)
		assert.NoError(t, err)

		var restored State
		restored.RWMutex = &sync.RWMutex{}
		assert.NoError(t, json.Unmarshal(data, &restored))
		assert.Len(t, restored.Streams, 1)

		if len(restored.Streams) == 1 {
			val, loaded := restored.Streams[0].State.Load("updated_at")
			assert.True(t, loaded)
			assert.Equal(t, "2025-06-19", val)
		}
	})

	t.Run("HoldsValue is true after unmarshal when state has values", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-06-19")

		data, _ := json.Marshal(s)
		var restored State
		restored.RWMutex = &sync.RWMutex{}
		assert.NoError(t, json.Unmarshal(data, &restored))

		if len(restored.Streams) == 1 {
			assert.True(t, restored.Streams[0].HoldsValue.Load())
		}
	})

	t.Run("stream name and namespace survive round-trip", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("orders", "commerce", INCREMENTAL, "order_date")
		s.SetCursor(stream, "order_date", "2025-05-01")

		data, _ := json.Marshal(s)
		var restored State
		restored.RWMutex = &sync.RWMutex{}
		assert.NoError(t, json.Unmarshal(data, &restored))

		if len(restored.Streams) == 1 {
			assert.Equal(t, "orders", restored.Streams[0].Stream)
			assert.Equal(t, "commerce", restored.Streams[0].Namespace)
		}
	})

	t.Run("chunk set is restored as *Set[Chunk] after round-trip", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")
		s.SetChunks(stream, NewSet(Chunk{Min: 1, Max: 100}, Chunk{Min: 101, Max: 200}))

		data, err := json.Marshal(s)
		assert.NoError(t, err)

		var restored State
		restored.RWMutex = &sync.RWMutex{}
		assert.NoError(t, json.Unmarshal(data, &restored))
		assert.Len(t, restored.Streams, 1)

		if len(restored.Streams) == 1 {
			raw, ok := restored.Streams[0].State.Load(ChunksKey)
			assert.True(t, ok, "chunks key must be present after unmarshal")
			if ok {
				chunkSet, castOk := raw.(*Set[Chunk])
				assert.True(t, castOk, "value must be a *Set[Chunk], not raw JSON")
				assert.Equal(t, 2, chunkSet.Len())
			}
		}
	})

	t.Run("state type is preserved across round-trip", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal("pos-1", "stream_a")

		data, _ := json.Marshal(s)
		var restored State
		restored.RWMutex = &sync.RWMutex{}
		assert.NoError(t, json.Unmarshal(data, &restored))
		assert.Equal(t, GlobalType, restored.Type)
	})
}

// ─── Group 6: Edge Cases & Concurrency ───────────────────────────────────────

func TestStateEdgeCases(t *testing.T) {
	t.Run("SetCursor with nil value still creates the stream entry", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "cursor", nil)

		assert.Len(t, s.Streams, 1, "stream entry should exist even when cursor value is nil")
		assert.Nil(t, s.GetCursor(stream, "cursor"))
	})

	t.Run("SetGlobal with nil state on fresh State still creates GlobalState", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		s.SetGlobal(nil, "stream_a")

		g := s.GetGlobal()
		assert.NotNil(t, g)
		assert.Nil(t, g.State, "State field should be nil since nil was passed")
		assert.True(t, g.Streams.Exists("stream_a"))
	})

	t.Run("SetCursor → ResetStreams → SetCursor starts fresh cleanly", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")

		s.SetCursor(stream, "updated_at", "2025-01-01")
		s.ResetStreams()
		s.SetCursor(stream, "updated_at", "2025-06-01")

		assert.Len(t, s.Streams, 1)
		assert.Equal(t, "2025-06-01", s.GetCursor(stream, "updated_at"))
	})

	t.Run("HasCompletedBackfill on GlobalType with nil Global.Streams returns false", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		// Global exists but Streams is nil
		s.Global = &GlobalState{State: "pos-1", Streams: nil}
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		assert.False(t, s.HasCompletedBackfill(stream))
	})

	t.Run("registering same stream ID in global twice is idempotent", func(t *testing.T) {
		s := newTestState(t, GlobalType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetGlobal("pos-1", stream.ID())
		s.SetGlobal("pos-2", stream.ID()) // duplicate stream ID

		assert.True(t, s.HasCompletedBackfill(stream))
		assert.Equal(t, 1, s.GetGlobal().Streams.Len(), "Set must not duplicate the same stream ID")
	})

	t.Run("GetChunks returns nil for stream that only has cursor state", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		assert.Nil(t, s.GetChunks(stream))
	})

	t.Run("RemoveChunk on stream with cursor but no chunks returns -1", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "2025-01-01")
		assert.Equal(t, -1, s.RemoveChunk(stream, Chunk{Min: 1, Max: 100}))
	})
}

func TestStateConcurrency(t *testing.T) {
	t.Run("concurrent SetCursor on different keys does not lose any write", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")

		const goroutines = 50
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := range goroutines {
			go func(n int) {
				defer wg.Done()
				s.SetCursor(stream, fmt.Sprintf("key_%d", n), n)
			}(i)
		}
		wg.Wait()

		for i := range goroutines {
			val := s.GetCursor(stream, fmt.Sprintf("key_%d", i))
			assert.NotNil(t, val, "key_%d should be present after concurrent writes", i)
		}
	})

	t.Run("concurrent reads and writes do not deadlock", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "updated_at")
		s.SetCursor(stream, "updated_at", "initial")

		const goroutines = 50
		var wg sync.WaitGroup
		wg.Add(goroutines * 2)

		for i := range goroutines {
			go func(n int) {
				defer wg.Done()
				s.SetCursor(stream, "updated_at", fmt.Sprintf("value_%d", n))
			}(i)
			go func() {
				defer wg.Done()
				_ = s.GetCursor(stream, "updated_at")
			}()
		}
		wg.Wait()

		// reaching here without deadlock or panic is the assertion
		assert.NotNil(t, s.GetCursor(stream, "updated_at"))
	})

	t.Run("concurrent RemoveChunk drains the set to zero safely", func(t *testing.T) {
		s := newTestState(t, StreamType)
		stream := newTestStream("users", "public", INCREMENTAL, "id")

		const chunkCount = 20
		chunks := make([]Chunk, chunkCount)
		for i := range chunkCount {
			chunks[i] = Chunk{Min: i * 100, Max: (i + 1) * 100}
		}
		s.SetChunks(stream, NewSet(chunks...))

		var wg sync.WaitGroup
		wg.Add(chunkCount)
		for _, c := range chunks {
			go func(chunk Chunk) {
				defer wg.Done()
				s.RemoveChunk(stream, chunk)
			}(c)
		}
		wg.Wait()

		got := s.GetChunks(stream)
		assert.NotNil(t, got, "chunk set must still exist")
		assert.Equal(t, 0, got.Len(), "all chunks must be removed")
	})

	t.Run("concurrent SetGlobal calls accumulate all streams without corruption", func(t *testing.T) {
		s := newTestState(t, GlobalType)

		const goroutines = 30
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := range goroutines {
			go func(n int) {
				defer wg.Done()
				s.SetGlobal(fmt.Sprintf("pos-%d", n), fmt.Sprintf("stream_%d", n))
			}(i)
		}
		wg.Wait()

		g := s.GetGlobal()
		assert.NotNil(t, g)
		assert.Equal(t, goroutines, g.Streams.Len(), "all %d streams must be recorded", goroutines)
	})
}
