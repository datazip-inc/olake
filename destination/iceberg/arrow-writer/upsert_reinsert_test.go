package arrowwriter

import (
	"testing"

	"github.com/datazip-inc/olake/types"
)

type memIndex struct {
	locations map[string]types.RowLocation
}

func (m *memIndex) Lookup(key string) (types.RowLocation, bool, error) {
	loc, found := m.locations[key]
	return loc, found, nil
}

func (m *memIndex) Commit(_ *types.StreamIndexThread, _ *int64) error { return nil }
func (m *memIndex) LastCommittedSnapshot() (int64, error)             { return 0, nil }
func (m *memIndex) Truncate() error                                   { return nil }
func (m *memIndex) Close() error                                      { return nil }

type staticStream struct {
	stream *types.Stream
}

func (s *staticStream) ID() string                    { return s.stream.ID() }
func (s *staticStream) Self() *types.ConfiguredStream { return nil }
func (s *staticStream) Name() string                  { return s.stream.Name }
func (s *staticStream) Namespace() string             { return s.stream.Namespace }
func (s *staticStream) Schema() *types.TypeSchema     { return s.stream.Schema }
func (s *staticStream) GetStream() *types.Stream      { return s.stream }
func (s *staticStream) GetSyncMode() types.SyncMode   { return s.stream.SyncMode }

func (s *staticStream) GetFilter() (types.FilterConfig, bool, error) {
	return types.FilterConfig{}, false, nil
}

func (s *staticStream) SupportedSyncModes() *types.Set[types.SyncMode] {
	return s.stream.SupportedSyncModes
}

func (s *staticStream) Cursor() (string, string)       { return s.stream.CursorField, "" }
func (s *staticStream) Validate(_ *types.Stream) error { return nil }
func (s *staticStream) NormalizationEnabled() bool     { return false }
func (s *staticStream) GetDestinationDatabase(_ *string) string {
	return "db"
}
func (s *staticStream) GetDestinationTable() string     { return "table" }
func (s *staticStream) GetPartitionRegex() string       { return "" }
func (s *staticStream) GetUpdateType() types.UpdateType { return types.UpdateTypePosition }

func (s *staticStream) RetainSelectedColumns() func(map[string]interface{}) map[string]interface{} {
	return func(in map[string]interface{}) map[string]interface{} { return in }
}

func (s *staticStream) IsSelectedColumn() func(string) bool {
	return func(string) bool { return true }
}

func (s *staticStream) ResolveColumnName(key string) string { return key }

func newTestStream(withPK bool) *types.Stream {
	stream := &types.Stream{Name: "reinsert_demo", Namespace: "public"}
	stream.SourceDefinedPrimaryKey = types.NewSet[string]()
	if withPK {
		stream.SourceDefinedPrimaryKey.Insert("id")
	}
	return stream
}

func newTestWriter(withPK bool) (*ArrowWriter, *Writer, *memIndex) {
	index := &memIndex{locations: make(map[string]types.RowLocation)}
	w := &ArrowWriter{
		upsertMode:  true,
		indexThread: types.NewStreamIndexThread(index),
		stream:      &staticStream{stream: newTestStream(withPK)},
	}
	writer := &Writer{
		dataWriter: &RollingWriter{filePath: "data-file-1"},
	}
	return w, writer, index
}

func TestReinsertAfterDeleteEmitsPositionalDelete(t *testing.T) {
	w, writer, _ := newTestWriter(true)

	if err := w.indexRecord(writer, "key-1", "d", 0); err != nil {
		t.Fatalf("delete write failed: %s", err)
	}
	if err := w.indexRecord(writer, "key-1", "c", 1); err != nil {
		t.Fatalf("re-insert write failed: %s", err)
	}

	if len(writer.positionalDeletes) != 1 {
		t.Fatalf("expected exactly one positional delete against the soft-deleted row, got %d", len(writer.positionalDeletes))
	}
	got := writer.positionalDeletes[0]
	if got.FilePath != "data-file-1" || got.Position != 0 {
		t.Fatalf("expected tombstone data-file-1:0 to be deleted, got %s:%d", got.FilePath, got.Position)
	}
	loc, found, err := w.indexThread.Lookup("key-1")
	if err != nil || !found {
		t.Fatalf("expected re-inserted row in index, found=%v err=%v", found, err)
	}
	if loc.FilePath != "data-file-1" || loc.Position != 1 {
		t.Fatalf("expected index to point at re-inserted row data-file-1:1, got %s:%d", loc.FilePath, loc.Position)
	}
}

func TestReinsertWithoutPrimaryKeyStaysSeparate(t *testing.T) {
	w, writer, _ := newTestWriter(false)

	if err := w.indexRecord(writer, "hash-1", "c", 0); err != nil {
		t.Fatalf("first insert write failed: %s", err)
	}
	if err := w.indexRecord(writer, "hash-1", "c", 1); err != nil {
		t.Fatalf("second insert write failed: %s", err)
	}

	if len(writer.positionalDeletes) != 0 {
		t.Fatalf("expected no positional deletes for identical inserts of a pk-less stream, got %d", len(writer.positionalDeletes))
	}
}
