package legacywriter

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
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

type captureServer struct {
	requests []interface{}
}

func (c *captureServer) SendClientRequest(_ context.Context, reqPayload interface{}) (interface{}, error) {
	c.requests = append(c.requests, reqPayload)
	return &proto.RecordIngestResponse{
		Result:  "ok",
		Success: true,
		FilePositionMaps: []*proto.FilePositionMap{
			{
				FilePath: "data-file-1",
				Ranges:   []*proto.FilePositionMap_Range{{BatchStartIdx: 0, StartPosition: 0, Count: 1}},
			},
		},
	}, nil
}

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

func TestReinsertAfterDeleteTargetsTombstone(t *testing.T) {
	stream := &types.Stream{Name: "reinsert_demo", Namespace: "public"}
	stream.SourceDefinedPrimaryKey = types.NewSet[string]()
	stream.SourceDefinedPrimaryKey.Insert("id")

	index := &memIndex{locations: make(map[string]types.RowLocation)}
	server := &captureServer{}
	w := &LegacyWriter{
		options:     &destination.Options{ThreadID: "test"},
		schema:      map[string]string{"id": "int"},
		stream:      &staticStream{stream: stream},
		server:      server,
		upsertMode:  true,
		indexThread: types.NewStreamIndexThread(index),
	}

	deleteRecord := types.RawRecord{
		Data: map[string]any{"id": 3},
		OlakeColumns: map[string]any{
			constants.OpType:  "d",
			constants.OlakeID: "key-1",
		},
	}
	if err := w.Write(context.Background(), []types.RawRecord{deleteRecord}); err != nil {
		t.Fatalf("delete write failed: %s", err)
	}

	reinsert := types.RawRecord{
		Data: map[string]any{"id": 3},
		OlakeColumns: map[string]any{
			constants.OpType:  "c",
			constants.OlakeID: "key-1",
		},
	}
	if err := w.Write(context.Background(), []types.RawRecord{reinsert}); err != nil {
		t.Fatalf("re-insert write failed: %s", err)
	}

	if len(server.requests) != 2 {
		t.Fatalf("expected two server requests, got %d", len(server.requests))
	}
	payload := server.requests[1].(*proto.IcebergPayload)
	if len(payload.Records) != 1 {
		t.Fatalf("expected one record in the re-insert batch, got %d", len(payload.Records))
	}
	rec := payload.Records[0]
	if rec.DeleteFilePath == nil || rec.DeletePosition == nil {
		t.Fatalf("expected re-inserted row to positionally delete the tombstone, got file=%v position=%v", rec.DeleteFilePath, rec.DeletePosition)
	}
	if *rec.DeleteFilePath != "data-file-1" || *rec.DeletePosition != 0 {
		t.Fatalf("expected tombstone data-file-1:0 to be deleted, got %s:%d", *rec.DeleteFilePath, *rec.DeletePosition)
	}
}
