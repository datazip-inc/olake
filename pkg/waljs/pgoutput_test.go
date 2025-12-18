package waljs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
)

func TestNewPgoutputReplicator(t *testing.T) {
	tests := []struct {
		name                string
		workerCount         int
		expectedWorkerCount int
	}{
		{"zero workers defaults to 1", 0, 1},
		{"negative workers defaults to 1", -5, 1},
		{"single worker", 1, 1},
		{"multiple workers", 4, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newPgoutputReplicator(nil, "test_pub", tt.workerCount)
			if r.workerCount != tt.expectedWorkerCount {
				t.Errorf("expected workerCount %d, got %d", tt.expectedWorkerCount, r.workerCount)
			}
		})
	}
}

func TestProcessBatch(t *testing.T) {
	stream := &mockStream{id: "public.users"}
	socket := &Socket{
		changeFilter: ChangeFilter{
			tables: map[string]types.StreamInterface{
				"public.users": stream,
			},
			converter: func(value interface{}, columnType string) (interface{}, error) {
				return value, nil
			},
		},
	}

	r := newPgoutputReplicator(socket, "test_pub", 1)
	r.relationIDToMsgMap[1] = &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "users",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	batch := &transactionBatch{
		commitTime: time.Now(),
		changes: []walChange{
			{
				kind:       "insert",
				relationID: 1,
				data: &pglogrepl.InsertMessage{
					RelationID: 1,
					Tuple:      &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}},
				},
			},
		},
	}

	var receivedChanges []abstract.CDCChange
	var mu sync.Mutex
	insertFn := func(ctx context.Context, change abstract.CDCChange) error {
		mu.Lock()
		receivedChanges = append(receivedChanges, change)
		mu.Unlock()
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	if err != nil {
		t.Fatalf("processBatch failed: %v", err)
	}

	if len(receivedChanges) != 1 {
		t.Fatalf("expected 1 change, got %d", len(receivedChanges))
	}

	if receivedChanges[0].Kind != "insert" {
		t.Errorf("expected kind 'insert', got '%s'", receivedChanges[0].Kind)
	}
}

func TestBatchWorkerProcessesMultipleBatches(t *testing.T) {
	stream := &mockStream{id: "public.orders"}
	socket := &Socket{
		changeFilter: ChangeFilter{
			tables: map[string]types.StreamInterface{
				"public.orders": stream,
			},
			converter: func(value interface{}, columnType string) (interface{}, error) {
				return value, nil
			},
		},
	}

	r := newPgoutputReplicator(socket, "test_pub", 2)
	r.relationIDToMsgMap[2] = &pglogrepl.RelationMessage{
		RelationID:   2,
		Namespace:    "public",
		RelationName: "orders",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	batchChan := make(chan *transactionBatch, 10)
	var processedCount atomic.Int32

	insertFn := func(ctx context.Context, change abstract.CDCChange) error {
		processedCount.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for i := 0; i < 5; i++ {
			batchChan <- &transactionBatch{
				commitTime: time.Now(),
				changes: []walChange{
					{
						kind:       "insert",
						relationID: 2,
						data: &pglogrepl.InsertMessage{
							RelationID: 2,
							Tuple:      &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}},
						},
					},
				},
			}
		}
		close(batchChan)
	}()

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_ = r.batchWorker(ctx, batchChan, insertFn)

	if processedCount.Load() != 5 {
		t.Errorf("expected 5 processed changes, got %d", processedCount.Load())
	}
}

func TestTransactionBatchMaintainsOrder(t *testing.T) {
	stream := &mockStream{id: "public.items"}
	socket := &Socket{
		changeFilter: ChangeFilter{
			tables: map[string]types.StreamInterface{
				"public.items": stream,
			},
			converter: func(value interface{}, columnType string) (interface{}, error) {
				return value, nil
			},
		},
	}

	r := newPgoutputReplicator(socket, "test_pub", 1)
	r.relationIDToMsgMap[3] = &pglogrepl.RelationMessage{
		RelationID:   3,
		Namespace:    "public",
		RelationName: "items",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	batch := &transactionBatch{
		commitTime: time.Now(),
		changes: []walChange{
			{kind: "insert", relationID: 3, data: &pglogrepl.InsertMessage{RelationID: 3, Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}}}},
			{kind: "update", relationID: 3, data: &pglogrepl.UpdateMessage{RelationID: 3, NewTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("2")}}}}},
			{kind: "delete", relationID: 3, data: &pglogrepl.DeleteMessage{RelationID: 3, OldTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("3")}}}}},
		},
	}

	var order []string
	insertFn := func(ctx context.Context, change abstract.CDCChange) error {
		order = append(order, change.Kind)
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	if err != nil {
		t.Fatalf("processBatch failed: %v", err)
	}

	expectedOrder := []string{"insert", "update", "delete"}
	for i, kind := range order {
		if kind != expectedOrder[i] {
			t.Errorf("at index %d: expected '%s', got '%s'", i, expectedOrder[i], kind)
		}
	}
}

type mockStream struct {
	id string
}

func (m *mockStream) ID() string                                      { return m.id }
func (m *mockStream) Self() *types.ConfiguredStream                   { return nil }
func (m *mockStream) Name() string                                    { return m.id }
func (m *mockStream) Namespace() string                               { return "public" }
func (m *mockStream) Schema() *types.TypeSchema                       { return nil }
func (m *mockStream) GetStream() *types.Stream                        { return &types.Stream{} }
func (m *mockStream) GetSyncMode() types.SyncMode                     { return types.CDC }
func (m *mockStream) GetFilter() (types.Filter, error)                { return types.Filter{}, nil }
func (m *mockStream) SupportedSyncModes() *types.Set[types.SyncMode]  { return nil }
func (m *mockStream) Cursor() (string, string)                        { return "", "" }
func (m *mockStream) Validate(source *types.Stream) error             { return nil }
func (m *mockStream) NormalizationEnabled() bool                      { return false }
func (m *mockStream) GetDestinationDatabase(icebergDB *string) string { return "" }
func (m *mockStream) GetDestinationTable() string                     { return m.id }
