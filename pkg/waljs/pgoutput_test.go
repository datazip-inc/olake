package waljs

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// newTestReplicator creates a pgoutputReplicator with a mock socket for testing
func newTestReplicator(t *testing.T, tableName string, workerCount int) (*pgoutputReplicator, types.StreamInterface) {
	t.Helper()

	stream := &mockStream{id: tableName}
	socket := &Socket{
		changeFilter: ChangeFilter{
			tables: map[string]types.StreamInterface{
				tableName: stream,
			},
			converter: func(value interface{}, _ string) (interface{}, error) {
				return value, nil
			},
		},
	}

	r := newPgoutputReplicator(socket, "test_pub", workerCount)
	return r, stream
}

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
			require.Equal(t, tt.expectedWorkerCount, r.workerCount, "unexpected workerCount")
		})
	}
}

func TestProcessBatch(t *testing.T) {
	r, _ := newTestReplicator(t, "public.users", 1)
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
	insertFn := func(_ context.Context, change abstract.CDCChange) error {
		receivedChanges = append(receivedChanges, change)
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	require.NoError(t, err, "processBatch should not return error")
	require.Len(t, receivedChanges, 1, "expected exactly 1 change")
	require.Equal(t, "insert", receivedChanges[0].Kind, "unexpected change kind")
}

func TestProcessBatchSkipsUnknownStreams(t *testing.T) {
	r, _ := newTestReplicator(t, "public.users", 1)
	// register relation for a table NOT in the change filter
	r.relationIDToMsgMap[99] = &pglogrepl.RelationMessage{
		RelationID:   99,
		Namespace:    "public",
		RelationName: "unknown_table",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	batch := &transactionBatch{
		commitTime: time.Now(),
		changes: []walChange{
			{
				kind:       "insert",
				relationID: 99,
				data: &pglogrepl.InsertMessage{
					RelationID: 99,
					Tuple:      &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}},
				},
			},
		},
	}

	var count int
	insertFn := func(_ context.Context, change abstract.CDCChange) error {
		count++
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	require.NoError(t, err, "processBatch should skip unknown streams without error")
	require.Equal(t, 0, count, "no changes should be emitted for unknown streams")
}

func TestProcessBatchUnknownRelationID(t *testing.T) {
	r, _ := newTestReplicator(t, "public.users", 1)
	// do NOT register any relation, so relation ID lookup fails

	batch := &transactionBatch{
		commitTime: time.Now(),
		changes: []walChange{
			{
				kind:       "insert",
				relationID: 999,
				data: &pglogrepl.InsertMessage{
					RelationID: 999,
					Tuple:      &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}},
				},
			},
		},
	}

	insertFn := func(_ context.Context, _ abstract.CDCChange) error {
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	require.Error(t, err, "processBatch should error on unknown relation ID")
	require.Contains(t, err.Error(), "unknown relation id")
}

func TestProcessBatchInsertFnError(t *testing.T) {
	r, _ := newTestReplicator(t, "public.users", 1)
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

	expectedErr := fmt.Errorf("downstream write failure")
	insertFn := func(_ context.Context, _ abstract.CDCChange) error {
		return expectedErr
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	require.ErrorIs(t, err, expectedErr, "processBatch should propagate insertFn errors")
}

func TestBatchWorkerProcessesMultipleBatches(t *testing.T) {
	r, _ := newTestReplicator(t, "public.orders", 2)
	r.relationIDToMsgMap[2] = &pglogrepl.RelationMessage{
		RelationID:   2,
		Namespace:    "public",
		RelationName: "orders",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	batchChan := make(chan *transactionBatch, 10)
	var processedCount atomic.Int32

	insertFn := func(_ context.Context, _ abstract.CDCChange) error {
		processedCount.Add(1)
		return nil
	}

	// send 5 batches
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

	err := r.batchWorker(context.Background(), batchChan, insertFn)
	require.NoError(t, err, "batchWorker should process all batches without error")
	require.Equal(t, int32(5), processedCount.Load(), "expected 5 processed changes")
}

func TestBatchWorkerStopsOnContextCancel(t *testing.T) {
	r, _ := newTestReplicator(t, "public.orders", 1)

	batchChan := make(chan *transactionBatch, 10)
	insertFn := func(_ context.Context, _ abstract.CDCChange) error {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := r.batchWorker(ctx, batchChan, insertFn)
	require.NoError(t, err, "batchWorker should return nil on context cancellation")
}

func TestTransactionBatchMaintainsOrder(t *testing.T) {
	r, _ := newTestReplicator(t, "public.items", 1)
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
	insertFn := func(_ context.Context, change abstract.CDCChange) error {
		order = append(order, change.Kind)
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	require.NoError(t, err, "processBatch should not return error")
	require.Equal(t, []string{"insert", "update", "delete"}, order, "changes should maintain transaction order")
}

func TestProcessBatchNilTuple(t *testing.T) {
	r, _ := newTestReplicator(t, "public.users", 1)
	r.relationIDToMsgMap[1] = &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "users",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	// delete with nil OldTuple should not panic
	batch := &transactionBatch{
		commitTime: time.Now(),
		changes: []walChange{
			{
				kind:       "delete",
				relationID: 1,
				data:       &pglogrepl.DeleteMessage{RelationID: 1, OldTuple: nil},
			},
		},
	}

	var receivedChanges []abstract.CDCChange
	insertFn := func(_ context.Context, change abstract.CDCChange) error {
		receivedChanges = append(receivedChanges, change)
		return nil
	}

	err := r.processBatch(context.Background(), batch, insertFn)
	require.NoError(t, err, "processBatch should handle nil tuple without error")
	require.Len(t, receivedChanges, 1, "expected 1 change even with nil tuple")
}

func TestConcurrentBatchWorkers(t *testing.T) {
	r, _ := newTestReplicator(t, "public.users", 4)
	r.relationIDToMsgMap[1] = &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "users",
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}},
	}

	const totalBatches = 100

	var callCount atomic.Int64
	insertFn := func(_ context.Context, _ abstract.CDCChange) error {
		callCount.Add(1)
		return nil
	}

	batchChan := make(chan *transactionBatch, 8)

	g, gCtx := errgroup.WithContext(context.Background())
	for i := 0; i < r.workerCount; i++ {
		g.Go(func() error {
			return r.batchWorker(gCtx, batchChan, insertFn)
		})
	}

	for i := 0; i < totalBatches; i++ {
		batchChan <- &transactionBatch{
			commitTime: time.Now(),
			changes: []walChange{{
				kind:       "insert",
				relationID: 1,
				data: &pglogrepl.InsertMessage{
					RelationID: 1,
					Tuple:      &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}},
				},
			}},
		}
	}
	close(batchChan)

	err := g.Wait()
	require.NoError(t, err)
	require.Equal(t, int64(totalBatches), callCount.Load(), "every batch should produce exactly one insertFn call")
}

// mockStream implements types.StreamInterface for testing
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
