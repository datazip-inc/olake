package waljs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPgoutputReplicator_EmitUpdate(t *testing.T) {
	text := func(value string) *pglogrepl.TupleDataColumn {
		return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(value)}
	}
	tuple := func(columns ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
		return &pglogrepl.TupleData{ColumnNum: 4, Columns: columns}
	}
	null := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	toast := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
	oldRow := tuple(text("before"), text("1"), text("10"), text("large value"))
	newRow := tuple(text("after"), text("2"), text("10"), text("large value"))
	tests := []struct {
		name        string
		keys        []string
		oldType     byte
		old, new    *pglogrepl.TupleData
		wantDelete  bool
		wantPayload string
	}{
		{"single key with DEFAULT identity", []string{"id"}, 'K', tuple(null, text("1"), null, null), newRow, true, "large value"},
		{"single key with FULL identity", []string{"id"}, 'O', oldRow, newRow, true, "large value"},
		{"composite key component changes", []string{"id", "tenant"}, 'K', tuple(null, text("2"), text("9"), null), newRow, true, "large value"},
		{"ordinary FULL update", []string{"id"}, 'O', oldRow, tuple(text("after"), text("1"), text("10"), text("large value")), false, "large value"},
		{"ordinary DEFAULT update without old tuple", []string{"id"}, 0, nil, newRow, false, "large value"},
		{"no primary key", nil, 'O', oldRow, newRow, false, "large value"},
		{"changed key retains unchanged TOAST", []string{"id"}, 'O', oldRow, tuple(text("after"), text("2"), text("10"), toast), true, "large value"},
		{"unavailable TOAST without old tuple", []string{"id"}, 0, nil, tuple(text("after"), text("2"), text("10"), toast), false, olakeUnavailableValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, stream := newUpdateReplicator(tt.keys...)
			if tt.oldType != 'O' {
				p.relationIDToMsgMap[1].ReplicaIdentity = 'd'
				for _, column := range p.relationIDToMsgMap[1].Columns {
					if !stream.Stream.SourceDefinedPrimaryKey.Exists(column.Name) {
						column.Flags = 0
					}
				}
			}
			message := &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: tt.oldType, OldTuple: tt.old, NewTuple: tt.new}
			var changes []abstract.CDCChange
			err := p.emitUpdate(context.Background(), message, func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			})
			require.NoError(t, err)
			wantKinds := []string{"update"}
			if tt.wantDelete {
				wantKinds = []string{"delete", "update"}
			}
			require.Len(t, changes, len(wantKinds))
			for i, change := range changes {
				assert.Equal(t, wantKinds[i], change.Kind)
				assert.Same(t, stream, change.Stream)
				assert.Equal(t, p.txnCommitTime, change.Timestamp)
				assert.Equal(t, map[string]any{CDCLSN: p.socket.ClientXLogPos.String()}, change.ExtraColumns)
			}
			updated := changes[len(changes)-1].Data
			assert.Equal(t, "after", updated["name"])
			assert.Equal(t, string(tt.new.Columns[1].Data), updated["id"])
			assert.Equal(t, tt.wantPayload, updated["payload"])
			if tt.wantDelete {
				// The two events must address distinct destination rows, in delete-before-update order.
				assert.Equal(t, string(tt.old.Columns[1].Data), changes[0].Data["id"])
				if len(tt.keys) > 1 {
					assert.Equal(t, string(tt.old.Columns[2].Data), changes[0].Data["tenant"])
					assert.Equal(t, string(tt.new.Columns[2].Data), updated["tenant"])
				}
				assert.NotEqual(t, utils.GetKeysHash(changes[0].Data, tt.keys...), utils.GetKeysHash(updated, tt.keys...))
			}
		})
	}

	t.Run("old tuple cannot identify the primary key", func(t *testing.T) {
		p, _ := newUpdateReplicator("id")
		relation := p.relationIDToMsgMap[1]
		relation.ReplicaIdentity = 'i'
		for _, column := range relation.Columns {
			column.Flags = 0
		}
		relation.Columns[2].Flags = 1 // The replica identity index covers tenant, not id.
		message := &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tuple(null, null, text("9"), null), NewTuple: newRow}
		err := p.emitUpdate(context.Background(), message, func(context.Context, abstract.CDCChange) error {
			t.Error("unidentifiable old row must fail before delivery")
			return nil
		})
		require.ErrorIs(t, err, constants.ErrNonRetryable)
		assert.ErrorContains(t, err, `old tuple for public.orders is missing primary key column "id"`)
	})

	for _, failKind := range []string{"delete", "update"} {
		t.Run(failKind+" callback failure", func(t *testing.T) {
			p, _ := newUpdateReplicator("id")
			failure := errors.New("destination write failed")
			var kinds []string
			err := p.emitUpdate(context.Background(), &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O', OldTuple: oldRow, NewTuple: newRow},
				func(_ context.Context, change abstract.CDCChange) error {
					kinds = append(kinds, change.Kind)
					if change.Kind == failKind {
						return failure
					}
					return nil
				})
			require.ErrorIs(t, err, failure)
			want := []string{"delete"}
			if failKind == "update" {
				want = append(want, "update")
			}
			assert.Equal(t, want, kinds)
		})
	}

	t.Run("conversion failure precedes delivery", func(t *testing.T) {
		p, _ := newUpdateReplicator("id")
		failure := errors.New("invalid column value")
		p.socket.changeFilter.converter = func(value any, _ string) (any, error) {
			if value == "before" {
				return nil, failure
			}
			return value, nil
		}
		err := p.emitUpdate(context.Background(), &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O', OldTuple: oldRow, NewTuple: newRow},
			func(context.Context, abstract.CDCChange) error {
				t.Error("conversion failure must not emit either event")
				return nil
			})
		require.ErrorIs(t, err, failure)
	})
}

func newUpdateReplicator(keys ...string) (*pgoutputReplicator, *types.ConfiguredStream) {
	stream := types.NewStream("orders", "public", nil).WithPrimaryKey(keys...).Wrap(0)
	relation := &pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "orders", ReplicaIdentity: 'f', ColumnNum: 4}
	for _, name := range []string{"name", "id", "tenant", "payload"} {
		// FULL identity marks every column, independently of the configured primary key.
		relation.Columns = append(relation.Columns, &pglogrepl.RelationMessageColumn{Name: name, DataType: pgtype.TextOID, Flags: 1})
	}
	return &pgoutputReplicator{
		socket: &Socket{ClientXLogPos: pglogrepl.LSN(100), changeFilter: NewChangeFilter(func(value any, _ string) (any, error) {
			return value, nil
		}, stream)},
		txnCommitTime:      time.Unix(1, 0).UTC(),
		relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{1: relation},
	}, stream
}
