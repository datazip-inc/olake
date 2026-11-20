package waljs

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumericScale(t *testing.T) {
	testCases := []struct {
		name   string
		packed int64
		want   int64
	}{
		{
			name:   "a positive scale",
			packed: 4,
			want:   4,
		},
		{
			name:   "a zero scale",
			packed: 0,
			want:   0,
		},
		{
			name:   "a negative scale postgres 15 packs as two's complement",
			packed: 2046,
			want:   -2,
		},
		{
			name:   "the most negative scale postgres allows",
			packed: 1048,
			want:   -1000,
		},
		{
			name:   "the largest scale postgres allows",
			packed: 1000,
			want:   1000,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NumericScale(tc.packed))
		})
	}
}

func TestPgoutputReplicator_PrimaryKeyChanged(t *testing.T) {
	p := newTestPgoutputReplicator()
	rel := p.relationIDToMsgMap[1]
	null := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	toast := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
	old := testTuple(textColumn("1"), textColumn("east"))
	newRow := testTuple(textColumn("2"), textColumn("east"))
	tests := []struct {
		name     string
		keys     []string
		old, new *pglogrepl.TupleData
		changed  bool
	}{
		{"single key changes", []string{"id"}, old, newRow, true},
		{"one composite key component changes", []string{"region", "id"}, old, newRow, true},
		{"ordinary update", []string{"region"}, old, newRow, false},
		{"no old tuple", []string{"id"}, nil, newRow, false},
		{"no primary key", nil, old, newRow, false},
		{"key absent from relation", []string{"absent"}, old, newRow, false},
		{"INDEX omits second key", []string{"id", "region"}, testTuple(textColumn("1"), null), newRow, false},
		{"INDEX omits first key", []string{"region", "id"}, testTuple(textColumn("1"), null), newRow, false},
		{"empty text key", []string{"region"}, testTuple(textColumn("1"), textColumn("")), newRow, true},
		{"unchanged TOAST key", []string{"region"}, old, testTuple(textColumn("2"), toast), false},
		{"same numeric key with different scale", []string{"id"}, old, testTuple(textColumn("1.00"), null), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &pglogrepl.UpdateMessage{OldTuple: tt.old, NewTuple: tt.new}
			values, _, _, err := p.tupleValuesToMap(rel, m.NewTuple, m.OldTuple)
			require.NoError(t, err)
			changed, err := p.primaryKeyChanged(rel, m, tt.keys, values)
			require.NoError(t, err)
			assert.Equal(t, tt.changed, changed)
		})
	}
}

func TestPgoutputReplicator_EmitUpdate(t *testing.T) {
	p := newTestPgoutputReplicator("id")
	p.preserveToastValues = true
	toast := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
	null := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	next := testTuple(textColumn("2"), toast)

	for _, tt := range []struct {
		name        string
		oldType     byte
		old         *pglogrepl.TupleData
		region      string
		unavailable []string
	}{
		{"FULL supplies old region", 'O', testTuple(textColumn("1"), textColumn("east")), "east", nil},
		{"DEFAULT supplies only old key", 'K', testTuple(textColumn("1"), null), constants.UnavailableValue, []string{"region"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			msg := &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: tt.oldType, OldTuple: tt.old, NewTuple: next}
			var changes []abstract.CDCChange
			err := p.emitUpdate(t.Context(), msg, func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, changes, 2)
			assert.Equal(t, "delete", changes[0].Kind)
			assert.Equal(t, 1.0, changes[0].Data["id"])
			assert.Equal(t, "update", changes[1].Kind)
			assert.Equal(t, map[string]any{"id": 2.0, "region": tt.region}, changes[1].Data)
			assert.Equal(t, tt.unavailable, changes[1].UnavailableColumns)
		})
	}

	t.Run("ordinary update emits one event", func(t *testing.T) {
		msg := &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: next}
		var kinds []string
		err := p.emitUpdate(t.Context(), msg, func(_ context.Context, change abstract.CDCChange) error {
			kinds = append(kinds, change.Kind)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"update"}, kinds)
	})

	for _, failKind := range []string{"delete", "update"} {
		t.Run("failed "+failKind+" stops delivery", func(t *testing.T) {
			failure := errors.New("destination write failed")
			msg := &pglogrepl.UpdateMessage{RelationID: 1, OldTuple: testTuple(textColumn("1"), null), NewTuple: next}
			var kinds []string
			err := p.emitUpdate(t.Context(), msg, func(_ context.Context, change abstract.CDCChange) error {
				kinds = append(kinds, change.Kind)
				if change.Kind == failKind {
					return failure
				}
				return nil
			})
			require.ErrorIs(t, err, failure)
			assert.Equal(t, failKind, kinds[len(kinds)-1])
		})
	}

	t.Run("old key conversion fails before delivery", func(t *testing.T) {
		msg := &pglogrepl.UpdateMessage{RelationID: 1, OldTuple: testTuple(textColumn("invalid"), null), NewTuple: next}
		err := p.emitUpdate(t.Context(), msg, func(context.Context, abstract.CDCChange) error {
			t.Fatal("unexpected change")
			return nil
		})
		require.Error(t, err)
	})
}

func TestPgoutputReplicator_EmitDelete(t *testing.T) {
	// An identity index can supply region without the configured primary key.
	p := newTestPgoutputReplicator("id")
	msg := &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K',
		OldTuple: testTuple(&pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}, textColumn("east")),
	}
	var changes []abstract.CDCChange
	err := p.emitDelete(t.Context(), msg, func(_ context.Context, change abstract.CDCChange) error {
		changes = append(changes, change)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "delete", changes[0].Kind)
	assert.Equal(t, map[string]any{"id": nil, "region": "east"}, changes[0].Data)
}

func newTestPgoutputReplicator(keys ...string) *pgoutputReplicator {
	stream := types.NewStream("orders", "public", nil).WithPrimaryKey(keys...).Wrap(0)
	return &pgoutputReplicator{
		socket: &Socket{changeFilter: NewChangeFilter(func(value any, columnType string) (any, error) {
			if columnType == "numeric" {
				return typeutils.ReformatFloat64(value)
			}
			return value, nil
		}, stream)},
		relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
			1: {RelationID: 1, Namespace: "public", RelationName: "orders", Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.NumericOID}, {Name: "region", DataType: pgtype.TextOID},
			}},
		},
	}
}

func testTuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: cols}
}

func textColumn(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}
