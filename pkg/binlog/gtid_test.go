package binlog

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

const testSID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

func TestBinlog_Compare(t *testing.T) {
	tests := []struct {
		name, left, right string
		want              int
		invalid           bool
	}{
		{"equal", testSID + ":1-3", testSID + ":1-3", 0, false},
		{"ahead", testSID + ":1-4", testSID + ":1-3", 1, false},
		{"behind", testSID + ":1-2", testSID + ":1-3", -1, false},
		{"empty start", "", testSID + ":1", -1, false},
		{"holes are not ordered", testSID + ":1:3", testSID + ":1-2", 0, true},
		{"invalid set", "invalid", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// File positions on different servers cannot order GTID checkpoints.
			left := Binlog{GTIDSet: &tt.left, ServerUUID: "a", Position: mysql.Position{Name: "bin.000009", Pos: 500}}
			right := Binlog{GTIDSet: &tt.right, ServerUUID: "b", Position: mysql.Position{Name: "bin.000001", Pos: 10}}
			got, err := left.Compare(right)
			if tt.invalid {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
	t.Run("legacy is distinct from empty GTID checkpoint", func(t *testing.T) {
		var legacy, empty Binlog
		require.NoError(t, json.Unmarshal([]byte(`{"position":{"Name":"bin.000001","Pos":4}}`), &legacy))
		require.NoError(t, json.Unmarshal([]byte(`{"gtid_set":""}`), &empty))
		require.Nil(t, legacy.GTIDSet)
		require.NotNil(t, empty.GTIDSet)
		_, err := empty.Compare(legacy)
		require.ErrorIs(t, err, constants.ErrNonRetryable)
	})
}

func TestValidateGTIDHistory(t *testing.T) {
	saved, err := parseGTIDSet(testSID + ":1-3")
	require.NoError(t, err)
	target, err := parseGTIDSet(testSID + ":1-5")
	require.NoError(t, err)
	tests := []struct {
		name, mode, executed, purged, wantError string
	}{
		{"available history", "ON", testSID + ":1-6", testSID + ":1-3", ""},
		{"needed transaction purged", "ON", testSID + ":1-6", testSID + ":1-4", "purged"},
		{"replica behind target", "ON", testSID + ":1-4", "", "has not executed"},
		{"unrelated server", "ON", "", "", "has not executed"},
		{"GTIDs disabled", "OFF", testSID + ":1-6", "", "gtid_mode=ON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGTIDHistory(saved, target, tt.mode, tt.executed, tt.purged)
			if tt.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
				require.ErrorContains(t, err, tt.wantError)
			}
		})
	}
}

func TestGTIDTransaction_Consume(t *testing.T) {
	set, err := parseGTIDSet(testSID + ":1-2")
	require.NoError(t, err)
	gtid := &replication.GTIDEvent{SID: bytes.Repeat([]byte{0xaa}, 16), GNO: 2}
	transactionSet, err := gtid.GTIDNext()
	require.NoError(t, err)
	reorderedSet, err := parseGTIDSet(testSID + ":1-3")
	require.NoError(t, err)
	query := func(sql string) replication.Event { return &replication.QueryEvent{Query: []byte(sql), GSet: set} }
	tests := []struct {
		name   string
		events []replication.Event
	}{
		{"row transaction", []replication.Event{gtid, query("BEGIN"), &replication.RowsEvent{}, &replication.XIDEvent{GSet: set}}},
		{"standalone DDL", []replication.Event{gtid, query("CREATE TABLE t (id INT)")}},
		{"explicit commit", []replication.Event{gtid, query("BEGIN"), query("COMMIT")}},
		{"empty transaction", []replication.Event{gtid, query("BEGIN"), &replication.XIDEvent{GSet: set}}},
		{"completion excludes other transactions in GSet", []replication.Event{gtid, query("BEGIN"), &replication.XIDEvent{GSet: reorderedSet}}},
		{"savepoint is not a commit", []replication.Event{gtid, query("BEGIN"), query("SAVEPOINT s"), query("ROLLBACK TO s"), query("ROLLBACK")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var transaction gtidTransaction
			for i, event := range tt.events {
				completed, err := transaction.consume(&replication.BinlogEvent{Header: &replication.EventHeader{}, Event: event})
				require.NoError(t, err)
				if i < len(tt.events)-1 {
					require.Nil(t, completed, "checkpoint advanced before the transaction ended")
				} else {
					require.True(t, completed.Equal(transactionSet))
					require.False(t, transaction.active)
				}
			}
		})
	}
	t.Run("incomplete transaction cannot start another", func(t *testing.T) {
		transaction := gtidTransaction{active: true}
		_, err := transaction.consume(&replication.BinlogEvent{Header: &replication.EventHeader{}, Event: gtid})
		require.ErrorContains(t, err, "preceding transaction")
	})
	t.Run("unsupported formats fail explicitly", func(t *testing.T) {
		for _, eventType := range []replication.EventType{replication.GTID_TAGGED_LOG_EVENT, replication.ANONYMOUS_GTID_EVENT, replication.XA_PREPARE_LOG_EVENT, replication.TRANSACTION_PAYLOAD_EVENT} {
			t.Run(eventType.String(), func(t *testing.T) {
				var transaction gtidTransaction
				_, err := transaction.consume(&replication.BinlogEvent{Header: &replication.EventHeader{EventType: eventType}})
				require.ErrorIs(t, err, constants.ErrNonRetryable)
			})
		}
	})
}
