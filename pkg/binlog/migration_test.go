package binlog

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestBinlog_CompareMigration(t *testing.T) {
	boundary, later := testSID+":1-3", testSID+":1-4"
	migration := &GTIDMigration{Position: mysql.Position{Name: "bin.000010", Pos: 900}, ServerUUID: "a", GTIDSet: boundary}
	tests := []struct {
		name, owner string
		position    uint32
		gtid        string
		want        int
		invalid     bool
	}{
		{"older table after promotion", "a", 400, later, 1, false},
		{"legacy table without identity", "", 400, later, 1, false},
		{"same transition boundary", "a", 900, boundary, 0, false},
		{"GTIDs advanced after migration", "a", 900, later, 1, false},
		{"unmapped later file position", "a", 950, later, 0, true},
		{"wrong original owner", "b", 400, later, 0, true},
		{"checkpoint predates migration", "a", 400, testSID + ":1-2", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The promoted server's physical position cannot order old file metadata.
			state := Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 100}, ServerUUID: "b", GTIDSet: &tt.gtid, Migration: migration}
			file := Binlog{Position: mysql.Position{Name: "bin.000010", Pos: tt.position}, ServerUUID: tt.owner}
			got, err := state.Compare(file)
			if tt.invalid {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			got, err = file.Compare(state)
			require.NoError(t, err)
			require.Equal(t, -tt.want, got)
			data, err := json.Marshal(state)
			require.NoError(t, err)
			var restored Binlog
			require.NoError(t, json.Unmarshal(data, &restored))
			require.Equal(t, state, restored)
		})
	}
}

func TestConnection_FinishMigration(t *testing.T) {
	var progress migrationProgress
	events := []struct {
		kind  replication.EventType
		event replication.Event
	}{
		{replication.PREVIOUS_GTIDS_EVENT, &replication.PreviousGTIDsEvent{GTIDSets: testSID + ":1"}},
		{replication.GTID_EVENT, &replication.GTIDEvent{SID: bytes.Repeat([]byte{0xaa}, 16), GNO: 2}},
		{replication.QUERY_EVENT, &replication.QueryEvent{Query: []byte("BEGIN")}},
		{replication.XID_EVENT, &replication.XIDEvent{}},
	}
	for _, event := range events {
		require.NoError(t, progress.consume(&replication.BinlogEvent{Header: &replication.EventHeader{EventType: event.kind}, Event: event.event}))
	}
	tests := []struct {
		name, target        string
		recovering, invalid bool
	}{
		{"status GTIDs lag the file position", testSID + ":1", false, false},
		{"exact persisted recovery boundary", testSID + ":1-2", true, false},
		{"persisted recovery boundary cannot change", testSID + ":1", true, true},
		{"unmapped executed transaction", testSID + ":1-3", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := parseGTIDSet(tt.target)
			require.NoError(t, err)
			conn := &Connection{targetGTIDSet: target, migration: &GTIDMigration{GTIDSet: tt.target}}
			err = conn.finishMigration(progress, tt.recovering)
			if tt.invalid {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
				require.Nil(t, conn.gtidSet)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testSID+":1-2", *conn.Checkpoint().GTIDSet)
			require.Equal(t, *conn.Checkpoint().GTIDSet, conn.Checkpoint().Migration.GTIDSet)
		})
	}
	t.Run("unfinished transaction", func(t *testing.T) {
		progress.transaction.active = true
		conn := &Connection{targetGTIDSet: progress.gtidSet, migration: &GTIDMigration{}}
		require.ErrorIs(t, conn.finishMigration(progress, false), constants.ErrNonRetryable)
		require.Nil(t, conn.gtidSet)
	})
}

func TestConnection_PrepareMigration(t *testing.T) {
	gtid := testSID + ":1-3"
	target := Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 900}, ServerUUID: "a", GTIDSet: &gtid}
	for _, owner := range []string{"a", "", "b"} {
		t.Run("source owner "+owner, func(t *testing.T) {
			state := Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 400}, ServerUUID: owner}
			conn, err := NewConnection(context.Background(), &Config{ServerID: 1001}, state, nil, nil)
			require.NoError(t, err)
			t.Cleanup(conn.Cleanup)
			err = conn.prepareMigration(target)
			if owner == "b" {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
				return
			}
			require.NoError(t, err)
			// Capturing the target must not publish its GTIDs before catch-up completes.
			require.Nil(t, conn.Checkpoint().GTIDSet)
			require.Nil(t, conn.Checkpoint().Migration)
			require.Equal(t, state.Position, conn.Checkpoint().Position)
		})
	}
}
