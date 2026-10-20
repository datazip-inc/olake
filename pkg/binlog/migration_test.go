package binlog

import (
	"bytes"
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestConnection_FinishMigration(t *testing.T) {
	var progress gtidRebuild
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
			conn := &Connection{targetGTIDSet: target}
			err = conn.finishMigration(progress, tt.recovering)
			if tt.invalid {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
				require.Nil(t, conn.gtidSet)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testSID+":1-2", *conn.Checkpoint().GTIDSet)
		})
	}
	t.Run("unfinished transaction", func(t *testing.T) {
		progress.transaction.active = true
		conn := &Connection{targetGTIDSet: progress.gtidSet}
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
			require.Equal(t, state.Position, conn.Checkpoint().Position)
		})
	}
}
