package binlog

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestNewConnection(t *testing.T) {
	t.Run("cancellation interrupts replication handshake", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		state := Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 4}}
		conn, err := NewConnection(ctx, &Config{
			ServerID: 1001, Host: listener.Addr().String(), HeartbeatPeriod: time.Minute,
		}, state, nil, nil)
		require.NoError(t, err)
		t.Cleanup(conn.Cleanup)

		result := make(chan error, 1)
		go func() {
			_, err := conn.syncer.StartSync(state.Position)
			result <- err
		}()
		require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(5*time.Second)))
		server, err := listener.Accept()
		require.NoError(t, err)
		t.Cleanup(func() { _ = server.Close() })

		// The peer accepts TCP but never sends a MySQL greeting.
		cancel()
		select {
		case err := <-result:
			require.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("replication handshake did not stop after cancellation")
		}
	})
}

func TestBinlog_ValidateServerUUID(t *testing.T) {
	tests := []struct {
		name       string
		savedUUID  string
		sourceUUID string
		wantError  bool
	}{
		{"same server", "server-a", "server-a", false},
		{"different server", "server-a", "server-b", true},
		{"missing source identity", "server-a", "", true},
		{"legacy checkpoint", "", "server-a", false},
		{"legacy MariaDB checkpoint", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 157}, ServerUUID: tt.savedUUID}
			err := state.ValidateServerUUID(tt.sourceUUID)
			if !tt.wantError {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, constants.ErrNonRetryable)
			require.Equal(t, errs.CDCPositionLost, errs.From(err).Category)
			require.Equal(t, "mysql.server_uuid_mismatch", errs.From(err).Code)
			require.ErrorContains(t, err, "mysql-bin.000001")
		})
	}
}

func TestConnection_StreamMessages(t *testing.T) {
	t.Run("GTIDs disabled after initialization", func(t *testing.T) {
		gtid := testSID + ":1-2"
		state := Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 200}, GTIDSet: &gtid}
		conn, err := NewConnection(context.Background(), &Config{ServerID: 1001}, state, nil, nil)
		require.NoError(t, err)
		t.Cleanup(conn.Cleanup)

		err = conn.StreamMessages(context.Background(), nil, Binlog{Position: state.Position}, nil)
		require.ErrorIs(t, err, constants.ErrNonRetryable)
		require.Equal(t, errs.CDCPositionLost, errs.From(err).Category)
		require.Equal(t, "mysql.gtid_disabled", errs.From(err).Code)
	})

	tests := []struct {
		name       string
		recovery   bool
		savedUUID  string
		targetUUID string
	}{
		{"legacy checkpoint adopts SQL owner", false, "", "server-a"},
		{"legacy checkpoint adopts recovery owner", true, "", "server-a"},
		{"legacy recovery target preserves saved owner", true, "server-a", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, mock.ExpectationsWereMet())
				_ = db.Close()
			})
			state := Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 200}, ServerUUID: tt.savedUUID}
			target := Binlog{Position: mysql.Position{Name: state.Position.Name, Pos: 300}, ServerUUID: tt.targetUUID}
			if !tt.recovery {
				mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.4.2"))
				mock.ExpectQuery(jdbc.MySQLGTIDModeQuery()).WillReturnRows(sqlmock.NewRows([]string{"uuid", "mode"}).AddRow(target.ServerUUID, "OFF"))
				mock.ExpectQuery(jdbc.MySQLMasterStatusQueryNew()).WillReturnRows(sqlmock.NewRows([]string{"file", "position", "do", "ignore", "gtids"}).
					AddRow(target.Position.Name, target.Position.Pos, "", "", ""))
				target = Binlog{}
			}
			// SQL or destination metadata identifies A; the replication endpoint now reaches B.
			address := testReplicationServer(t, jdbc.MySQLServerUUIDQuery(), "server-b")
			conn, err := NewConnection(context.Background(), &Config{ServerID: 1001, Host: address}, state, nil, nil)
			require.NoError(t, err)
			t.Cleanup(conn.Cleanup)
			err = conn.StreamMessages(context.Background(), sqlx.NewDb(db, "sqlmock"), target, nil)
			require.ErrorIs(t, err, constants.ErrNonRetryable)
			require.Equal(t, "mysql.server_uuid_mismatch", errs.From(err).Code)
			require.Equal(t, "server-a", conn.ServerUUID)
		})
	}

	t.Run("recovery target conflicts with saved owner", func(t *testing.T) {
		state := Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 200}, ServerUUID: "server-a"}
		conn, err := NewConnection(context.Background(), &Config{ServerID: 1001}, state, nil, nil)
		require.NoError(t, err)
		t.Cleanup(conn.Cleanup)
		target := Binlog{Position: mysql.Position{Name: state.Position.Name, Pos: 300}, ServerUUID: "server-b"}
		err = conn.StreamMessages(context.Background(), nil, target, nil)
		require.ErrorIs(t, err, constants.ErrNonRetryable)
		require.Equal(t, "mysql.server_uuid_mismatch", errs.From(err).Code)
		require.Equal(t, state.ServerUUID, conn.ServerUUID)
	})
}

func TestConnection_ValidateReplicationServer(t *testing.T) {
	saved, err := parseGTIDSet(testSID + ":1-2")
	require.NoError(t, err)
	target, err := parseGTIDSet(testSID + ":1-3")
	require.NoError(t, err)
	tests := []struct {
		name      string
		saved     mysql.GTIDSet
		target    mysql.GTIDSet
		query     string
		values    []string
		wantUUID  string
		wantError string
	}{
		{"file uses owner", nil, nil, jdbc.MySQLServerUUIDQuery(), []string{"server-a"}, "", ""},
		{"migration uses owner", nil, target, jdbc.MySQLServerUUIDQuery(), []string{"server-a"}, "", ""},
		{"GTIDs accept promoted server", saved, target, jdbc.MySQLGTIDStateQuery(), []string{"server-b", "ON", target.String(), ""}, "server-b", ""},
		{"GTIDs reject missing history", saved, target, jdbc.MySQLGTIDStateQuery(), []string{"server-b", "ON", saved.String(), ""}, "", "mysql.gtid_history_missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			address := testReplicationServer(t, tt.query, tt.values...)
			peer, err := client.Connect(address, "", "", "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = peer.Close() })
			conn := Connection{ServerUUID: "server-a", gtidSet: tt.saved, targetGTIDSet: tt.target}
			err = conn.validateReplicationServer(peer)
			if tt.wantError != "" {
				require.ErrorIs(t, err, constants.ErrNonRetryable)
				require.Equal(t, tt.wantError, errs.From(err).Code)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantUUID, conn.replicationUUID)
		})
	}
}

type replicationQueryHandler struct {
	server.EmptyHandler
	query  string
	result *mysql.Result
}

func (h replicationQueryHandler) HandleQuery(query string) (*mysql.Result, error) {
	if query != h.query {
		return nil, fmt.Errorf("unexpected replication query: %s", query)
	}
	return h.result, nil
}

func testReplicationServer(t *testing.T, query string, values ...string) string {
	t.Helper()
	names, row := make([]string, len(values)), make([]interface{}, len(values))
	for i, value := range values {
		names[i], row[i] = fmt.Sprintf("column%d", i), value
	}
	result, err := mysql.BuildSimpleResultset(names, [][]interface{}{row}, false)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		peer, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer peer.Close()
		_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
		conn, err := server.NewConn(peer, "", "", replicationQueryHandler{query: query, result: &mysql.Result{Resultset: result}})
		if err == nil {
			err = conn.HandleCommand()
		}
		done <- err
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		require.NoError(t, <-done)
	})
	return listener.Addr().String()
}
