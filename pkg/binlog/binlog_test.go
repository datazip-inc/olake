package binlog

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/go-mysql-org/go-mysql/mysql"
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
}
