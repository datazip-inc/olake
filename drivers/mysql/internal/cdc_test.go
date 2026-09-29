package driver

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/viper"
)

func TestMySQL_StreamChanges(t *testing.T) {
	// An unrelated server can reuse the filename with an offset ahead of or behind ours.
	tests := []struct {
		name string
		pos  uint32
	}{{"metadata behind", 100}, {"same position", 200}, {"metadata ahead", 300}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := MySQLGlobalState{
				ServerID: 1001,
				State:    binlog.Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 200}, ServerUUID: "server-a"},
			}
			m := &MySQL{state: &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: state}}}
			metadata, err := json.Marshal(binlog.Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: tt.pos}, ServerUUID: "server-b"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = m.StreamChanges(context.Background(), 0, map[string]any{"orders": string(metadata)}, nil)
			if !errors.Is(err, constants.ErrNonRetryable) || !strings.Contains(err.Error(), "invalid metadata for stream[orders]") {
				t.Fatalf("expected rejection before opening a connection, got %v", err)
			}
			if got := m.state.GetGlobal().State; got != state {
				t.Fatalf("source state changed: got %+v, want %+v", got, state)
			}
		})
	}
}

func TestMySQL_PostCDC(t *testing.T) {
	previousPath := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() { viper.Set(constants.StatePath, previousPath) })
	tests := []struct {
		name       string
		serverUUID string
		canceled   bool
	}{
		{"known server", "server-a", false},
		{"legacy identity stays unknown", "", false},
		{"failed sync preserves checkpoint", "server-a", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := MySQLGlobalState{
				ServerID: 1001,
				State:    binlog.Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 200}, ServerUUID: tt.serverUUID},
			}
			conn, err := binlog.NewConnection(context.Background(), &binlog.Config{ServerID: state.ServerID}, state.State, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			conn.CurrentPos.Pos = 300
			m := &MySQL{state: &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: state}}, BinlogConn: conn}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.canceled {
				cancel()
			}
			err = m.PostCDC(ctx, 0)
			if tt.canceled {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				state.State.Position.Pos = 300
			}
			if got := m.state.GetGlobal().State; got != state {
				t.Fatalf("got checkpoint %+v, want %+v", got, state)
			}
		})
	}
}
