package driver

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
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

func TestMySQL_GTIDRecovery(t *testing.T) {
	previousPath := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() { viper.Set(constants.StatePath, previousPath) })
	const sid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	start, end := sid+":1-2", sid+":1-3"
	orders := &types.ConfiguredStream{Stream: types.NewStream("orders", "demo", nil)}
	other := &types.ConfiguredStream{Stream: types.NewStream("other", "demo", nil)}
	tests := []struct {
		name      string
		otherGTID string
		wantError string
	}{
		{"all streams committed before state save", end, ""},
		{"conflicting committed boundaries", sid + ":1-4", "disagree"},
		{"incomparable histories", sid + ":1:3", "different transaction histories"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initial := MySQLGlobalState{ServerID: 1001, State: binlog.Binlog{
				Position: mysql.Position{Name: "bin.000009", Pos: 500}, ServerUUID: "a", GTIDSet: &start,
			}}
			checkpoint := binlog.Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 100}, ServerUUID: "b", GTIDSet: &end}
			metadata := map[string]any{}
			for streamID, gtid := range map[string]string{orders.ID(): end, other.ID(): tt.otherGTID} {
				value := checkpoint
				value.GTIDSet = &gtid
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				metadata[streamID] = string(data)
			}
			m := &MySQL{config: &Config{Port: 3306},
				state:   &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: initial}},
				streams: []types.StreamInterface{orders, other}}
			// No SQL client: completed destination commits must recover without reading old binlogs.
			got, err := m.StreamChanges(context.Background(), 0, metadata, nil)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("got %v, want %s", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, checkpoint) {
				t.Fatalf("got %+v, want %+v", got, checkpoint)
			}
			if err := m.PostCDC(context.Background(), 0); err != nil {
				t.Fatal(err)
			}
			want := MySQLGlobalState{ServerID: initial.ServerID, State: checkpoint}
			if !reflect.DeepEqual(m.state.GetGlobal().State, want) {
				t.Fatalf("recovered source state differs: %+v", m.state.GetGlobal())
			}
		})
	}
}
