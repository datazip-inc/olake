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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/mysql/internal/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
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
			if got := m.state.GetGlobal().State; !reflect.DeepEqual(got, state) {
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
			if got := m.state.GetGlobal().State; !reflect.DeepEqual(got, state) {
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
		// Both destinations are ahead of source state, but neither contains the other.
		{"incomparable committed boundaries", sid + ":1-2:4", "invalid metadata for stream["},
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
			m, _ := newMockMySQL(t)
			m.config = &Config{Port: 3306}
			m.state = &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: initial}}
			m.streams = []types.StreamInterface{orders, other}
			// No SQL queries: completed destination commits must recover without a live source.
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

func TestMySQL_FileRecovery(t *testing.T) {
	previousPath := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() { viper.Set(constants.StatePath, previousPath) })
	orders := &types.ConfiguredStream{Stream: types.NewStream("orders", "demo", nil)}
	other := &types.ConfiguredStream{Stream: types.NewStream("other", "demo", nil)}
	tests := []struct {
		name                string
		ordersPos, otherPos uint32
		wantError           string
	}{
		{"all streams committed before state save", 900, 900, ""},
		{"partial recovery still reads source", 900, 400, "source unavailable"},
		{"no recovery point still reads source", 400, 400, "source unavailable"},
		{"conflicting committed boundaries", 900, 1000, "disagree"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initial := MySQLGlobalState{ServerID: 1001, State: binlog.Binlog{
				Position: mysql.Position{Name: "bin.000001", Pos: 400}, ServerUUID: "a",
			}}
			checkpoint := binlog.Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 900}, ServerUUID: "a"}
			require.NoError(t, json.Unmarshal([]byte(`{"schemas":{"demo.orders":{"columns":[{"name":"id"}],"anchored_at":{"Name":"bin.000001","Pos":300}}}}`), &checkpoint))
			metadata := map[string]any{}
			for streamID, pos := range map[string]uint32{orders.ID(): tt.ordersPos, other.ID(): tt.otherPos} {
				value := checkpoint
				value.Position.Pos = pos
				data, err := json.Marshal(value)
				require.NoError(t, err)
				metadata[streamID] = string(data)
			}
			m, mock := newMockMySQL(t)
			m.config = &Config{Port: 3306}
			m.state = &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: initial}}
			m.streams = []types.StreamInterface{orders, other}
			if tt.wantError == "source unavailable" {
				mock.ExpectQuery("SELECT @@version").WillReturnError(errors.New(tt.wantError))
			}

			// Completed commits recover their checkpoint and schemas without any SQL queries.
			got, err := m.StreamChanges(context.Background(), 0, metadata, nil)
			require.Equal(t, initial, m.state.GetGlobal().State)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, checkpoint, got)
			require.NoError(t, m.PostCDC(context.Background(), 0))
			want := MySQLGlobalState{ServerID: initial.ServerID, State: checkpoint}
			require.Equal(t, want, m.state.GetGlobal().State)

			// Adopting committed progress does not make file positions portable to another server.
			mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
			mock.ExpectQuery("SELECT @@lower_case_table_names").WillReturnRows(sqlmock.NewRows([]string{"lower_case_table_names"}).AddRow(0))
			mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
			mock.ExpectQuery("SELECT @@server_uuid, @@gtid_mode").WillReturnRows(sqlmock.NewRows([]string{"uuid", "mode"}).AddRow("b", "OFF"))
			mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow("bin.000001", 1000, "", "", ""))
			_, err = m.StreamChanges(context.Background(), 0, metadata, nil)
			require.NotNil(t, m.BinlogConn)
			t.Cleanup(m.BinlogConn.Cleanup)
			require.ErrorIs(t, err, constants.ErrNonRetryable)
			require.ErrorContains(t, err, "server UUID changed")
			require.Equal(t, want, m.state.GetGlobal().State)
		})
	}
}

func TestMySQL_MigrationRecovery(t *testing.T) {
	previousPath := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() { viper.Set(constants.StatePath, previousPath) })
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3"
	checkpoint := binlog.Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 900}, ServerUUID: "a", GTIDSet: &gtid}
	orders := &types.ConfiguredStream{Stream: types.NewStream("orders", "demo", nil)}
	other := &types.ConfiguredStream{Stream: types.NewStream("other", "demo", nil)}
	for _, owner := range []string{"a", ""} {
		t.Run("legacy source owner "+owner, func(t *testing.T) {
			initial := MySQLGlobalState{ServerID: 1001, State: binlog.Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 400}, ServerUUID: owner}}
			m := &MySQL{config: &Config{Port: 3306},
				state:   &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: initial}},
				streams: []types.StreamInterface{orders, other}}
			data, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			// Migration committed before source-state save; recovery needs no SQL connection.
			fileData, err := json.Marshal(binlog.Binlog{Position: checkpoint.Position, ServerUUID: owner})
			if err != nil {
				t.Fatal(err)
			}
			got, err := m.StreamChanges(context.Background(), 0, map[string]any{orders.ID(): string(data), other.ID(): string(fileData)}, nil)
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
				t.Fatalf("got state %+v, want %+v", m.state.GetGlobal(), want)
			}
		})
	}
}

func TestMySQL_LegacyMetadataAfterMigration(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3"
	for _, owner := range []string{"a", ""} {
		t.Run("legacy metadata owner "+owner, func(t *testing.T) {
			m, mock := newMockMySQL(t)
			initial := MySQLGlobalState{ServerID: 1001, State: binlog.Binlog{
				Position: mysql.Position{Name: "bin.000001", Pos: 100}, ServerUUID: "b", GTIDSet: &gtid,
			}}
			m.config = &Config{Port: 3306}
			m.state = &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: initial}}
			metadata, err := json.Marshal(binlog.Binlog{
				Position: mysql.Position{Name: "bin.000010", Pos: 900}, ServerUUID: owner,
			})
			require.NoError(t, err)
			sourceErr := errors.New("source unavailable")
			mock.ExpectQuery("SELECT @@version").WillReturnError(sourceErr)

			// Idle-table metadata from before migration must not order the promoted server's position.
			_, err = m.StreamChanges(context.Background(), 0, map[string]any{"idle": string(metadata)}, nil)
			require.NotNil(t, m.BinlogConn)
			t.Cleanup(m.BinlogConn.Cleanup)
			require.ErrorIs(t, err, sourceErr)
			require.Equal(t, initial, m.state.GetGlobal().State)
		})
	}
}

func TestBinlogFlavor(t *testing.T) {
	tests := []struct {
		name         string
		serverFlavor string
		expected     string
	}{
		{name: "MariaDB server uses the mariadb replication flavor", serverFlavor: "MariaDB", expected: mysql.MariaDBFlavor},
		{name: "MySQL server keeps the mysql replication flavor", serverFlavor: "MySQL", expected: mysql.MySQLFlavor},
		{name: "unrecognized flavor falls back to mysql", serverFlavor: "", expected: mysql.MySQLFlavor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := binlogFlavor(tt.serverFlavor); got != tt.expected {
				t.Errorf("binlogFlavor(%q) = %q, want %q", tt.serverFlavor, got, tt.expected)
			}
		})
	}
}

func TestMySQL_PreCDC(t *testing.T) {
	previousPath := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() { viper.Set(constants.StatePath, previousPath) })
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3"
	tests := []struct {
		name, version, mode, query string
		wantGTID                   bool
	}{
		{"MySQL 8.0 GTIDs", "8.0.36", "ON", "SHOW MASTER STATUS", true},
		{"MySQL 8.4 GTIDs", "8.4.2", "ON", "SHOW BINARY LOG STATUS", true},
		{"MySQL 5.7 file position", "5.7.44", "OFF", "SHOW MASTER STATUS", false},
		{"MariaDB file position", "10.11.9-MariaDB", "", "SHOW MASTER STATUS", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, mock := newMockMySQL(t)
			m.state = &types.State{RWMutex: &sync.RWMutex{}}
			mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(tt.version))
			want := binlog.Binlog{Position: mysql.Position{Name: "bin.000001", Pos: 200}}
			columns := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}
			rows := sqlmock.NewRows(columns).AddRow("bin.000001", 200, "", "")
			if tt.mode != "" {
				mock.ExpectQuery("SELECT @@server_uuid, @@gtid_mode").WillReturnRows(sqlmock.NewRows([]string{"uuid", "mode"}).AddRow("server-a", tt.mode))
				rows = sqlmock.NewRows(append(columns, "Executed_Gtid_Set")).AddRow("bin.000001", 200, "", "", gtid)
				want.ServerUUID = "server-a"
			}
			if tt.wantGTID {
				want.GTIDSet = &gtid
			}
			mock.ExpectQuery(tt.query).WillReturnRows(rows)
			require.NoError(t, m.PreCDC(context.Background(), nil))
			state := m.state.GetGlobal().State.(MySQLGlobalState)
			require.Equal(t, want, state.State)
			require.NotZero(t, state.ServerID)
		})
	}
}
