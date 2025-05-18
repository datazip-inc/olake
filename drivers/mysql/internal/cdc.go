package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func (m *MySQL) prepareBinlogConn(ctx context.Context, globalState MySQLGlobalState, streams []protocol.Stream) (*binlog.Connection, error) {
	if !m.CDCSupport {
		return nil, fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	// validate global state
	if globalState.ServerID == 0 {
		return nil, fmt.Errorf("invalid global state; server_id is missing")
	}

	config := &binlog.Config{
		ServerID:        globalState.ServerID,
		Flavor:          "mysql",
		Host:            m.config.Host,
		Port:            uint16(m.config.Port),
		User:            m.config.Username,
		Password:        m.config.Password,
		Charset:         "utf8mb4",
		VerifyChecksum:  true,
		HeartbeatPeriod: 30 * time.Second,
		InitialWaitTime: time.Duration(m.cdcConfig.InitialWaitTime) * time.Second,
	}
	return binlog.NewConnection(ctx, config, globalState.State.Position, streams)
}

// RunChangeStream implements the CDC functionality for multiple streams using a single binlog connection.
func (m *MySQL) RunChangeStream(ctx context.Context, pool *protocol.WriterPool, streams ...protocol.Stream) (err error) {
	// Load or initialize global state
	globalState := m.State.GetGlobal()
	if globalState == nil || globalState.State == nil {
		binlogPos, err := m.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %s", err)
		}
		m.State.SetGlobal(MySQLGlobalState{ServerID: uint32(1000 + time.Now().UnixNano()%9000), State: binlog.Binlog{Position: binlogPos}})
		m.State.ResetStreams()
		// reinit state
		globalState = m.State.GetGlobal()
	}

	var MySQLGlobalState MySQLGlobalState
	if err = utils.Unmarshal(globalState.State, &MySQLGlobalState); err != nil {
		return fmt.Errorf("failed to unmarshal global state: %s", err)
	}

	conn, err := m.prepareBinlogConn(ctx, MySQLGlobalState, streams)
	if err != nil {
		return fmt.Errorf("failed to prepare binlog conn: %s", err)
	}

	postCDC := func(ctx context.Context, noErr bool) error {
		if noErr {
			MySQLGlobalState.State.Position = conn.CurrentPos
			m.State.SetGlobal(MySQLGlobalState)
			// TODO: Research about acknowledgment of binlogs in mysql
		}
		conn.Cleanup()
		return nil
	}
	return m.Driver.RunChangeStream(ctx, m, conn.StreamMessages, postCDC, pool, streams...)
}

// getCurrentBinlogPosition retrieves the current binlog position from MySQL.
func (m *MySQL) getCurrentBinlogPosition() (mysql.Position, error) {
	rows, err := m.client.Query(jdbc.MySQLMasterStatusQuery())
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get master status: %s", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return mysql.Position{}, fmt.Errorf("no binlog position available")
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, executeGtidSet string
	if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to scan binlog position: %s", err)
	}

	return mysql.Position{Name: file, Pos: position}, nil
}
