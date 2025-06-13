package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func (m *MySQL) prepareBinlogConn(ctx context.Context, globalState MySQLGlobalState, streams []types.StreamInterface) (*binlog.Connection, error) {
	if !m.CDCSupport {
		return nil, fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	// validate global state
	if globalState.ServerID == 0 {
		return nil, fmt.Errorf("invalid global state; server_id is missing")
	}
	// TODO: Support all flavour of mysql
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
	return binlog.NewConnection(ctx, config, globalState.State.Position, streams, m.dataTypeConverter)
}

func (m *MySQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	// Load or initialize global state
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		binlogPos, err := m.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %s", err)
		}
		m.state.SetGlobal(MySQLGlobalState{ServerID: uint32(1000 + time.Now().UnixNano()%9000), State: binlog.Binlog{Position: binlogPos}})
		m.state.ResetStreams()
		// reinit state
		globalState = m.state.GetGlobal()
	}

	var MySQLGlobalState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &MySQLGlobalState); err != nil {
		return fmt.Errorf("failed to unmarshal global state: %s", err)
	}

	conn, err := m.prepareBinlogConn(ctx, MySQLGlobalState, streams)
	if err != nil {
		return fmt.Errorf("failed to prepare binlog conn: %s", err)
	}
	m.BinlogConn = conn
	return nil
}

func (m *MySQL) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool) error {
	if noErr {
		m.state.SetGlobal(MySQLGlobalState{
			ServerID: m.BinlogConn.ServerID,
			State: binlog.Binlog{
				Position: m.BinlogConn.CurrentPos,
			},
		})
		// TODO: Research about acknowledgment of binlogs in mysql
	}
	m.BinlogConn.Cleanup()
	return nil
}

func (m *MySQL) StreamChanges(ctx context.Context, _ types.StreamInterface, OnMessage abstract.CDCMsgFn) error {
	return m.BinlogConn.StreamMessages(ctx, OnMessage)
}

// getCurrentBinlogPosition retrieves the current binlog position from MySQL.
func (m *MySQL) getCurrentBinlogPosition() (mysql.Position, error) {
	// SHOW MASTER STATUS is not supported in MySQL 8.4 and after

	// Get MySQL version
	majorVersion, minorVersion, err := jdbc.MySQLVersion(m.client)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get MySQL version: %s", err)
	}

	// Use the appropriate query based on the MySQL version
	query := utils.Ternary(majorVersion > 8 || (majorVersion == 8 && minorVersion >= 4), jdbc.MySQLMasterStatusQueryNew(), jdbc.MySQLMasterStatusQuery()).(string)

	rows, err := m.client.Query(query)
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
