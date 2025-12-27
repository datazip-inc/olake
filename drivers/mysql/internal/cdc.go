package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
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
		SSHClient:       m.sshClient,
	}

	return binlog.NewConnection(ctx, config, globalState.State.Position, streams, m.dataTypeConverter)
}

func (m *MySQL) ChangeStreamConfig() (bool, bool, bool) {
	return true, false, false
}

func (m *MySQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	// Load or initialize global state
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		binlogPos, err := binlog.GetCurrentBinlogPosition(ctx, m.client)
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %s", err)
		}
		m.state.SetGlobal(MySQLGlobalState{ServerID: uint32(1000 + time.Now().UnixNano()%4294966295), State: binlog.Binlog{Position: binlogPos}})
		m.state.ResetStreams()
		// reinit state
		globalState = m.state.GetGlobal()
	}

	var mySQLGlobalState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mySQLGlobalState); err != nil {
		return fmt.Errorf("failed to unmarshal global state: %s", err)
	}

	conn, err := m.prepareBinlogConn(ctx, mySQLGlobalState, streams)
	if err != nil {
		return fmt.Errorf("failed to prepare binlog conn: %s", err)
	}
	m.BinlogConn = conn
	return nil
}

func (m *MySQL) StreamChanges(ctx context.Context, _ int, OnMessage abstract.CDCMsgFn) error {
	return m.BinlogConn.StreamMessages(ctx, m.client, OnMessage)
}

func (m *MySQL) PostCDC(ctx context.Context, _ int) error {
	defer m.BinlogConn.Cleanup()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		m.state.SetGlobal(MySQLGlobalState{
			ServerID: m.BinlogConn.ServerID,
			State: binlog.Binlog{
				Position: m.BinlogConn.CurrentPos,
			},
		})
		return nil
	}
}
