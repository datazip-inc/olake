package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

func (m *MySQL) prepareBinlogConn(ctx context.Context) (*binlog.Connection, error) {
	savedState := m.state.GetGlobal()
	if savedState == nil || savedState.State == nil {
		return nil, fmt.Errorf("invalid global state; state is missing")
	}

	var mySQLGlobalState MySQLGlobalState
	if err := utils.Unmarshal(savedState.State, &mySQLGlobalState); err != nil {
		return nil, fmt.Errorf("failed to unmarshal global state: %s", err)
	}

	// validate server id
	if mySQLGlobalState.ServerID == 0 {
		return nil, fmt.Errorf("invalid global state; server_id is missing")
	}

	// Build TLS config if SSL is configured
	var tlsConfig *tls.Config
	if m.config.SSLConfiguration != nil && m.config.SSLConfiguration.Mode != utils.SSLModeDisable {
		var err error
		tlsConfig, err = m.config.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config for binlog: %s", err)
		}
	}

	config := &binlog.Config{
		ServerID:        mySQLGlobalState.ServerID,
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
		TLSConfig:       tlsConfig,
	}

	return binlog.NewConnection(ctx, config, mySQLGlobalState.State.Position, m.streams, m.dataTypeConverter)
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
	m.streams = streams
	return nil
}

func (m *MySQL) StreamChanges(ctx context.Context, streamIndex int, OnMessage abstract.CDCMsgFn) error {
	conn, err := m.prepareBinlogConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to prepare binlog conn: %s", err)
	}

	// persist binlog connection for post cdc
	m.BinlogConn = conn

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
