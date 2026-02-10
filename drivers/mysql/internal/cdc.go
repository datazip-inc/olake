package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// Ensure MySQL implements GlobalPosition2PC interface
var _ abstract.GlobalPosition2PC = (*MySQL)(nil)

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
		ServerID:                mySQLGlobalState.ServerID,
		Flavor:                  "mysql",
		Host:                    m.config.Host,
		Port:                    uint16(m.config.Port),
		User:                    m.config.Username,
		Password:                m.config.Password,
		Charset:                 "utf8mb4",
		TimestampStringLocation: m.effectiveTZ,
		VerifyChecksum:          true,
		HeartbeatPeriod:         30 * time.Second,
		InitialWaitTime:         time.Duration(m.cdcConfig.InitialWaitTime) * time.Second,
		SSHClient:               m.sshClient,
		TLSConfig:               tlsConfig,
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

	// Use target position for bounded sync if set (recovery mode)
	targetPos := m.GetTargetCDCPosition()
	return m.BinlogConn.StreamMessages(ctx, m.client, targetPos, OnMessage)
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

func (m *MySQL) GetCDCPosition() string {
	if m.BinlogConn == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", m.BinlogConn.CurrentPos.Name, m.BinlogConn.CurrentPos.Pos)
}

// GetCDCStartPosition returns the starting CDC position from state for predictable thread IDs
func (m *MySQL) GetCDCStartPosition() string {
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return ""
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", mysqlState.State.Position.Name, mysqlState.State.Position.Pos)
}

func (m *MySQL) SetCurrentCDCPosition(position string) {
	globalState := m.state.GetGlobal()
	if globalState == nil {
		logger.Warnf("SetCurrentCDCPosition called but global state is nil")
		return
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		logger.Warnf("Failed to unmarshal global state for SetCurrentCDCPosition: %s", err)
		return
	}
	// Parse position string "filename:pos" and update state
	// Format: mysql-bin.000070:772591
	positionParts := strings.Split(position, ":")
	if len(positionParts) != 2 {
		logger.Warnf("Invalid position format %s for SetCurrentCDCPosition: missing colon", position)
		return
	}
	name := positionParts[0]
	posStr := positionParts[1]
	posVal, err := strconv.ParseUint(posStr, 10, 32)
	if err != nil {
		logger.Warnf("Failed to parse position %s for SetCurrentCDCPosition: %s", position, err)
		return
	}
	pos := uint32(posVal)
	mysqlState.State.Position.Name = name
	mysqlState.State.Position.Pos = pos
	m.state.SetGlobal(mysqlState)
	logger.Infof("Set current CDC position in state: %s", position)
}

func (m *MySQL) SetTargetCDCPosition(position string) {
	m.targetPosition = position
	if position != "" {
		logger.Infof("Set target CDC position for bounded sync: %s", position)
	}
}

func (m *MySQL) GetTargetCDCPosition() string {
	return m.targetPosition
}
