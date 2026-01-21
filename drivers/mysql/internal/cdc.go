package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
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
		// Read existing state to preserve NextCDCPos and Processing fields
		var mysqlState MySQLGlobalState
		globalState := m.state.GetGlobal()
		if globalState != nil && globalState.State != nil {
			if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
				logger.Warnf("Failed to unmarshal global state in PostCDC: %s", err)
			}
		}
		// Update with current position while preserving other fields
		mysqlState.ServerID = m.BinlogConn.ServerID
		mysqlState.State = binlog.Binlog{
			Position: m.BinlogConn.CurrentPos,
		}
		// If all streams were successfully committed (processing is empty),
		// clear next_cdc_pos as we don't need it for recovery
		if len(mysqlState.Processing) == 0 {
			mysqlState.NextCDCPos = ""
		}
		m.state.SetGlobal(mysqlState)
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

func (m *MySQL) SetNextCDCPosition(position string) {
	globalState := m.state.GetGlobal()
	if globalState == nil {
		logger.Warnf("SetNextCDCPosition called but global state is nil")
		return
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		logger.Warnf("Failed to unmarshal global state for SetNextCDCPosition: %s", err)
		return
	}
	mysqlState.NextCDCPos = position
	m.state.SetGlobal(mysqlState)
	logger.Infof("Set next_cdc_pos in state: %s", position)
}

func (m *MySQL) GetNextCDCPosition() string {
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return ""
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		return ""
	}
	return mysqlState.NextCDCPos
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

func (m *MySQL) SetProcessingStreams(streamIDs []string) {
	globalState := m.state.GetGlobal()
	if globalState == nil {
		logger.Warnf("SetProcessingStreams called but global state is nil")
		return
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		logger.Warnf("Failed to unmarshal global state for SetProcessingStreams: %s", err)
		return
	}
	mysqlState.Processing = streamIDs
	m.state.SetGlobal(mysqlState)
	logger.Infof("Set processing streams in state: %v", streamIDs)
}

func (m *MySQL) RemoveProcessingStream(streamID string) {
	globalState := m.state.GetGlobal()
	if globalState == nil {
		logger.Warnf("RemoveProcessingStream called but global state is nil")
		return
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		logger.Warnf("Failed to unmarshal global state for RemoveProcessingStream: %s", err)
		return
	}
	// Remove streamID from processing array
	newProcessing := make([]string, 0, len(mysqlState.Processing))
	for _, s := range mysqlState.Processing {
		if s != streamID {
			newProcessing = append(newProcessing, s)
		}
	}
	mysqlState.Processing = newProcessing
	m.state.SetGlobal(mysqlState)
	logger.Infof("Removed stream %s from processing, remaining: %v", streamID, newProcessing)
}

func (m *MySQL) GetProcessingStreams() []string {
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return nil
	}
	var mysqlState MySQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mysqlState); err != nil {
		return nil
	}
	return mysqlState.Processing
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

// SaveNextCDCPositionForStream - no-op for MySQL (uses global position)
func (m *MySQL) SaveNextCDCPositionForStream(streamID string) {}

// CommitCDCPositionForStream - no-op for MySQL (uses global position)
func (m *MySQL) CommitCDCPositionForStream(streamID string) {}

// CheckPerStreamRecovery - no-op for MySQL (uses global position)
func (m *MySQL) CheckPerStreamRecovery(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error {
	return nil
}

// AcknowledgeCDCPosition - no-op for MySQL
func (m *MySQL) AcknowledgeCDCPosition(ctx context.Context, position string) error {
	return nil
}
