package driver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) prepareBinlogConn(ctx context.Context, mySQLGlobalState MySQLGlobalState, streamsToSync []types.StreamInterface) (*binlog.Connection, error) {
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

	return binlog.NewConnection(ctx, config, mySQLGlobalState.State.Position, streamsToSync, m.dataTypeConverter)
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

func (m *MySQL) StreamChanges(ctx context.Context, streamIndex int, metadataStates map[string]any, OnMessage abstract.CDCMsgFn) (any, error) {
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

	var finishedStreams []string
	currentBinlogPos, err := binlog.GetCurrentBinlogPosition(ctx, m.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get current binlog position: %s", err)
	}

	for streamID, rawMtState := range metadataStates {
		if rawMtState == nil {
			continue
		}
		if mtState, ok := rawMtState.(string); ok {
			var mysqlMetadataState binlog.Binlog
			err := json.Unmarshal([]byte(mtState), &mysqlMetadataState)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata state: %s", err)
			}

			// Recovery is only needed when metadata is strictly AHEAD of state.
			// metadata.Compare(state) > 0 means either:
			//   - same file but metadata.Pos > state.Pos, OR
			//   - metadata is on a later binlog file (e.g. mysql-bin.000043 vs .000042)
			if mysqlMetadataState.Position.Compare(mySQLGlobalState.State.Position) > 0 {
				// metadata ahead of state: genuine crash-recovery path
				currentBinlogPos = mysqlMetadataState.Position
				finishedStreams = append(finishedStreams, streamID)
			}
			// state >= metadata: blank sync scenario — stream forward normally
		} else {
			return nil, fmt.Errorf("failed to typecast raw metadata state of type[%T] to string", rawMtState)
		}
	}

	var remainingStreams []types.StreamInterface
	if len(finishedStreams) > 0 {
		finishedStreamSet := types.NewSet(finishedStreams...)
		_ = utils.ForEach(m.streams, func(stream types.StreamInterface) error {
			if !finishedStreamSet.Exists(stream.ID()) {
				logger.Infof("Running recovery sync for stream[%s]", stream.ID())
				remainingStreams = append(remainingStreams, stream)
			}
			return nil
		})
	} else {
		remainingStreams = m.streams
	}

	conn, err := m.prepareBinlogConn(ctx, mySQLGlobalState, remainingStreams)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare binlog conn: %s", err)
	}

	// persist binlog connection for post cdc
	m.BinlogConn = conn

	err = m.BinlogConn.StreamMessages(ctx, m.client, currentBinlogPos, OnMessage)
	if err != nil {
		return nil, err
	}
	// Incase of recovery, the current binlog position is updated to the metadata state
	// so that the next sync starts from the correct position
	m.BinlogConn.CurrentPos = currentBinlogPos
	return binlog.Binlog{Position: currentBinlogPos}, nil
}

func (m *MySQL) PostCDC(ctx context.Context, _ int) error {
	if m.BinlogConn == nil {
		return nil
	}
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
