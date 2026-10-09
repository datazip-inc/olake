package driver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) prepareBinlogConn(ctx context.Context, mySQLGlobalState MySQLGlobalState, streamsToSync []types.StreamInterface) (*binlog.Connection, error) {
	// Build TLS config if SSL is configured
	var tlsConfig *tls.Config
	if m.config.SSLConfiguration != nil && m.config.SSLConfiguration.Mode != utils.SSLModeDisable {
		var err error
		tlsConfig, err = m.config.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config for binlog: %w", err)
		}
	}

	port := m.config.Port
	if port <= 0 || port > math.MaxUint16 {
		return nil, errs.Precondition(errs.ConfigInvalid, codePortInvalid,
			fmt.Errorf("invalid mysql port: %d", port))
	}

	config := &binlog.Config{
		ServerID:                mySQLGlobalState.ServerID,
		Flavor:                  "mysql",
		Host:                    m.config.Host,
		Port:                    uint16(port),
		User:                    m.config.Username,
		Password:                m.config.Password,
		Charset:                 "utf8mb4",
		TimestampStringLocation: m.effectiveTZ,
		VerifyChecksum:          true,
		HeartbeatPeriod:         30 * time.Second,
		InitialWaitTime:         time.Duration(m.cdcConfig.InitialWaitTime) * time.Second,
		SSHClient:               m.sshClient,
		TLSConfig:               tlsConfig,
		SchemaClient:            m.client,
	}

	return binlog.NewConnection(ctx, config, mySQLGlobalState.State, streamsToSync, m.dataTypeConverter)
}

func (m *MySQL) ChangeStreamConfig() (bool, bool, bool) {
	return true, false, false
}

// minServerID is the lower bound for generated replication server IDs.
const minServerID = 1000

// newServerID derives a pseudo-random replication server ID in [minServerID, math.MaxUint32).
func newServerID() uint32 {
	offset := time.Now().UnixNano() % (math.MaxUint32 - minServerID)
	if offset < 0 || offset > math.MaxUint32-minServerID {
		return minServerID
	}
	return minServerID + uint32(offset)
}

func (m *MySQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	// Load or initialize global state
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		binlogState, err := binlog.GetCurrentBinlogState(ctx, m.client)
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}
		m.state.SetGlobal(MySQLGlobalState{ServerID: newServerID(), State: binlogState})
		m.state.ResetStreams()
	}
	m.streams = streams
	return nil
}

func (m *MySQL) StreamChanges(ctx context.Context, _ int, metadataStates map[string]any, OnMessage abstract.CDCMsgFn) (any, error) {
	savedState := m.state.GetGlobal()
	if savedState == nil || savedState.State == nil {
		return nil, errs.Precondition(errs.StateInvalid, codeGlobalStateInvalid,
			fmt.Errorf("invalid global state; state is missing"))
	}

	var mySQLGlobalState MySQLGlobalState
	if err := utils.Unmarshal(savedState.State, &mySQLGlobalState); err != nil {
		return nil, fmt.Errorf("failed to unmarshal global state: %w", err)
	}

	// validate server id
	if mySQLGlobalState.ServerID == 0 {
		return nil, errs.Precondition(errs.StateInvalid, codeServerIDMissing,
			fmt.Errorf("invalid global state; server_id is missing"))
	}

	var finishedStreams []string
	var recoveryState binlog.Binlog

	for streamID, rawMtState := range metadataStates {
		if rawMtState == nil {
			continue
		}
		if mtState, ok := rawMtState.(string); ok {
			var mysqlMetadataState binlog.Binlog
			err := json.Unmarshal([]byte(mtState), &mysqlMetadataState)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata state: %w", err)
			}
			comparison, err := mysqlMetadataState.Compare(mySQLGlobalState.State)
			if err != nil {
				return nil, fmt.Errorf("invalid metadata for stream[%s]: %w", streamID, err)
			}

			// Only destination progress strictly ahead of source state needs recovery.
			if comparison > 0 {
				if recoveryState.Position.Name != "" || recoveryState.GTIDSet != nil {
					comparison, err := mysqlMetadataState.Compare(recoveryState)
					if err != nil {
						return nil, err
					}
					if comparison != 0 {
						return nil, errs.Precondition(errs.StateInvalid, codeMetadataStateInvalid,
							fmt.Errorf("recovery checkpoints disagree across streams"))
					}
				}
				// Keep the migration mapping when file and GTID metadata describe the same boundary.
				if recoveryState.GTIDSet == nil || mysqlMetadataState.GTIDSet != nil {
					recoveryState = mysqlMetadataState
				}
				finishedStreams = append(finishedStreams, streamID)
			}
			// state >= metadata: blank sync scenario — stream forward normally
		} else {
			return nil, errs.Precondition(errs.StateInvalid, codeMetadataStateInvalid,
				fmt.Errorf("failed to typecast raw metadata state of type[%T] to string", rawMtState))
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

	// All selected streams already committed this GTID boundary; no binlog replay is needed.
	recovered := recoveryState.GTIDSet != nil && len(remainingStreams) == 0
	if recovered {
		mySQLGlobalState.State = recoveryState
	}
	conn, err := m.prepareBinlogConn(ctx, mySQLGlobalState, remainingStreams)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare binlog conn: %w", err)
	}

	// persist binlog connection for post cdc
	m.BinlogConn = conn

	if !recovered {
		if err := conn.StreamMessages(ctx, m.client, recoveryState, OnMessage); err != nil {
			return nil, err
		}
	}
	if recoveryState.GTIDSet == nil && recoveryState.Position.Name != "" {
		conn.CurrentPos = recoveryState.Position
	}

	return conn.Checkpoint(), nil
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
			State:    m.BinlogConn.Checkpoint(),
		})
		return nil
	}
}
