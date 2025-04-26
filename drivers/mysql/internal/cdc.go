package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func (m *MySQL) prepareBinlogConfig(serverID uint32) (*binlog.Config, error) {
	if !m.CDCSupport {
		return nil, fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	// validate global state
	if serverID == 0 {
		return nil, fmt.Errorf("invalid global state; server_id is missing")
	}

	return &binlog.Config{
		ServerID:        serverID,
		Flavor:          "mysql",
		Host:            m.config.Host,
		Port:            uint16(m.config.Port),
		User:            m.config.Username,
		Password:        m.config.Password,
		Charset:         "utf8mb4",
		VerifyChecksum:  true,
		HeartbeatPeriod: 30 * time.Second,
		InitialWaitTime: time.Duration(m.cdcConfig.InitialWaitTime) * time.Second,
	}, nil
}

// MySQLGlobalState tracks the binlog position and backfilled streams.
type MySQLGlobalState struct {
	ServerID uint32        `json:"server_id"`
	State    binlog.Binlog `json:"state"`
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

	config, err := m.prepareBinlogConfig(MySQLGlobalState.ServerID)
	if err != nil {
		return fmt.Errorf("failed to prepare binlog config: %s", err)
	}

	// Backfill streams that haven't been processed yet
	var needsBackfill []protocol.Stream
	for _, s := range streams {
		if globalState.Streams == nil || !globalState.Streams.Exists(s.ID()) {
			logger.Infof("backfill required for stream: %s", s.ID())
			needsBackfill = append(needsBackfill, s)
		}
	}

	if err := utils.Concurrent(ctx, needsBackfill, len(needsBackfill), func(_ context.Context, s protocol.Stream, _ int) error {
		if err := m.backfill(ctx, pool, s); err != nil {
			return fmt.Errorf("failed backfill of stream[%s]: %s", s.ID(), err)
		}
		m.State.SetGlobal(MySQLGlobalState, s.ID())
		return nil
	}); err != nil {
		return fmt.Errorf("failed concurrent backfill: %s", err)
	}

	// Set up inserters for each stream
	inserters := make(map[protocol.Stream]*protocol.ThreadEvent)
	errChans := make(map[protocol.Stream]chan error)
	for _, stream := range streams {
		errChan := make(chan error, 1)
		inserter, err := pool.NewThread(ctx, stream, protocol.WithErrorChannel(errChan))
		if err != nil {
			return fmt.Errorf("failed to create writer thread for stream[%s]: %s", stream.ID(), err)
		}
		inserters[stream], errChans[stream] = inserter, errChan
	}
	defer func() {
		if err == nil {
			for stream, insert := range inserters {
				insert.Close()
				if threadErr := <-errChans[stream]; threadErr != nil {
					err = fmt.Errorf("failed to write record for stream[%s]: %s", stream.ID(), threadErr)
				}
			}
			if err == nil {
				m.State.SetGlobal(MySQLGlobalState)
				// TODO: Research about acknowledgment of binlogs in mysql
			}
		}
	}()

	// Start binlog connection
	conn, err := binlog.NewConnection(ctx, config, MySQLGlobalState.State.Position)
	if err != nil {
		return fmt.Errorf("failed to create binlog connection: %s", err)
	}
	defer conn.Close()

	// Stream and process events
	logger.Infof("Starting MySQL CDC from binlog position %s:%d", MySQLGlobalState.State.Position.Name, MySQLGlobalState.State.Position.Pos)
	return conn.StreamMessages(ctx, binlog.NewChangeFilter(streams...), func(change binlog.CDCChange) error {
		stream := change.Stream
		pkColumn := stream.GetStream().SourceDefinedPrimaryKey.Array()[0]
		opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
		record := types.CreateRawRecord(
			utils.GetKeysHash(change.Data, pkColumn),
			change.Data,
			opType,
			change.Timestamp.UnixMilli(),
		)
		if err := inserters[stream].Insert(record); err != nil {
			return fmt.Errorf("failed to insert record for stream[%s]: %s", stream.ID(), err)
		}
		// Update global state with the new position
		MySQLGlobalState.State.Position = change.Position
		return nil
	})
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
