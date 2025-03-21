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

// MySQLGlobalState tracks the binlog position and backfilled streams.
type MySQLGlobalState struct {
	ServerID uint32             `json:"server_id"`
	State    binlog.Binlog      `json:"state"`
	Streams  *types.Set[string] `json:"streams"`
}

// RunChangeStream implements the CDC functionality for multiple streams using a single binlog connection.
func (m *MySQL) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	ctx := context.TODO()

	// Load or initialize global state
	gs := &MySQLGlobalState{
		State:   binlog.Binlog{Position: mysql.Position{}},
		Streams: types.NewSet[string](),
	}
	if m.State.Global != nil {
		if err := utils.Unmarshal(m.State.Global, gs); err != nil {
			return fmt.Errorf("failed to unmarshal global state: %s", err)
		}
	}

	// Set server ID if not set
	if gs.ServerID == 0 {
		gs.ServerID = uint32(1000 + time.Now().UnixNano()%9000)
		m.State.SetGlobalState(gs)
	}

	// Get current binlog position if state is empty
	if gs.State.Position.Name == "" {
		pos, err := m.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %s", err)
		}
		gs.State.Position = pos
		m.State.SetGlobalState(gs)
	}

	// Backfill streams that haven't been processed yet
	var needsBackfill []protocol.Stream
	for _, s := range streams {
		if !gs.Streams.Exists(s.ID()) {
			needsBackfill = append(needsBackfill, s)
		}
	}
	if len(needsBackfill) > 0 {
		if err := utils.Concurrent(ctx, needsBackfill, len(needsBackfill), func(_ context.Context, s protocol.Stream, _ int) error {
			if err := m.backfill(pool, s); err != nil {
				return fmt.Errorf("failed backfill of stream[%s]: %s", s.ID(), err)
			}
			gs.Streams.Insert(s.ID())
			m.State.SetGlobalState(gs)
			return nil
		}); err != nil {
			return fmt.Errorf("failed concurrent backfill: %s", err)
		}
	}

	// Set up inserters for each stream
	inserters := make(map[protocol.Stream]*protocol.ThreadEvent)
	for _, stream := range streams {
		insert, err := pool.NewThread(ctx, stream)
		if err != nil {
			return fmt.Errorf("failed to create writer thread for stream[%s]: %s", stream.ID(), err)
		}
		inserters[stream] = insert
	}
	defer func() {
		for _, insert := range inserters {
			insert.Close()
		}
	}()

	// Configure binlog connection
	config := &binlog.Config{
		ServerID:        gs.ServerID,
		Flavor:          "mysql",
		Host:            m.config.Host,
		Port:            uint16(m.config.Port),
		User:            m.config.Username,
		Password:        m.config.Password,
		Charset:         "utf8mb4",
		VerifyChecksum:  true,
		HeartbeatPeriod: 30 * time.Second,
	}

	// Start binlog connection
	conn, err := binlog.NewConnection(ctx, config, gs.State.Position)
	if err != nil {
		return fmt.Errorf("failed to create binlog connection: %s", err)
	}
	defer conn.Close()

	// Create change filter for all streams
	filter := binlog.NewChangeFilter(streams...)
	// Callback to get master’s binlog position
	getMasterPos := func() (mysql.Position, error) {
		return m.getCurrentBinlogPosition()
	}
	// Stream and process events
	logger.Infof("Starting MySQL CDC from binlog position %s:%d", gs.State.Position.Name, gs.State.Position.Pos)
	return conn.StreamMessages(ctx, filter, func(change binlog.CDCChange) error {
		stream := change.Stream
		insert := inserters[stream]
		pkColumn := getPrimaryKeyColumn(m.client, change.Table)
		deleteTS := utils.Ternary(change.Kind == "delete", change.Timestamp.UnixMilli(), int64(0)).(int64)
		record := types.CreateRawRecord(
			utils.GetKeysHash(change.Data, pkColumn),
			change.Data,
			deleteTS,
		)
		if err := insert.Insert(record); err != nil {
			return fmt.Errorf("failed to insert record for stream[%s]: %s", stream.ID(), err)
		}
		// Update global state with the new position
		gs.State.Position = change.Position
		m.State.SetGlobalState(gs)
		return nil
	}, getMasterPos)
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
