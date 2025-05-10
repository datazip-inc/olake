package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
)

func (p *Postgres) prepareWALJSConfig(streams ...protocol.Stream) (*waljs.Config, error) {
	if !p.CDCSupport {
		return nil, fmt.Errorf("invalid call; %s not running in CDC mode", p.Type())
	}

	return &waljs.Config{
		Connection:          *p.config.Connection,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     time.Duration(p.cdcConfig.InitialWaitTime) * time.Second,
		Tables:              types.NewSet[protocol.Stream](streams...),
		BatchSize:           p.config.BatchSize,
	}, nil
}

func (p *Postgres) RunChangeStream(ctx context.Context, pool *protocol.WriterPool, streams ...protocol.Stream) (err error) {
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return fmt.Errorf("failed to prepare wal config: %s", err)
	}

	socket, err := waljs.NewConnection(ctx, p.client, config, p.dataTypeConverter)
	if err != nil {
		return fmt.Errorf("failed to create wal connection: %s", err)
	}
	defer socket.Cleanup(ctx)

	currentLSN := socket.ConfirmedFlushLSN
	globalState := p.State.GetGlobal()

	if globalState == nil || globalState.State == nil {
		p.State.SetGlobal(waljs.WALState{LSN: currentLSN.String()})
		p.State.ResetStreams()
	} else {
		// global state exist check for cursor and cursor mismatch
		var postgresGlobalState waljs.WALState
		if err = utils.Unmarshal(globalState.State, &postgresGlobalState); err != nil {
			return fmt.Errorf("failed to unmarshal global state: %s", err)
		}
		if postgresGlobalState.LSN == "" {
			p.State.SetGlobal(waljs.WALState{LSN: currentLSN.String()})
			p.State.ResetStreams()
		} else {
			parsed, err := pglogrepl.ParseLSN(postgresGlobalState.LSN)
			if err != nil {
				return fmt.Errorf("failed to parse stored lsn[%s]: %s", postgresGlobalState.LSN, err)
			}
			if parsed != currentLSN {
				logger.Warnf("lsn mismatch, backfill will start again. prev lsn [%s] current lsn [%s]", parsed, currentLSN)
				p.State.SetGlobal(waljs.WALState{LSN: currentLSN.String()})
				p.State.ResetStreams()
			}
		}

	}

	var needsBackfill []protocol.Stream
	for _, s := range streams {
		if globalState == nil || !globalState.Streams.Exists(s.ID()) {
			needsBackfill = append(needsBackfill, s)
		}
	}
	if err = utils.Concurrent(ctx, needsBackfill, len(needsBackfill), func(ctx context.Context, s protocol.Stream, _ int) error {
		if err := p.backfill(ctx, pool, s); err != nil {
			return fmt.Errorf("failed backfill of stream[%s]: %s", s.ID(), err)
		}
		p.State.SetGlobal(waljs.WALState{LSN: currentLSN.String()}, s.ID())
		return nil
	}); err != nil {
		return fmt.Errorf("failed concurrent backfill: %s", err)
	}

	// Inserter lifecycle management
	inserters := make(map[protocol.Stream]*protocol.ThreadEvent)
	errChans := make(map[protocol.Stream]chan error)

	// Inserter initialization
	for _, stream := range streams {
		errChan := make(chan error)
		inserter, err := pool.NewThread(ctx, stream, protocol.WithErrorChannel(errChan), protocol.WithBackfill(false))
		if err != nil {
			return fmt.Errorf("failed to initiate writer thread for stream[%s]: %s", stream.ID(), err)
		}
		inserters[stream], errChans[stream] = inserter, errChan
	}

	defer func() {
		if err == nil {
			for stream, inserter := range inserters {
				inserter.Close()
				if threadErr := <-errChans[stream]; threadErr != nil {
					err = fmt.Errorf("failed to write record for stream[%s]: %s", stream.ID(), threadErr)
				}
			}
			// no write error
			if err == nil {
				// first save state
				p.State.SetGlobal(waljs.WALState{LSN: socket.ClientXLogPos.String()})
				// mark lsn for wal logs drop
				// TODO: acknowledge message should be called every batch_size records synced or so to reduce the size of the WAL.
				err = socket.AcknowledgeLSN(ctx)
			}
		}
	}()

	// Message processing
	return socket.StreamMessages(ctx, func(msg waljs.CDCChange) error {
		pkFields := msg.Stream.GetStream().SourceDefinedPrimaryKey.Array()
		opType := utils.Ternary(msg.Kind == "delete", "d", utils.Ternary(msg.Kind == "update", "u", "c")).(string)
		return inserters[msg.Stream].Insert(types.CreateRawRecord(
			utils.GetKeysHash(msg.Data, pkFields...),
			msg.Data,
			opType,
			msg.Timestamp.Time,
		))
	})
}

func doesReplicationSlotExists(conn *sqlx.DB, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(
		"SELECT EXISTS(Select 1 from pg_replication_slots where slot_name = $1)",
		slotName,
	).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, validateReplicationSlot(conn, slotName)
}

func validateReplicationSlot(conn *sqlx.DB, slotName string) error {
	slot := waljs.ReplicationSlot{}
	err := conn.Get(&slot, fmt.Sprintf(waljs.ReplicationSlotTempl, slotName))
	if err != nil {
		return err
	}

	if slot.Plugin != "wal2json" {
		return fmt.Errorf("plugin not supported[%s]: driver only supports wal2json", slot.Plugin)
	}

	if slot.SlotType != "logical" {
		return fmt.Errorf("only logical slots are supported: %s", slot.SlotType)
	}

	return nil
}
