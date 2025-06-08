package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
)

func (p *Postgres) prepareWALJSConfig(streams ...types.StreamInterface) (*waljs.Config, error) {
	if !p.CDCSupport {
		return nil, fmt.Errorf("invalid call; %s not running in CDC mode", p.Type())
	}

	return &waljs.Config{
		Connection:          *p.config.Connection,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     time.Duration(p.cdcConfig.InitialWaitTime) * time.Second,
		Tables:              types.NewSet[types.StreamInterface](streams...),
		BatchSize:           p.config.BatchSize,
	}, nil
}

func (p *Postgres) PreCDC(ctx context.Context, state *types.State, streams []types.StreamInterface) error {
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return fmt.Errorf("failed to prepare wal config: %s", err)
	}

	socket, err := waljs.NewConnection(ctx, p.client, config, p.dataTypeConverter)
	if err != nil {
		return fmt.Errorf("failed to create wal connection: %s", err)
	}

	p.Socket = socket
	currentLSN := p.Socket.ConfirmedFlushLSN
	globalState := state.GetGlobal()

	if globalState == nil || globalState.State == nil {
		state.SetGlobal(waljs.WALState{LSN: currentLSN.String()})
		state.ResetStreams()
	} else {
		// global state exist check for cursor and cursor mismatch
		var postgresGlobalState waljs.WALState
		if err = utils.Unmarshal(globalState.State, &postgresGlobalState); err != nil {
			return fmt.Errorf("failed to unmarshal global state: %s", err)
		}
		if postgresGlobalState.LSN == "" {
			state.SetGlobal(waljs.WALState{LSN: currentLSN.String()})
			state.ResetStreams()
		} else {
			parsed, err := pglogrepl.ParseLSN(postgresGlobalState.LSN)
			if err != nil {
				return fmt.Errorf("failed to parse stored lsn[%s]: %s", postgresGlobalState.LSN, err)
			}
			// TODO: handle cursor mismatch with user input (Example: user provide if it has to fail or do full load with new resume token)
			if parsed != currentLSN {
				logger.Warnf("lsn mismatch, backfill will start again. prev lsn [%s] current lsn [%s]", parsed, currentLSN)
				state.SetGlobal(waljs.WALState{LSN: currentLSN.String()})
				state.ResetStreams()
			}
		}
	}
	return nil
}

func (p *Postgres) StreamChanges(ctx context.Context, _ types.StreamInterface, callback abstract.CDCMsgFn) error {
	return p.Socket.StreamMessages(ctx, callback)
}

func (p *Postgres) PostCDC(ctx context.Context, state *types.State, _ types.StreamInterface, noErr bool) error {
	defer p.Socket.Cleanup(ctx)
	if noErr {
		state.SetGlobal(waljs.WALState{LSN: p.Socket.ClientXLogPos.String()})
		// TODO: acknowledge message should be called every batch_size records synced or so to reduce the size of the WAL.
		return p.Socket.AcknowledgeLSN(ctx)
	}
	return nil
}

func doesReplicationSlotExists(conn *sqlx.DB, slotName string) (bool, error) {
	var walStatus, lagBytes string
	err := conn.QueryRow(
		"SELECT wal_status,  pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&walStatus, &lagBytes)
	if err != nil {
		return false, err
	}
	if walStatus == "extended" {
		return false, fmt.Errorf("replication slot got extended create new to proceed")
	}
	logger.Infof("total replication lag bytes: %s", lagBytes)
	return true, validateReplicationSlot(conn, slotName)
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
