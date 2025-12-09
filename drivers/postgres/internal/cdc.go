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
		SSHClient:           p.sshClient,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     time.Duration(p.cdcConfig.InitialWaitTime) * time.Second,
		Tables:              types.NewSet(streams...),
		Publication:         p.cdcConfig.Publication,
	}, nil
}

func (p *Postgres) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return fmt.Errorf("failed to prepare wal config: %s", err)
	}

	replicator, err := waljs.NewReplicator(ctx, p.client, config, p.dataTypeConverter)
	if err != nil {
		return fmt.Errorf("failed to create wal connection: %s", err)
	}

	p.replicator = replicator
	socket := p.replicator.Socket()
	globalState := p.state.GetGlobal()
	fullLoadAck := func() error {
		p.state.SetGlobal(waljs.WALState{LSN: socket.CurrentWalPosition.String()})
		p.state.ResetStreams()

		// set lsn to start cdc from
		socket.ConfirmedFlushLSN = socket.CurrentWalPosition
		socket.ClientXLogPos = socket.CurrentWalPosition
		return waljs.AdvanceLSN(ctx, p.client, socket.ReplicationSlot, socket.CurrentWalPosition.String())
	}

	if globalState == nil || globalState.State == nil {
		if err := fullLoadAck(); err != nil {
			return fmt.Errorf("failed to ack lsn for full load: %s", err)
		}
	} else {
		// global state exist check for cursor and cursor mismatch
		var postgresGlobalState waljs.WALState
		if err = utils.Unmarshal(globalState.State, &postgresGlobalState); err != nil {
			return fmt.Errorf("failed to unmarshal global state: %s", err)
		}
		if postgresGlobalState.LSN == "" {
			if err := fullLoadAck(); err != nil {
				return fmt.Errorf("failed to ack lsn for full load: %s", err)
			}
		} else {
			parsed, err := pglogrepl.ParseLSN(postgresGlobalState.LSN)
			if err != nil {
				return fmt.Errorf("failed to parse stored lsn[%s]: %s", postgresGlobalState.LSN, err)
			}
			// failing sync when lsn mismatch found (from state and confirmed flush lsn), as otherwise on backfill, duplication of data will occur
			// suggesting to proceed with clear destination
			if parsed != socket.ConfirmedFlushLSN {
				return fmt.Errorf("lsn mismatch, please proceed with clear destination. lsn saved in state [%s] current lsn [%s]", parsed, socket.ConfirmedFlushLSN)
			}
		}
	}
	return nil
}

func (p *Postgres) StreamChanges(ctx context.Context, _ int, callback abstract.CDCMsgFn) error {
	// choose replicator via factory based on OutputPlugin config (default wal2json)
	return p.replicator.StreamChanges(ctx, p.client, callback)
}

func (p *Postgres) PostCDC(ctx context.Context, _ int, noErr bool) error {
	defer waljs.Cleanup(ctx, p.replicator.Socket())
	if noErr {
		socket := p.replicator.Socket()
		p.state.SetGlobal(waljs.WALState{LSN: socket.ClientXLogPos.String()})
		return waljs.AcknowledgeLSN(ctx, p.client, socket, false)
	}
	return nil
}

func doesReplicationSlotExists(ctx context.Context, conn *sqlx.DB, slotName string, publication string) (bool, error) {
	var exists bool
	err := conn.QueryRowContext(
		ctx,
		"SELECT EXISTS(Select 1 from pg_replication_slots where slot_name = $1)",
		slotName,
	).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, validateReplicationSlot(ctx, conn, slotName, publication)
}

func validateReplicationSlot(ctx context.Context, conn *sqlx.DB, slotName string, publication string) error {
	slot := waljs.ReplicationSlot{}
	err := conn.GetContext(ctx, &slot, fmt.Sprintf(waljs.ReplicationSlotTempl, slotName))
	if err != nil {
		return err
	}

	if slot.SlotType != "logical" {
		return fmt.Errorf("only logical slots are supported: %s", slot.SlotType)
	}

	logger.Debugf("replication slot[%s] with pluginType[%s] found", slotName, slot.Plugin)
	if slot.Plugin == "pgoutput" && publication == "" {
		return fmt.Errorf("publication is required for pgoutput")
	}
	return nil
}
