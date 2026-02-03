package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
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

func (p *Postgres) ChangeStreamConfig() (bool, bool, bool) {
	return true, false, false // sequential change stream
}

func (p *Postgres) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return fmt.Errorf("failed to get slot position: %s", err)
	}

	globalState := p.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		p.state.SetGlobal(waljs.WALState{LSN: slot.CurrentLSN.String()})
		p.state.ResetStreams()
		return waljs.AdvanceLSN(ctx, p.client, p.cdcConfig.ReplicationSlot, slot.CurrentLSN.String())
	}
	p.streams = streams
	return validateGlobalState(globalState, slot.LSN)
}

func (p *Postgres) StreamChanges(ctx context.Context, _ int, callback abstract.CDCMsgFn) error {
	config, err := p.prepareWALJSConfig(p.streams...)
	if err != nil {
		return fmt.Errorf("failed to prepare wal config: %s", err)
	}

	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return fmt.Errorf("failed to get slot position: %s", err)
	}

	replicator, err := waljs.NewReplicator(ctx, config, slot, p.dataTypeConverter)
	if err != nil {
		return fmt.Errorf("failed to create wal connection: %s", err)
	}

	// persist replicator for post cdc
	p.replicator = replicator

	// validate global state (might got invalid during full load)
	if err := validateGlobalState(p.state.GetGlobal(), slot.LSN); err != nil {
		return fmt.Errorf("%w: invalid global state: %s", constants.ErrNonRetryable, err)
	}

	// choose replicator via factory based on OutputPlugin config (default wal2json)
	return p.replicator.StreamChanges(ctx, p.client, callback)
}

func (p *Postgres) PostCDC(ctx context.Context, _ int) error {
	defer waljs.Cleanup(ctx, p.replicator.Socket())
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		socket := p.replicator.Socket()
		p.state.SetGlobal(waljs.WALState{LSN: socket.ClientXLogPos.String()})
		return waljs.AcknowledgeLSN(ctx, p.client, socket, false)
	}
}

func doesReplicationSlotExists(ctx context.Context, conn *sqlx.DB, slotName string, publication string, database string) (bool, error) {
	var exists bool
	err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database())`, slotName).Scan(&exists)
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

func validateGlobalState(globalState *types.GlobalState, confirmedFlushLSN pglogrepl.LSN) error {
	// global state exist check for cursor and cursor mismatch
	var postgresGlobalState waljs.WALState
	if err := utils.Unmarshal(globalState.State, &postgresGlobalState); err != nil {
		return fmt.Errorf("failed to unmarshal global state: %s", err)
	}
	if postgresGlobalState.LSN == "" {
		return fmt.Errorf("%w: lsn is empty, please proceed with clear destination", constants.ErrNonRetryable)
	} else {
		parsed, err := pglogrepl.ParseLSN(postgresGlobalState.LSN)
		if err != nil {
			return fmt.Errorf("failed to parse stored lsn[%s]: %s", postgresGlobalState.LSN, err)
		}
		// failing sync when lsn mismatch found (from state and confirmed flush lsn), as otherwise on backfill, duplication of data will occur
		// suggesting to proceed with clear destination
		if parsed != confirmedFlushLSN {
			return fmt.Errorf("%w: lsn mismatch, please proceed with clear destination. lsn saved in state [%s] current lsn [%s]", constants.ErrNonRetryable, parsed, confirmedFlushLSN)
		}
	}
	return nil
}
