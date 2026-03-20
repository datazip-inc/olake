package driver

import (
	"context"
	"encoding/json"
	"errors"
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

	tlsConfig, err := p.config.buildTLSConfig()
	if err != nil && !errors.Is(err, utils.ErrTLSConfigNotRequired) {
		return nil, fmt.Errorf("failed to build tls config for wal replication: %s", err)
	}

	return &waljs.Config{
		Connection:          *p.config.Connection,
		SSHClient:           p.sshClient,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     time.Duration(p.cdcConfig.InitialWaitTime) * time.Second,
		Tables:              types.NewSet(streams...),
		TLSConfig:           tlsConfig,
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
		if err := waljs.AdvanceLSN(ctx, p.client, p.cdcConfig.ReplicationSlot, slot.CurrentLSN.String()); err != nil {
			return err
		}
	}
	p.streams = streams
	return nil
}

func (p *Postgres) StreamChanges(ctx context.Context, _ int, metadataStates map[string]any, callback abstract.CDCMsgFn) (any, error) {
	var postgresGlobalState waljs.WALState
	rawGlobalState := p.state.GetGlobal()
	if err := utils.Unmarshal(rawGlobalState.State, &postgresGlobalState); err != nil {
		return nil, fmt.Errorf("failed to unmarshal global state: %s", err)
	}

	var metadataCommittedLSN string
	var finishedStreams []string

	for streamID, rawMtState := range metadataStates {
		if rawMtState == nil {
			// No previous CDC state for this stream (fresh run), skip recovery check.
			continue
		}
		if stMtState, ok := rawMtState.(string); ok {
			var mtState waljs.WALState
			if err := json.Unmarshal([]byte(stMtState), &mtState); err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata state: %s", err)
			}

			if mtState.LSN != postgresGlobalState.LSN {
				metadataCommittedLSN = mtState.LSN
				finishedStreams = append(finishedStreams, streamID)
			}
		} else {
			return nil, fmt.Errorf("failed to typecast metadata state of type[%T] to string", rawMtState)
		}
	}

	var remainingStreams []types.StreamInterface
	// recoveryLSN is non-nil only when we need a bounded recovery sync; in the
	// normal path it stays nil so NewReplicator uses the live IdentifySystem LSN.
	var recoveryLSN *pglogrepl.LSN

	if len(finishedStreams) > 0 {
		// recovery sync required: read up to the LSN stored in the Iceberg metadata
		parsed, err := pglogrepl.ParseLSN(metadataCommittedLSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse recovery LSN %q: %s", metadataCommittedLSN, err)
		}
		recoveryLSN = &parsed

		finishedStreamSet := types.NewSet(finishedStreams...)
		_ = utils.ForEach(p.streams, func(stream types.StreamInterface) error {
			if exists := finishedStreamSet.Exists(stream.ID()); !exists {
				logger.Infof("Running recovery sync for stream[%s]", stream.ID())
				remainingStreams = append(remainingStreams, stream)
			}
			return nil
		})
	} else {
		remainingStreams = p.streams
	}

	config, err := p.prepareWALJSConfig(remainingStreams...)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare wal config: %s", err)
	}

	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to get slot position: %s", err)
	}

	replicator, err := waljs.NewReplicator(ctx, config, slot, recoveryLSN, p.dataTypeConverter)
	if err != nil {
		return nil, fmt.Errorf("failed to create wal connection: %s", err)
	}

	// persist replicator for post cdc
	p.replicator = replicator

	// validate global state (might got invalid during full load)
	if err := validateGlobalState(postgresGlobalState, slot.LSN); err != nil {
		return nil, fmt.Errorf("%s: invalid global state: %s", constants.ErrNonRetryable, err)
	}

	// choose replicator via factory based on OutputPlugin config (default wal2json)
	err = p.replicator.StreamChanges(ctx, p.client, callback)
	if err != nil {
		return nil, err
	}

	// In recovery mode the replicator exits as soon as ClientXLogPos >= recoveryLSN,
	// which means ClientXLogPos may overshoot (e.g. land on the next WAL boundary or a
	// keepalive ServerWALEnd).  If we let that overshoot propagate, PostCDC will
	// acknowledge the slot past the recovery target, permanently skipping any WAL
	// records that fall between the target and the overshoot.
	// Fix: pin ClientXLogPos to the exact recovery LSN so that both the Iceberg
	// metadata state (returned here) and the PostCDC slot acknowledgement land on
	// the same position that Iceberg already committed.
	if recoveryLSN != nil {
		p.replicator.Socket().ClientXLogPos = *recoveryLSN
		return waljs.WALState{LSN: metadataCommittedLSN}, nil
	}

	finalLSN := p.replicator.Socket().ClientXLogPos.String()
	return waljs.WALState{LSN: finalLSN}, nil
}

func (p *Postgres) PostCDC(ctx context.Context, _ int) error {
	if p.replicator == nil {
		return nil
	}
	defer waljs.Cleanup(ctx, p.replicator.Socket())
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		socket := p.replicator.Socket()
		finalLSN := socket.ClientXLogPos.String()
		p.state.SetGlobal(waljs.WALState{LSN: finalLSN})
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

func validateGlobalState(postgresGlobalState waljs.WALState, confirmedFlushLSN pglogrepl.LSN) error {
	// global state exist check for cursor and cursor mismatch
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
