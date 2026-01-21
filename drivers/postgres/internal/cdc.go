package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
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

	currentLSN := slot.CurrentLSN.String()

	logger.Infof("Current LSN: %s", currentLSN)

	globalState := p.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		p.state.SetGlobal(waljs.WALState{LSN: currentLSN})
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

	// Set target position for bounded sync if set (recovery mode)
	targetPos := p.GetTargetCDCPosition()
	if targetPos != "" {
		if err := p.replicator.Socket().SetTargetPosition(targetPos); err != nil {
			return fmt.Errorf("failed to set target position: %s", err)
		}
	}

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
		finalLSN := socket.ClientXLogPos

		if err := waljs.AcknowledgeLSN(ctx, p.client, socket, false, finalLSN); err != nil {
			return fmt.Errorf("failed to acknowledge LSN: %s", err)
		}

		var walState waljs.WALState
		globalState := p.state.GetGlobal()
		if globalState != nil && globalState.State != nil {
			if err := utils.Unmarshal(globalState.State, &walState); err != nil {
				logger.Warnf("Failed to unmarshal global state in PostCDC: %s", err)
			}
		}

		walState.LSN = finalLSN.String()
		// If all streams were successfully committed (processing is empty),
		// clear next_cdc_pos as we don't need it for recovery
		if len(walState.Processing) == 0 {
			walState.NextCDCPos = ""
		}
		p.state.SetGlobal(walState)
		return nil
	}
}

func (p *Postgres) GetCDCPosition() string {
	if p.replicator == nil {
		return ""
	}
	return p.replicator.Socket().ClientXLogPos.String()
}

// GetCDCStartPosition returns the starting CDC position from state for predictable thread IDs
func (p *Postgres) GetCDCStartPosition() string {
	var walState waljs.WALState
	globalState := p.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return ""
	}
	if err := utils.Unmarshal(globalState.State, &walState); err != nil {
		return ""
	}
	return walState.LSN
}

func (p *Postgres) SetNextCDCPosition(position string) {
	// Read existing state to preserve other fields
	var walState waljs.WALState
	globalState := p.state.GetGlobal()
	if globalState != nil && globalState.State != nil {
		if err := utils.Unmarshal(globalState.State, &walState); err != nil {
			logger.Warnf("Failed to unmarshal global state for SetNextCDCPosition: %s", err)
		}
	}
	walState.NextCDCPos = position
	p.state.SetGlobal(walState)
	logger.Infof("Set next_cdc_pos in state: %s", position)
}

func (p *Postgres) GetNextCDCPosition() string {
	globalState := p.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return ""
	}
	var walState waljs.WALState
	if err := utils.Unmarshal(globalState.State, &walState); err != nil {
		return ""
	}
	return walState.NextCDCPos
}

func (p *Postgres) SetCurrentCDCPosition(position string) {
	var walState waljs.WALState
	globalState := p.state.GetGlobal()
	if globalState != nil && globalState.State != nil {
		if err := utils.Unmarshal(globalState.State, &walState); err != nil {
			logger.Warnf("Failed to unmarshal global state for SetCurrentCDCPosition: %s", err)
		}
	}
	walState.LSN = position
	p.state.SetGlobal(walState)
	logger.Infof("Set current CDC position (LSN) in state: %s", position)
}

func (p *Postgres) SetProcessingStreams(streamIDs []string) {
	globalState := p.state.GetGlobal()
	if globalState == nil {
		logger.Warnf("SetProcessingStreams called but global state is nil")
		return
	}
	var walState waljs.WALState
	if err := utils.Unmarshal(globalState.State, &walState); err != nil {
		logger.Warnf("Failed to unmarshal global state for SetProcessingStreams: %s", err)
		return
	}
	walState.Processing = streamIDs
	p.state.SetGlobal(walState)
	logger.Infof("Set processing streams in state: %v", streamIDs)
}

func (p *Postgres) RemoveProcessingStream(streamID string) {
	globalState := p.state.GetGlobal()
	if globalState == nil {
		logger.Warnf("RemoveProcessingStream called but global state is nil")
		return
	}
	var walState waljs.WALState
	if err := utils.Unmarshal(globalState.State, &walState); err != nil {
		logger.Warnf("Failed to unmarshal global state for RemoveProcessingStream: %s", err)
		return
	}
	// Remove streamID from processing array
	newProcessing := make([]string, 0, len(walState.Processing))
	for _, s := range walState.Processing {
		if s != streamID {
			newProcessing = append(newProcessing, s)
		}
	}
	walState.Processing = newProcessing
	p.state.SetGlobal(walState)
	logger.Infof("Removed stream %s from processing, remaining: %v", streamID, newProcessing)
}

func (p *Postgres) GetProcessingStreams() []string {
	globalState := p.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return nil
	}
	var walState waljs.WALState
	if err := utils.Unmarshal(globalState.State, &walState); err != nil {
		return nil
	}
	return walState.Processing
}

func (p *Postgres) SetTargetCDCPosition(position string) {
	p.targetPosition = position
	if position != "" {
		logger.Infof("Set target CDC position for bounded sync: %s", position)
	}
}

func (p *Postgres) GetTargetCDCPosition() string {
	return p.targetPosition
}

// SaveNextCDCPositionForStream - no-op for Postgres (uses global position)
func (p *Postgres) SaveNextCDCPositionForStream(streamID string) {}

// CommitCDCPositionForStream - no-op for Postgres (uses global position)
func (p *Postgres) CommitCDCPositionForStream(streamID string) {}

// CheckPerStreamRecovery - no-op for Postgres (uses global position)
func (p *Postgres) CheckPerStreamRecovery(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error {
	return nil
}

// AcknowledgeCDCPosition acknowledges LSN to Postgres for recovery
func (p *Postgres) AcknowledgeCDCPosition(ctx context.Context, position string) error {
	if position == "" {
		return nil
	}

	// Use existing replicator if available
	if p.replicator != nil {
		lsn, err := pglogrepl.ParseLSN(position)
		if err != nil {
			return fmt.Errorf("failed to parse LSN for acknowledgment: %s", err)
		}
		logger.Infof("Acknowledging LSN %s to Postgres for recovery (via replicator)", position)
		return waljs.AcknowledgeLSN(ctx, p.client, p.replicator.Socket(), false, lsn)
	}

	// If no replicator, use AdvanceLSN which uses SQL to advance the slot
	slotName := p.cdcConfig.ReplicationSlot
	logger.Infof("Acknowledging LSN %s to Postgres for recovery (via AdvanceLSN)", position)
	return waljs.AdvanceLSN(ctx, p.client, slotName, position)
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
