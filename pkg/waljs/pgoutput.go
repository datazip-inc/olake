package waljs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

// ProcessNextMessage handles the next replication message with retry logic for connection errors
func (s *Socket) ProcessNextMessage(ctx context.Context, db *sqlx.DB) error {
	// Update current lsn information
	var slot ReplicationSlot
	if err := db.Get(&slot, fmt.Sprintf(ReplicationSlotTempl, s.replicationSlot)); err != nil {
		return fmt.Errorf("failed to get replication slot: %s", err)
	}
	cdcStartTime := time.Now()
	messageReceived := false
	// Update current wal lsn
	s.CurrentWalPosition = slot.CurrentLSN
	logger.Info("current lsn that is being received: %s", s.CurrentWalPosition)
	err := pglogrepl.StartReplication(ctx, s.pgConn, s.replicationSlot, s.ConfirmedFlushLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names 'ankit_pub'", // Hard coded for now
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	lastStatusUpdate := time.Now()
	standbyMessageTimeout := 10 * time.Second
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && s.initialWaitTime > 0 && time.Since(cdcStartTime) > s.initialWaitTime {
				logger.Warnf("no records found in given initial wait time, try increasing it or do full load")
				return nil
			}

			if s.ClientXLogPos >= s.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", s.CurrentWalPosition)
				return nil
			}

			msg, err := s.pgConn.ReceiveMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}

			finalMsg, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("pgoutput unexpected message type: %T", msg)
			}

			switch finalMsg.Data[0] {
			case pglogrepl.XLogDataByteID:
				lsn, err := s.handleXLogData(finalMsg.Data[1:], &lastStatusUpdate)
				if err != nil {
					return err
				}
				messageReceived = true
				s.ClientXLogPos = *lsn
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				lsn, err := s.handlePrimaryKeepaliveMessage(ctx, finalMsg.Data[1:], &lastStatusUpdate)
				if err != nil {
					return err
				}
				s.ClientXLogPos = *lsn
			default:
				logger.Info("Received unexpected CopyData message type")
			}

			if time.Since(lastStatusUpdate) >= standbyMessageTimeout {
				if err := s.SendStandbyStatusUpdate(ctx); err != nil {
					return fmt.Errorf("failed to send standby status update: %v", err)
				}
				lastStatusUpdate = time.Now()
			}
		}
	}
}

// handleXLogData processes XLogData messages
func (s *Socket) handleXLogData(data []byte, lastStatusUpdate *time.Time) (*pglogrepl.LSN, error) {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse XLogData: %w", err)
	}

	if err := s.processWALData(xld.WALData, xld.WALStart); err != nil {
		return nil, fmt.Errorf("failed to process WAL data: %w", err)
	}

	*lastStatusUpdate = time.Now()
	return &xld.WALStart, nil
}

// processWALData handles different types of WAL messages
func (s *Socket) processWALData(walData []byte, lsn pglogrepl.LSN) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		s.handleRelationMessage(msg)
	case *pglogrepl.BeginMessage:
		return s.HandleBeginMessage(msg)
	case *pglogrepl.InsertMessage:
		return s.HandleInsertMessage(msg, lsn)
	case *pglogrepl.UpdateMessage:
		return s.HandleUpdateMessage(msg, lsn)
	case *pglogrepl.DeleteMessage:
		return s.HandleDeleteMessage(msg, lsn)
	case *pglogrepl.CommitMessage:
		return s.HandleCommitMessage(msg)
	default:
		logger.Info("Received unexpected logical replication message")
	}
	return nil
}

// handleRelationMessage handles RelationMessage messages
func (s *Socket) handleRelationMessage(msg *pglogrepl.RelationMessage) {
	s.relationID[msg.RelationID] = msg
	logger.Infof("relation id received: %d", msg.RelationID)
}

// HandleBeginMessage handles BeginMessage messages
func (s *Socket) HandleBeginMessage(msg *pglogrepl.BeginMessage) error {
	logger.Infof("Begin Message: LSN=%s, CommitTime=%v", msg.FinalLSN, msg.CommitTime)
	return nil
}

// HandleInsertMessage handles InsertMessage messages
func (s *Socket) HandleInsertMessage(msg *pglogrepl.InsertMessage, lsn pglogrepl.LSN) error {
	relation, ok := s.relationID[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Print insert record
	logger.Infof("INSERT: LSN=%s, Relation=%s.%s", lsn, relation.Namespace, relation.RelationName)
	for idx, col := range msg.Tuple.Columns {
		colName := relation.Columns[idx].Name
		if col.Data == nil {
			logger.Infof("  %s: NULL", colName)
		} else {
			logger.Infof("  %s: %s", colName, string(col.Data))
		}
	}
	return nil
}

// HandleUpdateMessage handles UpdateMessage messages
func (s *Socket) HandleUpdateMessage(msg *pglogrepl.UpdateMessage, lsn pglogrepl.LSN) error {
	relation, ok := s.relationID[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Print update record
	logger.Infof("UPDATE: LSN=%s, Relation=%s.%s", lsn, relation.Namespace, relation.RelationName)
	if msg.OldTuple != nil {
		logger.Info("  OLD VALUES:")
		for idx, col := range msg.OldTuple.Columns {
			colName := relation.Columns[idx].Name
			if col.Data == nil {
				logger.Infof("    %s: NULL", colName)
			} else {
				logger.Infof("    %s: %s", colName, string(col.Data))
			}
		}
	}
	logger.Info("  NEW VALUES:")
	for idx, col := range msg.NewTuple.Columns {
		colName := relation.Columns[idx].Name
		if col.Data == nil {
			logger.Infof("    %s: NULL", colName)
		} else {
			logger.Infof("    %s: %s", colName, string(col.Data))
		}
	}
	return nil
}

// HandleDeleteMessage handles DeleteMessage messages
func (s *Socket) HandleDeleteMessage(msg *pglogrepl.DeleteMessage, lsn pglogrepl.LSN) error {
	relation, ok := s.relationID[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Print delete record
	logger.Infof("DELETE: LSN=%s, Relation=%s.%s", lsn, relation.Namespace, relation.RelationName)
	for idx, col := range msg.OldTuple.Columns {
		colName := relation.Columns[idx].Name
		if col.Data == nil {
			logger.Infof("  %s: NULL", colName)
		} else {
			logger.Infof("  %s: %s", colName, string(col.Data))
		}
	}
	return nil
}

// HandleCommitMessage processes a commit message
func (s *Socket) HandleCommitMessage(msg *pglogrepl.CommitMessage) error {
	logger.Infof("Commit Message: LSN=%s, TransactionEndLSN=%s", msg.CommitLSN, msg.TransactionEndLSN)
	return nil
}

// SendStandbyStatusUpdate sends a status update to the primary server
func (s *Socket) SendStandbyStatusUpdate(ctx context.Context) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.ClientXLogPos,
		WALFlushPosition: s.ClientXLogPos,
		WALApplyPosition: s.ClientXLogPos,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}
	logger.Debugf("Sent standby status update at LSN: %s", s.ClientXLogPos)
	return nil
}

// handlePrimaryKeepaliveMessage processes PrimaryKeepaliveMessage messages
func (s *Socket) handlePrimaryKeepaliveMessage(ctx context.Context, data []byte, lastStatusUpdate *time.Time) (*pglogrepl.LSN, error) {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}
	logger.Debugf("Primary Keepalive Message: ServerWALEnd=%s, ServerTime=%v", pkm.ServerWALEnd, pkm.ServerTime)
	// if pkm.ReplyRequested {
	// 	if err := s.SendStandbyStatusUpdate(ctx); err != nil {
	// 		return nil, fmt.Errorf("failed to send standby status update: %w", err)
	// 	}
	// 	*lastStatusUpdate = time.Now()
	// }
	return &pkm.ServerWALEnd, nil
}
