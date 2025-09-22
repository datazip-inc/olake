package waljs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

// pgoutputReplicator implements Replicator for pgoutput
type pgoutputReplicator struct {
	socket        *Socket
	publications  []string
	txnCommitTime time.Time
}

func (p *pgoutputReplicator) Socket() *Socket {
	return p.socket
}

func (p *pgoutputReplicator) StreamChanges(ctx context.Context, db *sqlx.DB, insertFn abstract.CDCMsgFn) error {
	// Update current lsn information
	var slot ReplicationSlot
	if err := db.Get(&slot, fmt.Sprintf(ReplicationSlotTempl, p.socket.ReplicationSlot)); err != nil {
		return fmt.Errorf("failed to get replication slot: %s", err)
	}
	p.socket.CurrentWalPosition = slot.CurrentLSN

	logger.Infof("pgoutput starting from lsn=%s target=%s", p.socket.ConfirmedFlushLSN, p.socket.CurrentWalPosition)

	err := pglogrepl.StartReplication(ctx, p.socket.pgConn, p.socket.ReplicationSlot, p.socket.ConfirmedFlushLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", strings.Join(p.publications, ","))}})
	if err != nil {
		return fmt.Errorf("failed to start replication: %v", err)
	}

	lastStatusUpdate := time.Now()
	idleTimeout := 10 * time.Second
	cdcStartTime := time.Now()
	messageReceived := false

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && p.socket.initialWaitTime > 0 && time.Since(cdcStartTime) > p.socket.initialWaitTime {
				logger.Warnf("no records found in given initial wait time, try increasing it or do full load")
				return nil
			}

			if p.socket.ClientXLogPos >= p.socket.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", p.socket.CurrentWalPosition)
				return nil
			}

			msg, err := p.socket.pgConn.ReceiveMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				if strings.Contains(err.Error(), "EOF") {
					return nil
				}
				return err
			}
			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("pgoutput unexpected message type: %T", msg)
			}

			switch copyData.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %v", err)
				}
				if err := p.processPgoutputWAL(ctx, xld.WALData, xld.WALStart, insertFn); err != nil {
					return err
				}
				p.socket.ClientXLogPos = xld.WALStart
				lastStatusUpdate = time.Now()
				messageReceived = true
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// TODO: need to validate if it is required or not
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %v", err)
				}
				p.socket.ClientXLogPos = pkm.ServerWALEnd
				if pkm.ReplyRequested || time.Since(lastStatusUpdate) >= idleTimeout {
					if err := p.AcknowledgeLSN(ctx, true); err != nil {
						return fmt.Errorf("failed to send standby status update: %v", err)
					}
					lastStatusUpdate = time.Now()
				}
			default:
				logger.Debugf("pgoutput: unhandled message type: %d", copyData.Data[0])
			}
		}
	}
}

func (p *pgoutputReplicator) processPgoutputWAL(ctx context.Context, walData []byte, lsn pglogrepl.LSN, insertFn abstract.CDCMsgFn) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %v", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		p.socket.relationID[msg.RelationID] = msg
		return nil
	case *pglogrepl.BeginMessage:
		p.txnCommitTime = msg.CommitTime
		return nil
	case *pglogrepl.InsertMessage:
		return p.emitInsert(ctx, msg, insertFn)
	case *pglogrepl.UpdateMessage:
		return p.emitUpdate(ctx, msg, insertFn)
	case *pglogrepl.DeleteMessage:
		return p.emitDelete(ctx, msg, insertFn)
	case *pglogrepl.CommitMessage:
		return nil
	default:
		return nil
	}
}

func (p *pgoutputReplicator) tupleValuesToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, error) {
	data := make(map[string]any)
	if tuple == nil {
		return data, nil
	}

	// TODO: Verify logic first
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name
		colType := rel.Columns[idx].DataType
		if col.Data == nil {
			data[colName] = nil
			continue
		}
		// Convert according to OID inferred type mapping using existing converter where possible
		// We map OID to a generic type string so existing converter can be reused
		typeName := mapPgOIDToType(colType)
		val, err := p.socket.changeFilter.converter(string(col.Data), typeName)
		if err != nil && err != typeutils.ErrNullValue {
			return nil, err
		}
		data[colName] = val
	}
	return data, nil
}

func (p *pgoutputReplicator) emitInsert(ctx context.Context, m *pglogrepl.InsertMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.socket.relationID[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}

	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
	if stream == nil {
		return nil
	}

	values, err := p.tupleValuesToMap(rel, m.Tuple)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "insert", Data: values})
}

func (p *pgoutputReplicator) emitUpdate(ctx context.Context, m *pglogrepl.UpdateMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.socket.relationID[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}

	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
	if stream == nil {
		return nil
	}

	values, err := p.tupleValuesToMap(rel, m.NewTuple)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "update", Data: values})
}

func (p *pgoutputReplicator) emitDelete(ctx context.Context, m *pglogrepl.DeleteMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.socket.relationID[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}

	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
	if stream == nil {
		return nil
	}

	values, err := p.tupleValuesToMap(rel, m.OldTuple)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "delete", Data: values})
}

// Confirm that Logs has been recorded
func (p *pgoutputReplicator) AcknowledgeLSN(ctx context.Context, fakeAck bool) error {
	walPosition := utils.Ternary(fakeAck, p.socket.ConfirmedFlushLSN, p.socket.ClientXLogPos).(pglogrepl.LSN)

	err := pglogrepl.SendStandbyStatusUpdate(ctx, p.socket.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: walPosition,
		WALFlushPosition: walPosition,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status message on wal position[%s]: %s", walPosition.String(), err)
	}

	// Update local pointer and state
	logger.Debugf("sent standby status message at LSN#%s", walPosition.String())
	return nil
}

func (p *pgoutputReplicator) Cleanup(ctx context.Context) {
	if p.socket.pgConn != nil {
		_ = p.socket.pgConn.Close(ctx)
	}
}

func mapPgOIDToType(oid uint32) string {
	switch oid {
	case 16:
		return "boolean"
	case 20, 21, 23:
		return "integer"
	case 700, 701, 1700:
		return "numeric"
	case 25, 1043, 1042:
		return "text"
	case 1082:
		return "date"
	case 1083, 1266:
		return "time"
	case 1114, 1184:
		return "timestamp"
	case 17:
		return "bytea"
	default:
		return "text"
	}
}
