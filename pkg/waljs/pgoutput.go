package waljs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

// pgoutputReplicator implements Replicator for pgoutput
type pgoutputReplicator struct {
	socket               *Socket
	publication          string
	txnCommitTime        time.Time                             // transaction commit time
	relationIDToMsgMap   map[uint32]*pglogrepl.RelationMessage // map to store relation id
	transactionCompleted bool                                  // if both begin and commit message received, then transaction is completed
}

func (p *pgoutputReplicator) Socket() *Socket {
	return p.socket
}

func (p *pgoutputReplicator) StreamChanges(ctx context.Context, db *sqlx.DB, insertFn abstract.CDCMsgFn) error {
	err := pglogrepl.StartReplication(ctx, p.socket.pgConn, fmt.Sprintf("%q", p.socket.ReplicationSlot), p.socket.ConfirmedFlushLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", p.publication)}})
	if err != nil {
		return fmt.Errorf("failed to start replication: %v", err)
	}

	logger.Infof("pgoutput starting from lsn=%s target=%s", p.socket.ConfirmedFlushLSN, p.socket.GetEndPosition())

	cdcStartTime := time.Now()
	messageReceived := false
	// transactionCompleted default true
	p.transactionCompleted = true

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && p.socket.initialWaitTime > 0 && time.Since(cdcStartTime) > p.socket.initialWaitTime {
				return fmt.Errorf("%w, try increasing it or do full load", constants.ErrNonRetryable)
			}

			if p.transactionCompleted && p.socket.ClientXLogPos >= p.socket.GetEndPosition() {
				logger.Infof("finishing sync, reached wal position: %s", p.socket.GetEndPosition())
				return nil
			}

			// receive message with timeout
			msgCtx, cancel := context.WithTimeout(ctx, p.socket.initialWaitTime)
			msg, err := p.socket.pgConn.ReceiveMessage(msgCtx)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("%w: no records found in given initial wait time, try increasing it or do full load", constants.ErrNonRetryable)
				}

				if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "EOF") {
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
				if err := p.processPgoutputWAL(ctx, xld.WALData, insertFn); err != nil {
					return err
				}
				p.socket.ClientXLogPos = xld.WALStart
				messageReceived = true
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %v", err)
				}
				p.socket.ClientXLogPos = pkm.ServerWALEnd
				if pkm.ReplyRequested {
					if err := AcknowledgeLSN(ctx, db, p.socket, true, p.socket.ClientXLogPos); err != nil {
						return fmt.Errorf("failed to send standby status update: %v", err)
					}
				}
			default:
				logger.Debugf("pgoutput: unhandled message type: %d", copyData.Data[0])
			}
		}
	}
}

// TODO: can we parallelize this function
func (p *pgoutputReplicator) processPgoutputWAL(ctx context.Context, walData []byte, insertFn abstract.CDCMsgFn) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %v", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		p.relationIDToMsgMap[msg.RelationID] = msg
		return nil
	case *pglogrepl.BeginMessage:
		p.transactionCompleted = false
		p.txnCommitTime = msg.CommitTime
		return nil
	case *pglogrepl.InsertMessage:
		return p.emitInsert(ctx, msg, insertFn)
	case *pglogrepl.UpdateMessage:
		return p.emitUpdate(ctx, msg, insertFn)
	case *pglogrepl.DeleteMessage:
		return p.emitDelete(ctx, msg, insertFn)
	case *pglogrepl.CommitMessage:
		p.transactionCompleted = true
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

		// Convert according to OID to string
		typeName := oidToString(colType)
		val, err := p.socket.changeFilter.converter(col.Data, typeName)
		if err != nil && err != typeutils.ErrNullValue {
			return nil, err
		}
		data[colName] = val
	}
	return data, nil
}

func (p *pgoutputReplicator) emitInsert(ctx context.Context, m *pglogrepl.InsertMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.relationIDToMsgMap[m.RelationID]
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

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "insert", Data: values, Position: p.socket.ConfirmedFlushLSN.String()})
}

func (p *pgoutputReplicator) emitUpdate(ctx context.Context, m *pglogrepl.UpdateMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.relationIDToMsgMap[m.RelationID]
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

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "update", Data: values, Position: p.socket.ConfirmedFlushLSN.String()})
}

func (p *pgoutputReplicator) emitDelete(ctx context.Context, m *pglogrepl.DeleteMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.relationIDToMsgMap[m.RelationID]
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

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "delete", Data: values, Position: p.socket.ConfirmedFlushLSN.String()})
}

// OIDToString converts a PostgreSQL OID to its string representation
func oidToString(oid uint32) string {
	if typeName, ok := oidToTypeName[oid]; ok {
		return typeName
	}
	logger.Warnf("unknown oid[%d] falling back to string", oid)
	// default to json, which will be converted to string
	return "json"
}

// OidToTypeName maps PostgreSQL OIDs to their corresponding type names
var oidToTypeName = map[uint32]string{
	pgtype.BoolOID:             "bool",
	pgtype.ByteaOID:            "bytea",
	pgtype.Int8OID:             "int8",
	pgtype.Int2OID:             "int2",
	pgtype.Int4OID:             "int4",
	pgtype.TextOID:             "text",
	pgtype.UUIDOID:             "uuid",
	pgtype.JSONOID:             "json",
	pgtype.Float4OID:           "float4",
	pgtype.Float8OID:           "float8",
	pgtype.BoolArrayOID:        "bool[]",
	pgtype.Int2ArrayOID:        "int2[]",
	pgtype.Int4ArrayOID:        "int4[]",
	pgtype.TextArrayOID:        "text[]",
	pgtype.ByteaArrayOID:       "bytea[]",
	pgtype.Int8ArrayOID:        "int8[]",
	pgtype.Float4ArrayOID:      "float4[]",
	pgtype.Float8ArrayOID:      "float8[]",
	pgtype.BPCharOID:           "bpchar",
	pgtype.VarcharOID:          "varchar",
	pgtype.DateOID:             "date",
	pgtype.TimeOID:             "time",
	pgtype.TimestampOID:        "timestamp",
	pgtype.TimestampArrayOID:   "timestamp[]",
	pgtype.DateArrayOID:        "date[]",
	pgtype.TimestamptzOID:      "timestamptz",
	pgtype.TimestamptzArrayOID: "timestamptz[]",
	pgtype.IntervalOID:         "interval",
	pgtype.NumericArrayOID:     "numeric[]",
	pgtype.BitOID:              "bit",
	pgtype.VarbitOID:           "varbit",
	pgtype.NumericOID:          "numeric",
	pgtype.UUIDArrayOID:        "uuid[]",
	pgtype.JSONBOID:            "jsonb",
	pgtype.JSONBArrayOID:       "jsonb[]",
}
