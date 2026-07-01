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

	logger.Infof("pgoutput starting from lsn=%s target=%s", p.socket.ConfirmedFlushLSN, p.socket.CurrentWalPosition)

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

			if p.transactionCompleted && p.socket.ClientXLogPos >= p.socket.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", p.socket.CurrentWalPosition)
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
				p.socket.ClientXLogPos = xld.WALStart
				if err := p.processPgoutputWAL(ctx, xld.WALData, insertFn); err != nil {
					return err
				}
				messageReceived = true
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %v", err)
				}
				p.socket.ClientXLogPos = pkm.ServerWALEnd
				if pkm.ReplyRequested {
					if err := AcknowledgeLSN(ctx, db, p.socket, true); err != nil {
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
		if _, relationVisited := p.relationIDToMsgMap[msg.RelationID]; !relationVisited && msg.ReplicaIdentity != 'f' {
			logger.Warnf("table[%s.%s] replica identity is not FULL, unchanged TOAST column values may be lost during CDC UPDATE events; set REPLICA IDENTITY FULL to avoid data loss", msg.Namespace, msg.RelationName)
		}
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

func (p *pgoutputReplicator) tupleValuesToMap(rel *pglogrepl.RelationMessage, tuple, oldTuple *pglogrepl.TupleData) (map[string]any, int64, error) {
	data := make(map[string]any)
	if tuple == nil {
		return data, 0, nil
	}

	// Compute on-disk bytes in one pass; attached to CDCChange.Bytes by the caller.
	rowBytes := tupleRowBytes(rel, tuple)

	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name
		colType := rel.Columns[idx].DataType
		// On UPDATE, unchanged TOAST columns in the new tuple are marked TupleDataTypeToast.
		// REPLICA IDENTITY FULL includes the complete old row and allows recovery of these values.
		// DEFAULT, INDEX, and NOTHING do not provide old TOAST values, so recovery is not possible.
		if col.DataType == pglogrepl.TupleDataTypeToast && oldTuple != nil && idx < len(oldTuple.Columns) {
			col = oldTuple.Columns[idx]
		}
		if col.Data == nil {
			data[colName] = nil
			continue
		}

		// Convert according to OID to string
		typeName := oidToString(colType)
		val, err := p.socket.changeFilter.converter(string(col.Data), typeName)
		if err != nil && err != typeutils.ErrNullValue {
			return nil, 0, err
		}
		data[colName] = val
	}
	return data, rowBytes, nil
}

// NumericBinaryBytes returns the size (in bytes) of a PostgreSQL NUMERIC value from
// its text representation (as sent by pgoutput or scanned by pgx), using PostgreSQL's base-10000 digit encoding
func NumericBinaryBytes(s string) int64 {
	if s == "" {
		return 3
	}
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	sl := strings.ToLower(s)
	if sl == "nan" || sl == "infinity" || sl == "inf" {
		return 3
	}

	var intPart, fracPart string
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	} else {
		intPart = s
	}
	intPart = strings.TrimLeft(intPart, "0")
	fracPart = strings.TrimRight(fracPart, "0")

	intDigits := (len(intPart) + 3) / 4
	fracDigits := (len(fracPart) + 3) / 4
	ndigits := int64(intDigits + fracDigits)
	return 3 + 2*ndigits
}

// oidStorageBytes returns the size of the data Olake reads for a column value with
// the given OID. Fixed-width types use their natural width; NUMERIC uses the
// base-10000 encoding; variable-width types use the length of the value pgoutput sent (as text).
func oidStorageBytes(oid uint32, data []byte) int64 {
	switch oid {
	case pgtype.Int2OID:
		return 2
	case pgtype.Int4OID:
		return 4
	case pgtype.Int8OID:
		return 8
	case pgtype.Float4OID:
		return 4
	case pgtype.Float8OID:
		return 8
	case pgtype.BoolOID:
		return 1
	case pgtype.DateOID:
		return 4
	case pgtype.TimeOID:
		return 8
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return 8
	case pgtype.UUIDOID:
		return 16
	case pgtype.NumericOID:
		// NUMERIC is variable-length; pgoutput sends it as text (e.g. "3.52").
		return NumericBinaryBytes(string(data))
	default:
		// Variable-width: VARCHAR, TEXT, BYTEA, JSON, JSONB, arrays, etc.
		return int64(len(data))
	}
}

// tupleRowBytes sums the data bytes of every non-NULL column in a CDC tuple — the
// data Olake reads for the row, excluding heap-tuple storage overhead.
func tupleRowBytes(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) int64 {
	var total int64
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) || col.Data == nil {
			continue // out-of-range or NULL / unchanged-TOAST columns carry no data
		}
		total += oidStorageBytes(rel.Columns[i].DataType, col.Data)
	}
	return total
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

	values, rowBytes, err := p.tupleValuesToMap(rel, m.Tuple, nil)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.CDCChange{
		Stream:       stream,
		Timestamp:    p.txnCommitTime,
		Kind:         "insert",
		Data:         values,
		ExtraColumns: map[string]any{CDCLSN: p.socket.ClientXLogPos.String()},
		Bytes:        rowBytes,
	})
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

	values, rowBytes, err := p.tupleValuesToMap(rel, m.NewTuple, m.OldTuple)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.CDCChange{
		Stream:       stream,
		Timestamp:    p.txnCommitTime,
		Kind:         "update",
		Data:         values,
		ExtraColumns: map[string]any{CDCLSN: p.socket.ClientXLogPos.String()},
		Bytes:        rowBytes,
	})
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

	values, rowBytes, err := p.tupleValuesToMap(rel, m.OldTuple, nil)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.CDCChange{
		Stream:       stream,
		Timestamp:    p.txnCommitTime,
		Kind:         "delete",
		Data:         values,
		ExtraColumns: map[string]any{CDCLSN: p.socket.ClientXLogPos.String()},
		Bytes:        rowBytes,
	})
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
