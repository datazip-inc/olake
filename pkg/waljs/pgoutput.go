package waljs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

type walChange struct {
	kind       string
	relationID uint32
	data       any
}

type transactionBatch struct {
	commitTime time.Time
	changes    []walChange
}

type pgoutputReplicator struct {
	socket               *Socket
	publication          string
	workerCount          int
	txnCommitTime        time.Time
	relationIDToMsgMap   map[uint32]*pglogrepl.RelationMessage
	relationMu           sync.RWMutex
	transactionCompleted bool
}

func newPgoutputReplicator(socket *Socket, publication string, workerCount int) *pgoutputReplicator {
	if workerCount <= 0 {
		workerCount = 1
	}
	return &pgoutputReplicator{
		socket:             socket,
		publication:        publication,
		workerCount:        workerCount,
		relationIDToMsgMap: make(map[uint32]*pglogrepl.RelationMessage),
	}
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

	logger.Infof("pgoutput starting from lsn=%s target=%s workers=%d", p.socket.ConfirmedFlushLSN, p.socket.CurrentWalPosition, p.workerCount)

	if p.workerCount <= 1 {
		return p.streamChangesSequential(ctx, db, insertFn)
	}
	return p.streamChangesParallel(ctx, db, insertFn)
}

func (p *pgoutputReplicator) streamChangesSequential(ctx context.Context, db *sqlx.DB, insertFn abstract.CDCMsgFn) error {
	cdcStartTime := time.Now()
	messageReceived := false
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
				if err := p.processWALSequential(ctx, xld.WALData, insertFn); err != nil {
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

func (p *pgoutputReplicator) streamChangesParallel(ctx context.Context, db *sqlx.DB, insertFn abstract.CDCMsgFn) error {
	batchChan := make(chan *transactionBatch, p.workerCount*2)

	g, gCtx := errgroup.WithContext(ctx)

	for i := 0; i < p.workerCount; i++ {
		g.Go(func() error {
			return p.batchWorker(gCtx, batchChan, insertFn)
		})
	}

	g.Go(func() error {
		defer close(batchChan)
		return p.receiveAndBatch(gCtx, db, batchChan)
	})

	return g.Wait()
}

func (p *pgoutputReplicator) receiveAndBatch(ctx context.Context, db *sqlx.DB, batchChan chan<- *transactionBatch) error {
	cdcStartTime := time.Now()
	messageReceived := false
	transactionCompleted := true
	var currentBatch *transactionBatch

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && p.socket.initialWaitTime > 0 && time.Since(cdcStartTime) > p.socket.initialWaitTime {
				return fmt.Errorf("%s, try increasing it or do full load", constants.NoRecordsFoundError)
			}

			if transactionCompleted && p.socket.ClientXLogPos >= p.socket.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", p.socket.CurrentWalPosition)
				return nil
			}

			msgCtx, cancel := context.WithTimeout(ctx, p.socket.initialWaitTime)
			msg, err := p.socket.pgConn.ReceiveMessage(msgCtx)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("no records found in given initial wait time, try increasing it or do full load")
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

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("failed to parse WAL data: %v", err)
				}

				switch msg := logicalMsg.(type) {
				case *pglogrepl.RelationMessage:
					p.relationMu.Lock()
					p.relationIDToMsgMap[msg.RelationID] = msg
					p.relationMu.Unlock()

				case *pglogrepl.BeginMessage:
					transactionCompleted = false
					currentBatch = &transactionBatch{
						commitTime: msg.CommitTime,
						changes:    make([]walChange, 0),
					}

				case *pglogrepl.InsertMessage:
					if currentBatch != nil {
						currentBatch.changes = append(currentBatch.changes, walChange{
							kind:       "insert",
							relationID: msg.RelationID,
							data:       msg,
						})
					}

				case *pglogrepl.UpdateMessage:
					if currentBatch != nil {
						currentBatch.changes = append(currentBatch.changes, walChange{
							kind:       "update",
							relationID: msg.RelationID,
							data:       msg,
						})
					}

				case *pglogrepl.DeleteMessage:
					if currentBatch != nil {
						currentBatch.changes = append(currentBatch.changes, walChange{
							kind:       "delete",
							relationID: msg.RelationID,
							data:       msg,
						})
					}

				case *pglogrepl.CommitMessage:
					transactionCompleted = true
					if currentBatch != nil && len(currentBatch.changes) > 0 {
						select {
						case <-ctx.Done():
							return nil
						case batchChan <- currentBatch:
						}
					}
					currentBatch = nil
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

func (p *pgoutputReplicator) batchWorker(ctx context.Context, batchChan <-chan *transactionBatch, insertFn abstract.CDCMsgFn) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-batchChan:
			if !ok {
				return nil
			}
			if err := p.processBatch(ctx, batch, insertFn); err != nil {
				return err
			}
		}
	}
}

func (p *pgoutputReplicator) processBatch(ctx context.Context, batch *transactionBatch, insertFn abstract.CDCMsgFn) error {
	for _, change := range batch.changes {
		p.relationMu.RLock()
		rel, ok := p.relationIDToMsgMap[change.relationID]
		p.relationMu.RUnlock()

		if !ok {
			return fmt.Errorf("unknown relation id: %d", change.relationID)
		}

		stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
		if stream == nil {
			continue
		}

		var values map[string]any
		var err error

		switch change.kind {
		case "insert":
			msg := change.data.(*pglogrepl.InsertMessage)
			values, err = p.tupleValuesToMap(rel, msg.Tuple)
		case "update":
			msg := change.data.(*pglogrepl.UpdateMessage)
			values, err = p.tupleValuesToMap(rel, msg.NewTuple)
		case "delete":
			msg := change.data.(*pglogrepl.DeleteMessage)
			values, err = p.tupleValuesToMap(rel, msg.OldTuple)
		}

		if err != nil {
			return err
		}

		if err := insertFn(ctx, abstract.CDCChange{
			Stream:    stream,
			Timestamp: batch.commitTime,
			Kind:      change.kind,
			Data:      values,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *pgoutputReplicator) processWALSequential(ctx context.Context, walData []byte, insertFn abstract.CDCMsgFn) error {
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
		val, err := p.socket.changeFilter.converter(string(col.Data), typeName)
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

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "insert", Data: values})
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

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "update", Data: values})
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

	return insertFn(ctx, abstract.CDCChange{Stream: stream, Timestamp: p.txnCommitTime, Kind: "delete", Data: values})
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
