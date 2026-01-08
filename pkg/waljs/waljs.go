package waljs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

const AdvanceLSNQuery = "SELECT * FROM pg_replication_slot_advance($1, $2)"

var pluginArguments = []string{
	"\"include-lsn\" 'on'",
	"\"pretty-print\" 'off'",
	"\"include-timestamp\" 'on'",
}

// wal2jsonReplicator implements Replicator for wal2json plugin
type wal2jsonReplicator struct {
	socket *Socket
}

func (w *wal2jsonReplicator) Socket() *Socket {
	return w.socket
}

func (w *wal2jsonReplicator) StreamChanges(ctx context.Context, db *sqlx.DB, callback abstract.CDCMsgFn) error {
	// update current lsn information
	var slot ReplicationSlot
	if err := db.GetContext(ctx, &slot, ReplicationSlotQuery, w.socket.ReplicationSlot); err != nil {
		return fmt.Errorf("failed to get replication slot: %s", err)
	}

	// update current wal lsn
	w.socket.CurrentWalPosition = slot.CurrentLSN

	// Start logical replication with wal2json plugin arguments.
	var tables []string
	for key := range w.socket.changeFilter.tables {
		tables = append(tables, key)
	}
	pluginArguments = append(pluginArguments, fmt.Sprintf("\"add-tables\" '%s'", strings.Join(tables, ",")))
	if err := pglogrepl.StartReplication(
		ctx,
		w.socket.pgConn,
		w.socket.ReplicationSlot,
		w.socket.ConfirmedFlushLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	); err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logger.Infof("Started logical replication for slot[%s] from lsn[%s] to lsn[%s]", w.socket.ReplicationSlot, w.socket.ConfirmedFlushLSN, w.socket.CurrentWalPosition)
	messageReceived := false
	cdcStartTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && w.socket.initialWaitTime > 0 && time.Since(cdcStartTime) > w.socket.initialWaitTime {
				return fmt.Errorf("%s, try increasing it or do full load", constants.NoRecordsFoundError)
			}

			if w.socket.ClientXLogPos >= w.socket.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", w.socket.CurrentWalPosition)
				return nil
			}

			msg, err := w.socket.pgConn.ReceiveMessage(ctx)
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					return nil
				}
				return fmt.Errorf("failed to receive message from wal logs: %s", err)
			}

			// Process only CopyData messages.
			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("unexpected message type: %T", msg)
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// For keepalive messages, process them (but no ack is sent here).
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %s", err)
				}
				w.socket.ClientXLogPos = pkm.ServerWALEnd
				if pkm.ReplyRequested {
					logger.Debugf("keep alive message received: %v", pkm)
					// send fake acknowledgement
					err := AcknowledgeLSN(ctx, db, w.socket, true)
					if err != nil {
						return fmt.Errorf("failed to ack lsn: %s", err)
					}
				}
			case pglogrepl.XLogDataByteID:
				// Reset the idle timer on receiving WAL data.
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %s", err)
				}
				// Process change with the provided callback.
				nextLSN, records, err := w.socket.changeFilter.FilterWalJsChange(ctx, xld.WALData, callback)
				if err != nil {
					return fmt.Errorf("failed to filter change: %s", err)
				}
				messageReceived = records > 0 || messageReceived
				w.socket.ClientXLogPos = *nextLSN
			default:
				logger.Warnf("received unhandled message type: %v", copyData.Data[0])
			}
		}
	}
}
