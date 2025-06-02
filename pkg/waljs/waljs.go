package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

const (
	ReplicationSlotTempl = "SELECT plugin, slot_type, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'"
)

var pluginArguments = []string{
	"\"include-lsn\" 'on'",
	"\"pretty-print\" 'off'",
	"\"include-timestamp\" 'on'",
}

// Socket represents a connection to PostgreSQL's logical replication stream
type Socket struct {
	// pgConn is the underlying PostgreSQL replication connection
	pgConn *pgconn.PgConn
	// clientXLogPos tracks the current position in the Write-Ahead Log (WAL)
	ClientXLogPos pglogrepl.LSN
	// idleStartTime tracks when the connection last received data
	idleStartTime time.Time
	// changeFilter filters WAL changes based on configured tables
	changeFilter ChangeFilter
	// confirmedLSN is the position from which replication should start (Prev marked lsn)
	ConfirmedFlushLSN pglogrepl.LSN
	// replicationSlot is the name of the PostgreSQL replication slot being used
	replicationSlot string
	// initialWaitTime is the duration to wait for initial data before timing out
	initialWaitTime time.Duration
}

func NewConnection(ctx context.Context, db *sqlx.DB, config *Config, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Socket, error) {
	// Build PostgreSQL connection config
	connURL := config.Connection
	q := connURL.Query()
	q.Set("replication", "database")
	connURL.RawQuery = q.Encode()

	cfg, err := pgconn.ParseConfig(connURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection url: %s", err)
	}

	if config.TLSConfig != nil {
		// TODO: use proper TLS Configurations
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: false, MinVersion: tls.VersionTLS12}
	}

	// Establish PostgreSQL connection
	pgConn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %s", err)
	}

	// System identification
	sysident, err := pglogrepl.IdentifySystem(ctx, pgConn)
	if err != nil {
		return nil, fmt.Errorf("failed to indentify system: %s", err)
	}
	logger.Infof("SystemID:%s Timeline:%d XLogPos:%s Database:%s",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// Get replication slot position
	var slot ReplicationSlot
	if err := db.Get(&slot, fmt.Sprintf(ReplicationSlotTempl, config.ReplicationSlotName)); err != nil {
		return nil, fmt.Errorf("failed to get replication slot: %s", err)
	}

	// Create and return final connection object
	return &Socket{
		pgConn:            pgConn,
		changeFilter:      NewChangeFilter(typeConverter, config.Tables.Array()...),
		ConfirmedFlushLSN: slot.LSN,
		ClientXLogPos:     slot.LSN,
		replicationSlot:   config.ReplicationSlotName,
		initialWaitTime:   config.InitialWaitTime,
	}, nil
}

// Confirm that Logs has been recorded
func (s *Socket) AcknowledgeLSN(ctx context.Context) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.ClientXLogPos,
		WALFlushPosition: s.ClientXLogPos,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status message: %s", err)
	}

	// Update local pointer and state
	logger.Debugf("sent standby status message at LSN#%s", s.ClientXLogPos.String())
	return nil
}

func (s *Socket) StreamMessages(ctx context.Context, callback OnMessage) error {
	// Start logical replication with wal2json plugin arguments.
	// TODO: need research on if we need initial wait time or not (currently we are using idle time)
	if err := pglogrepl.StartReplication(
		ctx,
		s.pgConn,
		s.replicationSlot,
		s.ConfirmedFlushLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	); err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logger.Infof("Started logical replication on slot[%s]", s.replicationSlot)
	s.idleStartTime = time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if time.Since(s.idleStartTime) > s.initialWaitTime {
				logger.Debug("Idle timeout reached while waiting for new messages")
				return nil
			}
			// Use a context with timeout for receiving a message.
			msg, err := s.pgConn.ReceiveMessage(ctx)
			// If the receive timed out, log the idle state and continue waiting.
			if err != nil {
				return fmt.Errorf("failed to receive message from wal: %s", err)
			}

			// Process only CopyData messages.
			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("unexpected message type: %T", msg)
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// For keepalive messages, process them (but no ack is sent here).
				_, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %s", err)
				}

			case pglogrepl.XLogDataByteID:
				// Reset the idle timer on receiving WAL data.
				s.idleStartTime = time.Now()
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %s", err)
				}
				// Calculate new LSN based on the received WAL data.
				newLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				// Process change with the provided callback.
				if err := s.changeFilter.FilterChange(newLSN, xld.WALData, callback); err != nil {
					return fmt.Errorf("failed to filter change: %s", err)
				}
				// Update the current LSN pointer.
				s.ClientXLogPos = newLSN

			default:
				logger.Debugf("received unhandled message type: %v", copyData.Data[0])
			}
		}
	}
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Socket) Cleanup(ctx context.Context) {
	_ = s.pgConn.Close(ctx)
}
