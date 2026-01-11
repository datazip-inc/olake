package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

const (
	ReplicationSlotTempl = "SELECT plugin, slot_type, confirmed_flush_lsn, pg_current_wal_lsn() as current_lsn FROM pg_replication_slots WHERE slot_name = '%s'"
)

// Socket represents a connection to PostgreSQL's logical replication stream
type Socket struct {
	// pgConn is the underlying PostgreSQL replication connection
	pgConn *pgconn.PgConn
	// clientXLogPos tracks the current position (while reading logs) in the Write-Ahead Log (WAL)
	ClientXLogPos pglogrepl.LSN
	// changeFilter filters WAL changes based on configured tables
	changeFilter ChangeFilter
	// confirmedLSN is the position from which replication should start (Prev marked lsn)
	ConfirmedFlushLSN pglogrepl.LSN
	// wal position at a point of time
	CurrentWalPosition pglogrepl.LSN
	// replicationSlot is the name of the PostgreSQL replication slot being used
	ReplicationSlot string
	// initialWaitTime is the duration to wait for first wal log catchup before timing out
	initialWaitTime time.Duration
}

// Replicator defines an abstraction over different logical decoding plugins.
type Replicator interface {
	// info about socket
	Socket() *Socket
	// StreamChanges processes messages until it emits changes via insertFn or exits per logic.
	StreamChanges(ctx context.Context, db *sqlx.DB, slot ReplicationSlot, insertFn abstract.CDCMsgFn) error
}

func NewReplicator(ctx context.Context, db *sqlx.DB, config *Config, slot ReplicationSlot, typeConverter func(value interface{}, columnType string) (interface{}, error)) (Replicator, error) {
	// Build PostgreSQL connection config
	connURL := config.Connection
	q := connURL.Query()
	q.Set("replication", "database")
	connURL.RawQuery = q.Encode()

	cfg, err := pgconn.ParseConfig(connURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection url: %s", err)
	}

	if config.SSHClient != nil {
		cfg.DialFunc = func(_ context.Context, _, addr string) (net.Conn, error) {
			return config.SSHClient.Dial("tcp", addr)
		}
	}

	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		logger.Warnf("notice received from pg conn: %s", n.Message)
	}

	cfg.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) {
		logger.Warnf("notification received from pg conn: %s", n.Payload)
	}

	cfg.OnPgError = func(_ *pgconn.PgConn, pe *pgconn.PgError) bool {
		logger.Warnf("pg conn thrown code[%s] and error: %s", pe.Code, pe.Message)
		// close connection and fail sync
		return false
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

	// Create and return final connection object
	socket := &Socket{
		pgConn:             pgConn,
		changeFilter:       NewChangeFilter(typeConverter, config.Tables.Array()...),
		ConfirmedFlushLSN:  slot.LSN,
		ClientXLogPos:      slot.LSN,
		CurrentWalPosition: slot.CurrentLSN,
		ReplicationSlot:    config.ReplicationSlotName,
		initialWaitTime:    config.InitialWaitTime,
	}

	plugin := strings.ToLower(strings.TrimSpace(slot.Plugin))
	switch plugin {
	case "pgoutput":
		return &pgoutputReplicator{socket: socket, publication: config.Publication, relationIDToMsgMap: make(map[uint32]*pglogrepl.RelationMessage)}, nil
	default:
		return &wal2jsonReplicator{socket: socket}, nil
	}
}

// advanceLSN advances the logical replication position to the current WAL position.
func AdvanceLSN(ctx context.Context, db *sqlx.DB, slot, currentWalPos string) error {
	// Get replication slot position
	if _, err := db.ExecContext(ctx, fmt.Sprintf(AdvanceLSNTemplate, slot, currentWalPos)); err != nil {
		return fmt.Errorf("failed to advance replication slot: %s", err)
	}
	logger.Debugf("advanced LSN to %s", currentWalPos)
	return nil
}

// Confirm that Logs has been recorded
// in fake ack prev confirmed flush lsn is sent
func AcknowledgeLSN(ctx context.Context, db *sqlx.DB, socket *Socket, fakeAck bool) error {
	walPosition := utils.Ternary(fakeAck, socket.ConfirmedFlushLSN, socket.ClientXLogPos).(pglogrepl.LSN)
	err := pglogrepl.SendStandbyStatusUpdate(ctx, socket.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: walPosition,
		WALFlushPosition: walPosition,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status message on wal position[%s]: %s", walPosition.String(), err)
	}

	logger.Debugf("sent standby status message at LSN#%s", walPosition.String())

	// if fakeAck, no need to wait for slot to be updated
	if fakeAck {
		return nil
	}

	// wait for slot to be updated
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			// stop waiting after 5 minutes or if parent ctx is canceled
			return fmt.Errorf("%w: %s", constants.ErrNonRetryable, "LSN not updated after 5 minutes")
		case <-ticker.C:
			slot, err := GetSlotPosition(ctx, db, socket.ReplicationSlot)
			if err != nil {
				return fmt.Errorf("failed to get slot position: %s", err)
			}

			if slot.LSN == walPosition {
				return nil
			}
		}
	}
}

// cleanup replicator
func Cleanup(ctx context.Context, socket *Socket) {
	if socket.pgConn != nil {
		_ = socket.pgConn.Close(ctx)
	}
}

func GetSlotPosition(ctx context.Context, db *sqlx.DB, replicationSlotName string) (ReplicationSlot, error) {
	// Get replication slot position
	var slot ReplicationSlot
	err := db.GetContext(ctx, &slot, fmt.Sprintf(ReplicationSlotTempl, replicationSlotName))
	return slot, err
}
