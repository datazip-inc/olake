package binlog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
)

// Connection manages the binlog syncer and streamer for multiple streams.
type Connection struct {
	syncer          *replication.BinlogSyncer
	CurrentPos      mysql.Position // Current binlog position
	ServerID        uint32
	initialWaitTime time.Duration
	changeFilter    ChangeFilter // Filter for processing binlog events
}

// NewConnection creates a new binlog connection starting from the given position.
func NewConnection(_ context.Context, config *Config, state Binlog, streams []types.StreamInterface, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Connection, error) {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:        config.ServerID,
		Flavor:          config.Flavor,
		Host:            config.Host,
		Port:            config.Port,
		User:            config.User,
		Password:        config.Password,
		Charset:         config.Charset,
		VerifyChecksum:  config.VerifyChecksum,
		HeartbeatPeriod: config.HeartbeatPeriod,
		TLSConfig:       config.TLSConfig,
	}
	// For state versions > 1, use the connection's configured timezone.
	// This ensures consistency between Full Refresh and CDC timestamps.
	// Older versions maintain UTC/Local depending on context for backward compatibility.
	if constants.LoadedStateVersion > 1 {
		syncerConfig.TimestampStringLocation = config.TimestampStringLocation
	}

	if config.SSHClient != nil {
		syncerConfig.Dialer = func(_ context.Context, _, addr string) (net.Conn, error) {
			return config.SSHClient.Dial("tcp", addr)
		}
	}

	conn := &Connection{
		ServerID:        config.ServerID,
		syncer:          replication.NewBinlogSyncer(syncerConfig),
		CurrentPos:      state.Position,
		initialWaitTime: config.InitialWaitTime,
		changeFilter:    NewChangeFilter(config.SchemaClient, state, typeConverter, streams...),
	}
	return conn, nil
}

// isRecovery reports whether the caller fixed the stop position, as a recovery run does: it must
// end exactly there, where a previous run already committed to the destination.
func isRecovery(stop mysql.Position) bool {
	return stop.Name != "" && stop.Pos != 0
}

// lastPosToRead is the position the run reads to. A recovery run stops exactly at latestBinlogPos.
// Otherwise it reads on to the newest cached schema's anchor when that is later (the server head
// when that table was loaded), so State can save every schema: the next run resumes there and
// drops schemas anchored later. Tables load while the run reads, so the answer can move.
func (c *Connection) lastPosToRead(latestBinlogPos mysql.Position, recovery bool) mysql.Position {
	if recovery {
		return latestBinlogPos
	}
	if anchor := c.changeFilter.schema.maxAnchor(); anchor.Compare(latestBinlogPos) > 0 {
		return anchor
	}
	return latestBinlogPos
}

func (c *Connection) StreamMessages(ctx context.Context, client *sqlx.DB, latestBinlogPos mysql.Position, callback abstract.CDCMsgFn) error {
	// Decided before latestBinlogPos is replaced below, which would make it look caller-given.
	recovery := isRecovery(latestBinlogPos)
	if !recovery {
		var err error
		latestBinlogPos, err = GetCurrentBinlogPosition(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}
	}

	logger.Infof("Starting MySQL CDC from %s:%d to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos, latestBinlogPos.Name, latestBinlogPos.Pos)

	streamer, err := c.syncer.StartSync(c.CurrentPos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %w", err)
	}

	startTime := time.Now()
	messageReceived := false

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && c.initialWaitTime > 0 && time.Since(startTime) > c.initialWaitTime {
				logger.Warnf("no records found in given initial wait time, try increasing it")
				return nil
			}

			// Stop once past the latest binlog position. Outside recovery, first read on past every
			// cached schema's anchor, which is the server head when that table was loaded, so State
			// can save it: the next run resumes here and drops schemas anchored later.
			if c.CurrentPos.Compare(latestBinlogPos) >= 0 && c.CurrentPos.Compare(c.lastPosToRead(latestBinlogPos, recovery)) >= 0 {
				logger.Infof("Reached the configured latest binlog position %s:%d; stopping CDC sync", c.CurrentPos.Name, c.CurrentPos.Pos)
				return nil
			}

			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				if err == context.DeadlineExceeded {
					// Timeout means no event, continue to monitor idle time
					continue
				}
				return fmt.Errorf("failed to get binlog event: %w", err)
			}
			// Update current position
			c.CurrentPos.Pos = ev.Header.LogPos

			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				c.CurrentPos.Name = string(e.NextLogName)
				if e.Position > math.MaxUint32 {
					return fmt.Errorf("binlog position overflow: %d exceeds uint32 max value", e.Position)
				}
				c.CurrentPos.Pos = uint32(e.Position)
				logger.Infof("Binlog rotated to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos)

			case *replication.GTIDEvent:
				if e.OriginalCommitTimestamp > 0 {
					c.changeFilter.lastGTIDEvent = time.UnixMicro(int64(e.OriginalCommitTimestamp)) // #nosec G115 - timestamp value is always within int64 range
				}

				// TODO: Investigate MariaDB GTID event structure for microsecond timestamp support.

			case *replication.RowsEvent:
				messageReceived = true
				if err := c.changeFilter.FilterRowsEvent(ctx, e, ev, c.CurrentPos, callback); err != nil {
					return err
				}

			case *replication.QueryEvent:
				// QueryEvent carries DDL even under binlog_format=ROW. Apply it to the cached
				// schemas in stream order, so they keep matching the rows that follow.
				c.changeFilter.schema.applyDDL(string(e.Query), string(e.Schema), c.CurrentPos)
			}
		}
	}
}

// State is the position to resume from, with the column metadata valid at it
func (c *Connection) State() Binlog {
	return Binlog{Position: c.CurrentPos, Schemas: c.changeFilter.schema.snapshot(c.CurrentPos)}
}

// Cleanup terminates the binlog syncer.
func (c *Connection) Cleanup() {
	c.syncer.Close()
}

// ErrNoBinlogPosition means the server reported no binlog position, i.e. binary logging is off.
var ErrNoBinlogPosition = errors.New("no binlog position available")

// GetCurrentBinlogPosition retrieves the current binlog position from MySQL.
func GetCurrentBinlogPosition(ctx context.Context, client *sqlx.DB) (mysql.Position, error) {
	// SHOW MASTER STATUS is not supported in MySQL 8.4 and after

	// Get MySQL version
	mysqlFlavor, majorVersion, minorVersion, err := jdbc.MySQLVersion(ctx, client)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get MySQL version: %w", err)
	}

	// Use the appropriate query based on the MySQL version
	query := utils.Ternary(mysqlFlavor == "MySQL" && (majorVersion > 8 || (majorVersion == 8 && minorVersion >= 4)), jdbc.MySQLMasterStatusQueryNew(), jdbc.MySQLMasterStatusQuery()).(string)

	rows, err := client.QueryContext(ctx, query)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get master status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// Next is also false when reading the row failed; only an empty result means binlog is off
		if err := rows.Err(); err != nil {
			return mysql.Position{}, fmt.Errorf("failed to read master status: %w", err)
		}
		return mysql.Position{}, errs.Precondition(errs.CDCPreconditionFailed, "mysql.binlog_disabled", ErrNoBinlogPosition)
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, executeGtidSet string

	switch mysqlFlavor {
	case "MySQL":
		if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet); err != nil {
			return mysql.Position{}, fmt.Errorf("failed to scan MySQL binlog position: %w", err)
		}
	case "MariaDB":
		// MariaDB returns 4 columns: File, Position, Binlog_Do_DB, Binlog_Ignore_DB
		if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB); err != nil {
			return mysql.Position{}, fmt.Errorf("failed to scan MariaDB binlog position: %w", err)
		}
	default:
		return mysql.Position{}, fmt.Errorf("unsupported database flavor: %s", mysqlFlavor)
	}

	return mysql.Position{Name: file, Pos: position}, nil
}
