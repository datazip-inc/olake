package binlog

import (
	"context"
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
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
)

// Connection manages the binlog syncer and streamer for multiple streams.
type Connection struct {
	syncer          *replication.BinlogSyncer
	cancel          context.CancelFunc
	CurrentPos      mysql.Position // Current binlog position
	ServerUUID      string
	gtidSet         mysql.GTIDSet
	targetGTIDSet   mysql.GTIDSet
	replicationUUID string
	readTimeout     time.Duration
	ServerID        uint32
	initialWaitTime time.Duration
	changeFilter    ChangeFilter // Filter for processing binlog events
}

// NewConnection creates a new binlog connection starting from the given position.
func NewConnection(ctx context.Context, config *Config, state Binlog, streams []types.StreamInterface, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Connection, error) {
	var gtidSet mysql.GTIDSet
	if state.GTIDSet != nil {
		var err error
		gtidSet, err = parseGTIDSet(*state.GTIDSet)
		if err != nil {
			return nil, err
		}
	}
	connectionCtx, cancel := context.WithCancel(ctx)
	c := &Connection{
		ServerID:        config.ServerID,
		cancel:          cancel,
		CurrentPos:      state.Position,
		ServerUUID:      state.ServerUUID,
		gtidSet:         gtidSet,
		readTimeout:     2 * config.HeartbeatPeriod,
		initialWaitTime: config.InitialWaitTime,
		changeFilter:    NewChangeFilter(config.SchemaClient, typeConverter, streams...),
	}
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:             config.ServerID,
		Flavor:               config.Flavor,
		Host:                 config.Host,
		Port:                 config.Port,
		User:                 config.User,
		Password:             config.Password,
		Charset:              config.Charset,
		VerifyChecksum:       config.VerifyChecksum,
		HeartbeatPeriod:      config.HeartbeatPeriod,
		TLSConfig:            config.TLSConfig,
		MaxReconnectAttempts: 1,
		// Replaying part of a transaction inside the syncer can duplicate rows already delivered.
		DisableRetrySync: state.GTIDSet != nil,
	}
	// SSH channels reject socket deadlines; their reads are interrupted by cancellation.
	if config.SSHClient == nil {
		syncerConfig.ReadTimeout = 2 * config.HeartbeatPeriod
	}
	if state.ServerUUID != "" || state.GTIDSet != nil {
		// The hook runs on the actual replication session, including reconnects.
		syncerConfig.Option = func(conn *client.Conn) error {
			query := "SELECT @@server_uuid"
			if c.gtidSet != nil {
				query += ", @@gtid_mode, @@global.gtid_executed, @@global.gtid_purged"
			}
			result, err := conn.Execute(query)
			if err != nil {
				return fmt.Errorf("failed to get replication server UUID: %w", err)
			}
			serverUUID, err := result.GetString(0, 0)
			if err != nil {
				return fmt.Errorf("failed to read replication server UUID: %w", err)
			}
			if c.gtidSet == nil {
				return state.ValidateServerUUID(serverUUID)
			}
			values := make([]string, 3)
			for i := range values {
				values[i], err = result.GetString(0, i+1)
				if err != nil {
					return fmt.Errorf("failed to read replication GTID history: %w", err)
				}
			}
			if err := validateGTIDHistory(c.gtidSet, c.targetGTIDSet, values[0], values[1], values[2]); err != nil {
				return err
			}
			c.replicationUUID = serverUUID
			return nil
		}
	}
	// For state versions > 1, use the connection's configured timezone.
	// This ensures consistency between Full Refresh and CDC timestamps.
	// Older versions maintain UTC/Local depending on context for backward compatibility.
	if constants.LoadedStateVersion > 1 {
		syncerConfig.TimestampStringLocation = config.TimestampStringLocation
	}

	syncerConfig.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if err := connectionCtx.Err(); err != nil {
			return nil, err
		}
		var conn net.Conn
		var err error
		if config.SSHClient != nil {
			conn, err = config.SSHClient.DialContext(ctx, "tcp", addr)
		} else {
			conn, err = (&net.Dialer{}).DialContext(ctx, network, addr)
		}
		if err != nil {
			return nil, err
		}
		// SSH channels do not support the read deadline used by the syncer's Close.
		context.AfterFunc(connectionCtx, func() { _ = conn.Close() })
		return conn, nil
	}

	c.syncer = replication.NewBinlogSyncer(syncerConfig)
	return c, nil
}

func (c *Connection) StreamMessages(ctx context.Context, client *sqlx.DB, latestState Binlog, callback abstract.CDCMsgFn) error {
	if latestState.GTIDSet == nil && (latestState.Position.Name == "" || latestState.Position.Pos == 0) {
		var err error
		latestState, err = GetCurrentBinlogState(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}
		if c.gtidSet == nil {
			if err := c.Checkpoint().ValidateServerUUID(latestState.ServerUUID); err != nil {
				return err
			}
		}
	}
	if c.gtidSet != nil {
		if latestState.GTIDSet == nil {
			return errs.Precondition(errs.CDCPositionLost, "mysql.gtid_disabled",
				fmt.Errorf("%w: a GTID checkpoint requires gtid_mode=ON on the source", constants.ErrNonRetryable))
		}
		var err error
		c.targetGTIDSet, err = parseGTIDSet(*latestState.GTIDSet)
		if err != nil {
			return err
		}
	}
	latestBinlogPos := latestState.Position

	logger.Infof("Starting MySQL CDC from %s:%d to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos, latestBinlogPos.Name, latestBinlogPos.Pos)

	var streamer *replication.BinlogStreamer
	var err error
	if c.gtidSet != nil {
		logger.Infof("Resuming MySQL CDC with GTIDs")
		streamer, err = c.syncer.StartSyncGTID(c.gtidSet.Clone())
	} else {
		streamer, err = c.syncer.StartSync(c.CurrentPos)
	}
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %w", err)
	}

	startTime := time.Now()
	messageReceived := false
	var transaction gtidTransaction

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if c.gtidSet != nil && c.gtidSet.Contain(c.targetGTIDSet) {
				return nil
			}
			if !messageReceived && c.initialWaitTime > 0 && time.Since(startTime) > c.initialWaitTime {
				if c.gtidSet != nil {
					return fmt.Errorf("initial wait time expired before reaching the MySQL GTID sync target")
				}
				logger.Warnf("no records found in given initial wait time, try increasing it")
				return nil
			}

			// if the current position has reached or passed the latest binlog position, stop the syncer
			if c.gtidSet == nil && c.CurrentPos.Compare(latestBinlogPos) >= 0 {
				logger.Infof("Reached the configured latest binlog position %s:%d; stopping CDC sync", c.CurrentPos.Name, c.CurrentPos.Pos)
				return nil
			}

			eventCtx := ctx
			cancel := func() {}
			if c.gtidSet != nil && c.readTimeout > 0 {
				eventCtx, cancel = context.WithTimeout(ctx, c.readTimeout)
			}
			ev, err := streamer.GetEvent(eventCtx)
			cancel()
			if err != nil {
				if err == context.DeadlineExceeded && c.gtidSet == nil {
					// Timeout means no event, continue to monitor idle time
					continue
				}
				return fmt.Errorf("failed to get binlog event: %w", err)
			}
			var completed mysql.GTIDSet
			withinTarget := true
			if c.gtidSet != nil {
				completed, err = transaction.consume(ev)
				if err != nil {
					return err
				}
				withinTarget = transaction.gtid == nil || c.targetGTIDSet.Contain(transaction.gtid)
			}
			// Update current position
			if ev.Header.LogPos > 0 {
				c.CurrentPos.Pos = ev.Header.LogPos
			}

			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				c.CurrentPos.Name = string(e.NextLogName)
				if c.gtidSet != nil {
					c.ServerUUID = c.replicationUUID
				}
				if e.Position > math.MaxUint32 {
					return fmt.Errorf("binlog position overflow: %d exceeds uint32 max value", e.Position)
				}
				c.CurrentPos.Pos = uint32(e.Position)
				logger.Infof("Binlog rotated to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos)

			case *replication.GTIDEvent:
				if c.gtidSet != nil {
					messageReceived = true
				}
				if e.OriginalCommitTimestamp > 0 {
					c.changeFilter.lastGTIDEvent = time.UnixMicro(int64(e.OriginalCommitTimestamp)) // #nosec G115 - timestamp value is always within int64 range
				}

				// TODO: Investigate MariaDB GTID event structure for microsecond timestamp support.

			case *replication.RowsEvent:
				if !withinTarget {
					continue
				}
				messageReceived = true
				if err := c.changeFilter.FilterRowsEvent(ctx, e, ev, c.CurrentPos, callback); err != nil {
					return err
				}

			case *replication.QueryEvent:
				// QueryEvent carries DDL even under binlog_format=ROW. Any DDL may have
				// reshaped a cached table, so drop the cache and reload lazily.
				if withinTarget && isDDL(e.Query) {
					logger.Infof("DDL observed in binlog, invalidating cached column metadata: %s", string(e.Query))
					c.changeFilter.schema.invalidate()
				}
			}
			if completed != nil && withinTarget {
				if err := c.gtidSet.Update(completed.String()); err != nil {
					return fmt.Errorf("failed to advance completed GTID checkpoint: %w", err)
				}
			}
		}
	}
}

// Checkpoint returns progress for destination metadata and source state persistence.
func (c *Connection) Checkpoint() Binlog {
	state := Binlog{Position: c.CurrentPos, ServerUUID: c.ServerUUID}
	if c.gtidSet != nil {
		value := c.gtidSet.String()
		state.GTIDSet = &value
	}
	return state
}

// Cleanup terminates the binlog syncer.
func (c *Connection) Cleanup() {
	// Close must not redial a changed endpoint to KILL a server-local connection ID.
	c.cancel()
	c.syncer.Close()
}

// GetCurrentBinlogState reads the position and its server identity on the same SQL session.
func GetCurrentBinlogState(ctx context.Context, client *sqlx.DB) (Binlog, error) {
	conn, err := client.Connx(ctx)
	if err != nil {
		return Binlog{}, fmt.Errorf("failed to acquire binlog state connection: %w", err)
	}
	defer conn.Close()

	// SHOW MASTER STATUS is not supported in MySQL 8.4 and after

	// Get MySQL version
	mysqlFlavor, majorVersion, minorVersion, err := jdbc.MySQLVersion(ctx, conn)
	if err != nil {
		return Binlog{}, fmt.Errorf("failed to get MySQL version: %w", err)
	}
	var state Binlog
	var gtidMode string
	if mysqlFlavor == "MySQL" {
		if err := conn.QueryRowContext(ctx, "SELECT @@server_uuid, @@gtid_mode").Scan(&state.ServerUUID, &gtidMode); err != nil {
			return Binlog{}, fmt.Errorf("failed to get MySQL server UUID: %w", err)
		}
	}

	// Use the appropriate query based on the MySQL version
	query := utils.Ternary(mysqlFlavor == "MySQL" && (majorVersion > 8 || (majorVersion == 8 && minorVersion >= 4)), jdbc.MySQLMasterStatusQueryNew(), jdbc.MySQLMasterStatusQuery()).(string)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return Binlog{}, fmt.Errorf("failed to get master status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return Binlog{}, fmt.Errorf("no binlog position available")
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, executeGtidSet string

	switch mysqlFlavor {
	case "MySQL":
		if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet); err != nil {
			return Binlog{}, fmt.Errorf("failed to scan MySQL binlog position: %w", err)
		}
	case "MariaDB":
		// MariaDB returns 4 columns: File, Position, Binlog_Do_DB, Binlog_Ignore_DB
		if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB); err != nil {
			return Binlog{}, fmt.Errorf("failed to scan MariaDB binlog position: %w", err)
		}
	default:
		return Binlog{}, fmt.Errorf("unsupported database flavor: %s", mysqlFlavor)
	}

	state.Position = mysql.Position{Name: file, Pos: position}
	if gtidMode == "ON" {
		state.GTIDSet = &executeGtidSet
	}
	return state, nil
}

// ValidateServerUUID rejects file positions whose known owner differs from the source.
func (b Binlog) ValidateServerUUID(serverUUID string) error {
	if b.ServerUUID == "" || b.ServerUUID == serverUUID {
		return nil
	}
	return errs.Precondition(errs.CDCPositionLost, "mysql.server_uuid_mismatch",
		fmt.Errorf("%w: MySQL server UUID changed from %q to %q; cannot resume binlog position %s on a different server; reconnect to the original server", constants.ErrNonRetryable, b.ServerUUID, serverUUID, b.Position))
}
