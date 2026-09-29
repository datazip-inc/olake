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
	ServerID        uint32
	initialWaitTime time.Duration
	changeFilter    ChangeFilter // Filter for processing binlog events
}

// NewConnection creates a new binlog connection starting from the given position.
func NewConnection(ctx context.Context, config *Config, state Binlog, streams []types.StreamInterface, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Connection, error) {
	connectionCtx, cancel := context.WithCancel(ctx)
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
	}
	// SSH channels reject socket deadlines; their reads are interrupted by cancellation.
	if config.SSHClient == nil {
		syncerConfig.ReadTimeout = 2 * config.HeartbeatPeriod
	}
	if state.ServerUUID != "" {
		// The hook runs on the actual replication session, including reconnects.
		syncerConfig.Option = func(conn *client.Conn) error {
			result, err := conn.Execute("SELECT @@server_uuid")
			if err != nil {
				return fmt.Errorf("failed to get replication server UUID: %w", err)
			}
			serverUUID, err := result.GetString(0, 0)
			if err != nil {
				return fmt.Errorf("failed to read replication server UUID: %w", err)
			}
			return state.ValidateServerUUID(serverUUID)
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

	return &Connection{
		ServerID:        config.ServerID,
		syncer:          replication.NewBinlogSyncer(syncerConfig),
		cancel:          cancel,
		CurrentPos:      state.Position,
		ServerUUID:      state.ServerUUID,
		initialWaitTime: config.InitialWaitTime,
		changeFilter:    NewChangeFilter(config.SchemaClient, typeConverter, streams...),
	}, nil
}

func (c *Connection) StreamMessages(ctx context.Context, client *sqlx.DB, latestBinlogPos mysql.Position, callback abstract.CDCMsgFn) error {
	if latestBinlogPos.Name == "" || latestBinlogPos.Pos == 0 {
		latestState, err := GetCurrentBinlogState(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}
		if err := (Binlog{Position: c.CurrentPos, ServerUUID: c.ServerUUID}).ValidateServerUUID(latestState.ServerUUID); err != nil {
			return err
		}
		latestBinlogPos = latestState.Position
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

			// if the current position has reached or passed the latest binlog position, stop the syncer
			if c.CurrentPos.Compare(latestBinlogPos) >= 0 {
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
				// QueryEvent carries DDL even under binlog_format=ROW. Any DDL may have
				// reshaped a cached table, so drop the cache and reload lazily.
				if isDDL(e.Query) {
					logger.Infof("DDL observed in binlog, invalidating cached column metadata: %s", string(e.Query))
					c.changeFilter.schema.invalidate()
				}
			}
		}
	}
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
	if mysqlFlavor == "MySQL" {
		if err := conn.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&state.ServerUUID); err != nil {
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
