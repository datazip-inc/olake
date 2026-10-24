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

// NewConnection creates a binlog connection from the saved checkpoint.
func NewConnection(ctx context.Context, config *Config, state Binlog, streams []types.StreamInterface, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Connection, error) {
	var gtidSet mysql.GTIDSet
	if state.GTIDSet != nil {
		var err error
		gtidSet, err = parseGTIDSet(*state.GTIDSet)
		if err != nil {
			return nil, err
		}
	}
	var lowerCaseTableNames int
	// Completed destination recovery has no streams to replay and needs no live source.
	if config.SchemaClient != nil && len(streams) > 0 {
		if err := config.SchemaClient.QueryRowContext(ctx, jdbc.MySQLLowerCaseTableNamesQuery()).Scan(&lowerCaseTableNames); err != nil {
			return nil, fmt.Errorf("failed to read lower_case_table_names: %w", err)
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
		changeFilter:    NewChangeFilter(config.SchemaClient, state, lowerCaseTableNames != 0, typeConverter, streams...),
	}
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
		Option:          c.validateReplicationServer,
		Dialer:          newBinlogDialer(connectionCtx, config),
		// Replaying part of a transaction inside the syncer can duplicate rows already delivered.
		DisableRetrySync: true,
	}
	// SSH channels reject socket deadlines; their reads are interrupted by cancellation.
	if config.SSHClient == nil {
		syncerConfig.ReadTimeout = 2 * config.HeartbeatPeriod
	}
	// For state versions > 1, use the connection's configured timezone.
	// This ensures consistency between Full Refresh and CDC timestamps.
	// Older versions maintain UTC/Local depending on context for backward compatibility.
	if constants.LoadedStateVersion > 1 {
		syncerConfig.TimestampStringLocation = config.TimestampStringLocation
	}

	c.syncer = replication.NewBinlogSyncer(syncerConfig)
	return c, nil
}

// Validate the actual replication session; the SQL connection may use another server.
func (c *Connection) validateReplicationServer(conn *client.Conn) error {
	if c.ServerUUID == "" && c.gtidSet == nil {
		return nil
	}
	query := jdbc.MySQLServerUUIDQuery()
	if c.gtidSet != nil || c.targetGTIDSet != nil {
		query = jdbc.MySQLGTIDStateQuery()
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
		if err := c.Checkpoint().ValidateServerUUID(serverUUID); err != nil {
			return err
		}
		if c.targetGTIDSet == nil {
			return nil
		}
	}
	values := make([]string, 3)
	for i := range values {
		values[i], err = result.GetString(0, i+1)
		if err != nil {
			return fmt.Errorf("failed to read replication GTID history: %w", err)
		}
	}
	saved := c.gtidSet
	if saved == nil {
		saved = c.targetGTIDSet
	}
	if err := validateGTIDHistory(saved, c.targetGTIDSet, values[0], values[1], values[2]); err != nil {
		return err
	}
	c.replicationUUID = serverUUID
	return nil
}

func newBinlogDialer(connectionCtx context.Context, config *Config) client.Dialer {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
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
}

// isRecovery reports whether the caller fixed the stop position, as a recovery run does: it must
// end exactly there, where a previous run already committed to the destination.
func isRecovery(stop mysql.Position) bool {
	return stop.Name != "" && stop.Pos != 0
}

// lastPosToRead includes newly loaded schemas' anchors only for normal file-position syncs.
// Recovery boundaries and GTID targets must remain fixed.
func (c *Connection) lastPosToRead(latestBinlogPos mysql.Position, recovery bool) mysql.Position {
	if recovery || c.gtidSet != nil {
		return latestBinlogPos
	}
	if anchor := c.changeFilter.schema.maxAnchor(); anchor.Compare(latestBinlogPos) > 0 {
		return anchor
	}
	return latestBinlogPos
}

type syncMode int

const (
	fileMode syncMode = iota
	migrationMode
	gtidMode
)

type syncRun struct {
	mode       syncMode
	target     Binlog
	recovering bool
	resumePos  mysql.Position
	streamer   *replication.BinlogStreamer
}

func (c *Connection) StreamMessages(ctx context.Context, client *sqlx.DB, latestState Binlog, callback abstract.CDCMsgFn) error {
	run, err := c.prepareSyncTarget(ctx, client, latestState)
	if err != nil {
		return err
	}

	startTime := time.Now()
	messageReceived := false
	var transaction gtidTransaction
	var rebuild gtidRebuild

	for ctx.Err() == nil {
		reached, err := c.reachedTarget(&run, &rebuild)
		if err != nil || reached {
			return err
		}
		if !messageReceived && c.initialWaitTime > 0 && time.Since(startTime) > c.initialWaitTime {
			if run.mode != fileMode {
				return fmt.Errorf("initial wait time expired before reaching the MySQL GTID sync target")
			}
			logger.Warnf("no records found in given initial wait time, try increasing it")
			return nil
		}

		ev, err := c.nextEvent(ctx, run.streamer)
		if err != nil {
			return err
		}
		var completed mysql.GTIDSet
		withinTarget := true
		switch run.mode {
		case gtidMode:
			completed, err = transaction.consume(ev)
			withinTarget = transaction.gtid == nil || c.targetGTIDSet.Contain(transaction.gtid)
		case migrationMode:
			withinTarget, err = rebuild.observe(ev, c.CurrentPos, run.resumePos)
		}
		if err != nil {
			return err
		}
		// Update current position
		if ev.Header.LogPos > 0 {
			c.CurrentPos.Pos = ev.Header.LogPos
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			c.CurrentPos.Name = string(e.NextLogName)
			if run.mode == gtidMode {
				c.ServerUUID = c.replicationUUID
			}
			if e.Position > math.MaxUint32 {
				return fmt.Errorf("binlog position overflow: %d exceeds uint32 max value", e.Position)
			}
			c.CurrentPos.Pos = uint32(e.Position)
			logger.Infof("Binlog rotated to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos)

		case *replication.GTIDEvent:
			if run.mode != fileMode {
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
			// Apply DDL in stream order, excluding already processed migration history
			// and transactions outside the fixed GTID target.
			if withinTarget {
				c.changeFilter.schema.applyDDL(string(e.Query), string(e.Schema), c.CurrentPos, transaction.gtid)
			}
		}
		if completed != nil && withinTarget {
			if err := c.gtidSet.Update(completed.String()); err != nil {
				return fmt.Errorf("failed to advance completed GTID checkpoint: %w", err)
			}
		}
	}
	return nil
}

func (c *Connection) prepareSyncTarget(ctx context.Context, client *sqlx.DB, latestState Binlog) (syncRun, error) {
	run := syncRun{recovering: latestState.GTIDSet != nil || isRecovery(latestState.Position), resumePos: c.CurrentPos}
	if latestState.GTIDSet == nil && (latestState.Position.Name == "" || latestState.Position.Pos == 0) {
		var err error
		latestState, err = GetCurrentBinlogState(ctx, client)
		if err != nil {
			return syncRun{}, fmt.Errorf("failed to get current binlog state: %w", err)
		}
		if c.gtidSet == nil {
			if err := c.Checkpoint().ValidateServerUUID(latestState.ServerUUID); err != nil {
				return syncRun{}, err
			}
		}
	}

	switch {
	case c.gtidSet != nil:
		run.mode = gtidMode
		if latestState.GTIDSet == nil {
			return syncRun{}, errs.Precondition(errs.CDCPositionLost, "mysql.gtid_disabled",
				fmt.Errorf("%w: a GTID checkpoint requires gtid_mode=ON on the source", constants.ErrNonRetryable))
		}
		var err error
		c.targetGTIDSet, err = parseGTIDSet(*latestState.GTIDSet)
		if err != nil {
			return syncRun{}, err
		}
	case latestState.GTIDSet != nil:
		run.mode = migrationMode
		if err := c.prepareMigration(latestState); err != nil {
			return syncRun{}, err
		}
	default:
		run.mode = fileMode
	}
	run.target = latestState
	logger.Infof("Starting MySQL CDC from %s:%d to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos, latestState.Position.Name, latestState.Position.Pos)

	var err error
	if run.mode == gtidMode {
		logger.Infof("Resuming MySQL CDC with GTIDs")
		run.streamer, err = c.syncer.StartSyncGTID(c.gtidSet.Clone())
	} else {
		startPos := run.resumePos
		if run.mode == migrationMode {
			// Reconstruct GTIDs and table maps, including checkpoints inside a transaction.
			startPos.Pos = 4
		}
		run.streamer, err = c.syncer.StartSync(startPos)
	}
	if err != nil {
		return syncRun{}, fmt.Errorf("failed to start binlog sync: %w", err)
	}
	return run, nil
}

func (c *Connection) nextEvent(ctx context.Context, streamer *replication.BinlogStreamer) (*replication.BinlogEvent, error) {
	if c.readTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.readTimeout)
		defer cancel()
	}
	event, err := streamer.GetEvent(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog event: %w", err)
	}
	return event, nil
}

func (c *Connection) reachedTarget(run *syncRun, rebuild *gtidRebuild) (bool, error) {
	run.target.Position = c.lastPosToRead(run.target.Position, run.recovering)
	if run.mode == gtidMode {
		return c.gtidSet.Contain(c.targetGTIDSet), nil
	}
	if c.CurrentPos.Compare(run.target.Position) < 0 || (run.mode == migrationMode && rebuild.gtidSet == nil) {
		return false, nil
	}
	if run.mode == migrationMode {
		if c.CurrentPos.Compare(run.target.Position) != 0 {
			return false, fmt.Errorf("binlog stream passed the GTID migration boundary")
		}
		if err := c.finishMigration(*rebuild, run.recovering); err != nil {
			return false, err
		}
	}
	logger.Infof("Reached the configured latest binlog position %s:%d; stopping CDC sync", c.CurrentPos.Name, c.CurrentPos.Pos)
	return true, nil
}

// Checkpoint returns progress for destination metadata and source state persistence.
func (c *Connection) Checkpoint() Binlog {
	state := Binlog{Position: c.CurrentPos, ServerUUID: c.ServerUUID, Schemas: c.changeFilter.schema.snapshot(c.CurrentPos, c.gtidSet)}
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

// ErrNoBinlogPosition means the server reported no binlog position, i.e. binary logging is off.
var ErrNoBinlogPosition = errors.New("no binlog position available")

// GetCurrentBinlogPosition retrieves the current binlog position from MySQL.
func GetCurrentBinlogPosition(ctx context.Context, client *sqlx.DB) (mysql.Position, error) {
	flavor, major, minor, err := jdbc.MySQLVersion(ctx, client)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get MySQL version: %w", err)
	}
	state, err := readBinlogStatus(ctx, client, flavor, major, minor)
	return state.Position, err
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
	var serverUUID, gtidMode string
	if mysqlFlavor == "MySQL" {
		if err := conn.QueryRowContext(ctx, jdbc.MySQLGTIDModeQuery()).Scan(&serverUUID, &gtidMode); err != nil {
			return Binlog{}, fmt.Errorf("failed to get MySQL server UUID: %w", err)
		}
	}

	state, err := readBinlogStatus(ctx, conn, mysqlFlavor, majorVersion, minorVersion)
	if err != nil {
		return Binlog{}, err
	}
	state.ServerUUID = serverUUID
	if gtidMode != "ON" {
		state.GTIDSet = nil
	}
	return state, nil
}

func readBinlogStatus(ctx context.Context, conn sqlx.QueryerContext, mysqlFlavor string, majorVersion, minorVersion int) (Binlog, error) {
	// Use the appropriate query based on the MySQL version
	query := utils.Ternary(mysqlFlavor == "MySQL" && (majorVersion > 8 || (majorVersion == 8 && minorVersion >= 4)), jdbc.MySQLMasterStatusQueryNew(), jdbc.MySQLMasterStatusQuery()).(string)

	rows, err := conn.QueryxContext(ctx, query)
	if err != nil {
		return Binlog{}, fmt.Errorf("failed to get master status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// Next is also false when reading the row failed; only an empty result means binlog is off
		if err := rows.Err(); err != nil {
			return Binlog{}, fmt.Errorf("failed to read master status: %w", err)
		}
		return Binlog{}, errs.Precondition(errs.CDCPreconditionFailed, "mysql.binlog_disabled", ErrNoBinlogPosition)
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

	state := Binlog{Position: mysql.Position{Name: file, Pos: position}}
	if mysqlFlavor == "MySQL" {
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
