package binlog

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
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
func NewConnection(_ context.Context, config *Config, pos mysql.Position, streams []types.StreamInterface, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Connection, error) {
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
	}

	if config.SSHClient != nil {
		syncerConfig.Dialer = func(_ context.Context, _, addr string) (net.Conn, error) {
			return config.SSHClient.Dial("tcp", addr)
		}
	}

	return &Connection{
		ServerID:        config.ServerID,
		syncer:          replication.NewBinlogSyncer(syncerConfig),
		CurrentPos:      pos,
		initialWaitTime: config.InitialWaitTime,
		changeFilter:    NewChangeFilter(typeConverter, streams...),
	}, nil
}

func (c *Connection) StreamMessages(ctx context.Context, client *sqlx.DB, callback abstract.CDCMsgFn) error {
	latestBinlogPos, err := GetCurrentBinlogPosition(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to get latest binlog position: %s", err)
	}

	if latestBinlogPos.Name == "" || latestBinlogPos.Pos == 0 {
		return fmt.Errorf("latest binlog position is not set")
	}

	logger.Infof("Starting MySQL CDC from %s:%d to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos, latestBinlogPos.Name, latestBinlogPos.Pos)

	streamer, err := c.syncer.StartSync(c.CurrentPos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %s", err)
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
				return fmt.Errorf("failed to get binlog event: %s", err)
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
					c.changeFilter.lastCommitTimestamp = time.UnixMicro(int64(e.OriginalCommitTimestamp)) // #nosec G115 - timestamp value is always within int64 range
				}

			case *replication.RowsEvent:
				messageReceived = true
				if err := c.changeFilter.FilterRowsEvent(ctx, e, ev, callback); err != nil {
					return err
				}
			}
		}
	}
}

// Cleanup terminates the binlog syncer.
func (c *Connection) Cleanup() {
	c.syncer.Close()
}

// GetCurrentBinlogPosition retrieves the current binlog position from MySQL.
func GetCurrentBinlogPosition(ctx context.Context, client *sqlx.DB) (mysql.Position, error) {
	// SHOW MASTER STATUS is not supported in MySQL 8.4 and after

	// Get MySQL version
	mysqlFlavor, majorVersion, minorVersion, err := jdbc.MySQLVersion(ctx, client)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get MySQL version: %s", err)
	}

	// Use the appropriate query based on the MySQL version
	query := utils.Ternary(mysqlFlavor == "MySQL" && (majorVersion > 8 || (majorVersion == 8 && minorVersion >= 4)), jdbc.MySQLMasterStatusQueryNew(), jdbc.MySQLMasterStatusQuery()).(string)

	rows, err := client.QueryContext(ctx, query)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get master status: %s", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return mysql.Position{}, fmt.Errorf("no binlog position available")
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, executeGtidSet string

	switch mysqlFlavor {
	case "MySQL":
		if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet); err != nil {
			return mysql.Position{}, fmt.Errorf("failed to scan MySQL binlog position: %s", err)
		}
	case "MariaDB":
		// MariaDB returns 4 columns: File, Position, Binlog_Do_DB, Binlog_Ignore_DB
		if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB); err != nil {
			return mysql.Position{}, fmt.Errorf("failed to scan MariaDB binlog position: %s", err)
		}
	default:
		return mysql.Position{}, fmt.Errorf("unsupported database flavor: %s", mysqlFlavor)
	}

	return mysql.Position{Name: file, Pos: position}, nil
}
