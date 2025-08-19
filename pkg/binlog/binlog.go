package binlog

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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
	return &Connection{
		ServerID:        config.ServerID,
		syncer:          replication.NewBinlogSyncer(syncerConfig),
		CurrentPos:      pos,
		initialWaitTime: config.InitialWaitTime,
		changeFilter:    NewChangeFilter(typeConverter, streams...),
	}, nil
}

func (c *Connection) StreamMessages(ctx context.Context, callback abstract.CDCMsgFn) error {
	logger.Infof("Starting MySQL CDC from binlog position %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos)
	streamer, err := c.syncer.StartSync(c.CurrentPos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %s", err)
	}
	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Check if we’ve been idle too long
			if time.Since(startTime) > c.initialWaitTime {
				logger.Debug("Idle timeout reached, exiting bin syncer")
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
				startTime = time.Now()
				c.CurrentPos.Name = string(e.NextLogName)
				if e.Position > math.MaxUint32 {
					return fmt.Errorf("binlog position overflow: %d exceeds uint32 max value", e.Position)
				}
				c.CurrentPos.Pos = uint32(e.Position)
				logger.Infof("Binlog rotated to %s:%d", c.CurrentPos.Name, c.CurrentPos.Pos)

			case *replication.RowsEvent:
				startTime = time.Now()
				if err := c.changeFilter.FilterRowsEvent(ctx, e, ev, callback); err != nil {
					return err
				}
			}
		}
	}
}

// Close terminates the binlog syncer.
func (c *Connection) Cleanup() {
	c.syncer.Close()
}
