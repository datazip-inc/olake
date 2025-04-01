package binlog

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Connection manages the binlog syncer and streamer for multiple streams.
type Connection struct {
	syncer          *replication.BinlogSyncer   // Binlog syncer instance
	streamer        *replication.BinlogStreamer // Binlog event streamer
	currentPos      mysql.Position              // Current binlog position
	initialWaitTime time.Duration
	idleStartTime   time.Time
}

// NewConnection creates a new binlog connection starting from the given position.
func NewConnection(_ context.Context, config *Config, pos mysql.Position) (*Connection, error) {
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
	syncer := replication.NewBinlogSyncer(syncerConfig)
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return nil, fmt.Errorf("failed to start binlog sync: %w", err)
	}
	return &Connection{
		syncer:          syncer,
		streamer:        streamer,
		currentPos:      pos,
		initialWaitTime: config.InitialWaitTime,
	}, nil
}

func (c *Connection) StreamMessages(ctx context.Context, filter ChangeFilter, callback OnChange) error {
	c.idleStartTime = time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Check if we’ve been idle too long
			if time.Since(c.idleStartTime) > c.initialWaitTime {
				logger.Debug("Idle timeout reached, exiting bin syncer")
				return nil
			}

			ev, err := c.streamer.GetEvent(ctx)
			if err != nil {
				if err == context.DeadlineExceeded {
					// Timeout means no event, continue to monitor idle time
					continue
				}
				return fmt.Errorf("failed to get binlog event: %w", err)
			}
			// Update current position
			c.currentPos.Pos = ev.Header.LogPos

			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				c.idleStartTime = time.Now()
				c.currentPos.Name = string(e.NextLogName)
				if e.Position > math.MaxUint32 {
					return fmt.Errorf("binlog position overflow: %d exceeds uint32 max value", e.Position)
				}
				c.currentPos.Pos = uint32(e.Position)
				logger.Infof("Binlog rotated to %s:%d", c.currentPos.Name, c.currentPos.Pos)

			case *replication.RowsEvent:
				c.idleStartTime = time.Now()
				if err := filter.FilterRowsEvent(e, ev, func(change CDCChange) error {
					change.Position = c.currentPos
					return callback(change)
				}); err != nil {
					return err
				}
			}
		}
	}
}

// Close terminates the binlog syncer.
func (c *Connection) Close() {
	c.syncer.Close()
}
