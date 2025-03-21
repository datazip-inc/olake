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
	syncer     *replication.BinlogSyncer   // Binlog syncer instance
	streamer   *replication.BinlogStreamer // Binlog event streamer
	currentPos mysql.Position              // Current binlog position
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
		syncer:     syncer,
		streamer:   streamer,
		currentPos: pos,
	}, nil
}

func (c *Connection) StreamMessages(ctx context.Context, filter ChangeFilter, callback OnChange, getMasterPos MasterPositionFunc) error {
	idleStartTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Check if we’ve been idle too long
			if time.Since(idleStartTime) > 10*time.Second {
				logger.Debug("Idle timeout reached, checking master position")
				masterPos, err := getMasterPos()
				if err != nil {
					return fmt.Errorf("failed to get master position: %w", err)
				}
				if c.currentPos.Name == masterPos.Name && c.currentPos.Pos == masterPos.Pos {
					logger.Infof("Caught up to master position %s:%d, stopping CDC", masterPos.Name, masterPos.Pos)
					return nil
				}
				// Reset idle timer and continue
				idleStartTime = time.Now()
			}

			// Use a short timeout to check for events frequently
			eventCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			ev, err := c.streamer.GetEvent(eventCtx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// Timeout means no event, continue to monitor idle time
					continue
				}
				return fmt.Errorf("failed to get binlog event: %w", err)
			}

			// Reset idle timer since we received an event
			idleStartTime = time.Now()

			// Update current position
			c.currentPos.Pos = ev.Header.LogPos

			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				c.currentPos.Name = string(e.NextLogName)
				if e.Position > math.MaxUint32 {
					return fmt.Errorf("binlog position overflow: %d exceeds uint32 max value", e.Position)
				}
				c.currentPos.Pos = uint32(e.Position)
				logger.Infof("Binlog rotated to %s:%d", c.currentPos.Name, c.currentPos.Pos)

			case *replication.RowsEvent:
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
