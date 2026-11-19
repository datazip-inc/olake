package waljs

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ReplicationConn abstracts receiving replication messages and sending standby status updates,
// allowing the keepalive loop to be exercised with fakes in tests.
type ReplicationConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error
}

type pgConnAdapter struct {
	conn *pgconn.PgConn
}

func (a *pgConnAdapter) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return a.conn.ReceiveMessage(ctx)
}

func (a *pgConnAdapter) SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, a.conn, status)
}

// KeepaliveHandler manages a background goroutine that services keepalive messages
// between StreamChanges completion and PostCDC acknowledgment.
type KeepaliveHandler struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// StartKeepalive launches a background loop on conn that handles PrimaryKeepaliveMessage
// with ReplyRequested by sending standby status updates pinned to confirmedLSN.
func StartKeepalive(ctx context.Context, conn ReplicationConn, confirmedLSN pglogrepl.LSN) *KeepaliveHandler {
	loopCtx, cancel := context.WithCancel(ctx)
	h := &KeepaliveHandler{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go func() {
		defer close(h.done)
		serveKeepalives(loopCtx, conn, confirmedLSN)
	}()

	return h
}

// Stop signals the keepalive loop to stop and blocks until the goroutine has fully exited.
// It is safe to call multiple times concurrently or sequentially; it runs exactly once.
func (h *KeepaliveHandler) Stop() {
	if h == nil {
		return
	}
	h.stopOnce.Do(func() {
		h.cancel()
		<-h.done
	})
}

// serveKeepalives receives messages from conn with a short deadline (~1s).
// It responds to PrimaryKeepaliveMessage (with ReplyRequested) using confirmedLSN,
// discards XLogData, and terminates when ctx is canceled or a fatal receive error occurs.
func serveKeepalives(ctx context.Context, conn ReplicationConn, confirmedLSN pglogrepl.LSN) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		readCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		msg, err := conn.ReceiveMessage(readCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			logger.Debugf("keepalive loop stopped receiving: %v", err)
			return
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				logger.Warnf("failed to parse primary keepalive message during commit: %v", err)
				continue
			}
			if pkm.ReplyRequested {
				// HARD RULE: replies during the destination commit phase must report ONLY
				// the LSN that was already confirmed before this cycle started (confirmedLSN),
				// NEVER the target/walPosition. Destination writers are still in the middle of
				// committing; acknowledging the new target LSN now would tell Postgres it is
				// safe to discard WAL before the destination has persisted it. If a crash or
				// destination commit failure occurs, any LSN acknowledged past the last
				// committed state would be permanently skipped on restart, causing data loss.
				err := conn.SendStandbyStatusUpdate(ctx, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: confirmedLSN,
					WALFlushPosition: confirmedLSN,
					ClientTime:       time.Now(),
					ReplyRequested:   false,
				})
				if err != nil {
					logger.Warnf("failed to send standby status update during commit: %v", err)
				} else {
					logger.Debugf("sent keepalive standby status update at confirmed LSN#%s", confirmedLSN)
				}
			}
		case pglogrepl.XLogDataByteID:
			// Discard XLogData received during destination commit phase. Writers are in the
			// process of closing and committing; we cannot process new records until the next cycle.
			continue
		default:
			// ignore unknown message types
		}
	}
}
