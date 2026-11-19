package waljs

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type fakeReplicationConn struct {
	mu            sync.Mutex
	msgCh         chan pgproto3.BackendMessage
	errCh         chan error
	statusUpdates []pglogrepl.StandbyStatusUpdate
	updateCh      chan pglogrepl.StandbyStatusUpdate
	closed        atomic.Bool
	inFlightRead  atomic.Int32
	onUpdate      func(status pglogrepl.StandbyStatusUpdate)
}

func newFakeReplicationConn() *fakeReplicationConn {
	return &fakeReplicationConn{
		msgCh:    make(chan pgproto3.BackendMessage, 10),
		errCh:    make(chan error, 10),
		updateCh: make(chan pglogrepl.StandbyStatusUpdate, 10),
	}
}

func (f *fakeReplicationConn) setOnUpdate(fn func(status pglogrepl.StandbyStatusUpdate)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onUpdate = fn
}

func (f *fakeReplicationConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	f.inFlightRead.Add(1)
	defer f.inFlightRead.Add(-1)

	if f.closed.Load() {
		return nil, io.EOF
	}

	select {
	case err := <-f.errCh:
		return nil, err
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-f.errCh:
		return nil, err
	case msg, ok := <-f.msgCh:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

func (f *fakeReplicationConn) SendStandbyStatusUpdate(_ context.Context, status pglogrepl.StandbyStatusUpdate) error {
	f.mu.Lock()
	f.statusUpdates = append(f.statusUpdates, status)
	callback := f.onUpdate
	f.mu.Unlock()

	if callback != nil {
		callback(status)
	}

	select {
	case f.updateCh <- status:
	default:
	}
	return nil
}

func buildPrimaryKeepaliveMsg(serverWALEnd pglogrepl.LSN, replyRequested bool) *pgproto3.CopyData {
	data := make([]byte, 1+8+8+1)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:9], uint64(serverWALEnd))
	binary.BigEndian.PutUint64(data[9:17], 0) // ServerTime
	if replyRequested {
		data[17] = 1
	} else {
		data[17] = 0
	}
	return &pgproto3.CopyData{Data: data}
}

func buildXLogDataMsg(walStart pglogrepl.LSN) *pgproto3.CopyData {
	data := make([]byte, 1+8+8+8+4)
	data[0] = pglogrepl.XLogDataByteID
	binary.BigEndian.PutUint64(data[1:9], uint64(walStart))
	binary.BigEndian.PutUint64(data[9:17], uint64(walStart+100))
	binary.BigEndian.PutUint64(data[17:25], 0) // ServerTime
	return &pgproto3.CopyData{Data: data}
}

// TestKeepaliveReplyUsesConfirmedLSN asserts that keepalives received during the destination
// commit phase reply with the OLD confirmed LSN, never the newer target LSN or keepalive ServerWALEnd.
func TestKeepaliveReplyUsesConfirmedLSN(t *testing.T) {
	defer goleak.VerifyNone(t)

	oldConfirmedLSN := pglogrepl.LSN(0x1000)
	targetLSN := pglogrepl.LSN(0x5000)
	keepaliveLSN := pglogrepl.LSN(0x6000)

	conn := newFakeReplicationConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := StartKeepalive(ctx, conn, oldConfirmedLSN)

	// Send an XLogData message; it should be discarded with no response
	conn.msgCh <- buildXLogDataMsg(targetLSN)

	// Send a PrimaryKeepaliveMessage with ReplyRequested: true
	conn.msgCh <- buildPrimaryKeepaliveMsg(keepaliveLSN, true)

	// Wait for the standby status update
	select {
	case update := <-conn.updateCh:
		assert.Equal(t, oldConfirmedLSN, update.WALWritePosition, "must report confirmed LSN, not target or keepalive LSN")
		assert.Equal(t, oldConfirmedLSN, update.WALFlushPosition, "must report confirmed LSN, not target or keepalive LSN")
		assert.False(t, update.ReplyRequested)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for standby status update")
	}

	// Send a keepalive with ReplyRequested: false; no update should be sent
	conn.msgCh <- buildPrimaryKeepaliveMsg(keepaliveLSN, false)

	select {
	case unexpected := <-conn.updateCh:
		t.Fatalf("unexpected standby status update for ReplyRequested=false: %+v", unexpected)
	case <-time.After(100 * time.Millisecond):
		// Expected: no update sent
	}

	handler.Stop()
}

// TestKeepaliveLoopStopsCleanly asserts that the keepalive loop stops without leaking goroutines on:
// 1. Explicit Stop() signal
// 2. Context cancellation (stops on ctx cancel)
// 3. Connection EOF
func TestKeepaliveLoopStopsCleanly(t *testing.T) {
	t.Run("stops on explicit Stop signal", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		conn := newFakeReplicationConn()
		ctx := context.Background()

		handler := StartKeepalive(ctx, conn, pglogrepl.LSN(0x1000))

		// Stop should exit cleanly
		handler.Stop()

		select {
		case <-handler.done:
			// OK: done channel closed
		case <-time.After(2 * time.Second):
			t.Fatal("keepalive loop did not stop within timeout")
		}

		// Idempotent Stop calls
		handler.Stop()
	})

	t.Run("stops on ctx cancel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		conn := newFakeReplicationConn()
		ctx, cancel := context.WithCancel(context.Background())

		handler := StartKeepalive(ctx, conn, pglogrepl.LSN(0x1000))

		// Cancel context simulating handleWriterCleanup failure or SIGTERM
		cancel()

		select {
		case <-handler.done:
			// OK: goroutine exited
		case <-time.After(2 * time.Second):
			t.Fatal("keepalive loop did not exit on context cancel")
		}

		handler.Stop()
	})

	t.Run("stops on connection EOF", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		conn := newFakeReplicationConn()
		ctx := context.Background()

		handler := StartKeepalive(ctx, conn, pglogrepl.LSN(0x1000))

		// Close connection channel
		close(conn.msgCh)

		select {
		case <-handler.done:
			// OK: loop terminated on read error
		case <-time.After(2 * time.Second):
			t.Fatal("keepalive loop did not exit on connection close")
		}

		handler.Stop()
	})
}

// TestKeepaliveLoopContinuesOnPgconnTimeout asserts that when ReceiveMessage returns a pgconn-style
// timeout error, the keepalive loop continues running and still answers subsequent keepalive messages.
func TestKeepaliveLoopContinuesOnPgconnTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeReplicationConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	confirmedLSN := pglogrepl.LSN(0x1000)
	handler := StartKeepalive(ctx, conn, confirmedLSN)

	// Generate a genuine pgconn timeout error via pgconn.Construct with net.Pipe
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	cfg, err := pgconn.ParseConfig("postgres://dummy:dummy@localhost:5432/dummy")
	require.NoError(t, err)
	pgConn, err := pgconn.Construct(&pgconn.HijackedConn{
		Config: cfg,
		Conn:   clientConn,
	})
	require.NoError(t, err)

	expiredCtx, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	expiredCancel()
	_, timeoutErr := pgConn.ReceiveMessage(expiredCtx)
	require.True(t, pgconn.Timeout(timeoutErr), "generated error must satisfy pgconn.Timeout")

	// Inject the pgconn timeout error
	conn.errCh <- timeoutErr

	// Follow up with a PrimaryKeepaliveMessage with ReplyRequested: true
	conn.msgCh <- buildPrimaryKeepaliveMsg(pglogrepl.LSN(0x2000), true)

	// Assert that the keepalive loop did not terminate on timeout and answered the keepalive
	select {
	case update := <-conn.updateCh:
		assert.Equal(t, confirmedLSN, update.WALWritePosition)
		assert.Equal(t, confirmedLSN, update.WALFlushPosition)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for keepalive update after pgconn timeout error")
	}

	handler.Stop()
}

// TestFinalAckSentOnlyAfterLoopStopped asserts that the final LSN acknowledgment in PostCDC
// occurs strictly AFTER the keepalive loop has fully stopped, verified by checking that
// in-flight ReceiveMessage calls are 0 and Stop() blocks until in-flight reads complete.
func TestFinalAckSentOnlyAfterLoopStopped(t *testing.T) {
	defer goleak.VerifyNone(t)

	confirmedLSN := pglogrepl.LSN(0x1000)
	finalTargetLSN := pglogrepl.LSN(0x5000)

	conn := newFakeReplicationConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	socket := &Socket{
		ConfirmedFlushLSN: confirmedLSN,
		ClientXLogPos:     finalTargetLSN,
		ReplicationSlot:   "test_slot",
	}
	socket.SetReplicationConn(conn)

	socket.StartKeepalive(ctx)

	// 1. Wait until the keepalive loop is actively waiting in ReceiveMessage (in-flight read == 1)
	require.Eventually(t, func() bool {
		return conn.inFlightRead.Load() == 1
	}, 500*time.Millisecond, 5*time.Millisecond, "loop should enter ReceiveMessage")

	// 2. Service a keepalive to verify loop is active
	conn.msgCh <- buildPrimaryKeepaliveMsg(pglogrepl.LSN(0x2000), true)
	select {
	case update := <-conn.updateCh:
		require.Equal(t, confirmedLSN, update.WALWritePosition)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for commit-phase keepalive reply")
	}

	// 3. Set up final ack verification: assert in-flight reads == 0 when final update is sent
	var finalAckSent atomic.Bool
	conn.setOnUpdate(func(status pglogrepl.StandbyStatusUpdate) {
		if status.WALWritePosition == finalTargetLSN {
			finalAckSent.Store(true)
			assert.Equal(t, int32(0), conn.inFlightRead.Load(), "must have 0 in-flight ReceiveMessage calls when final ack is sent")
		}
	})

	// Wait until the loop is back waiting in ReceiveMessage
	require.Eventually(t, func() bool {
		return conn.inFlightRead.Load() == 1
	}, 500*time.Millisecond, 5*time.Millisecond, "loop should be waiting in ReceiveMessage before stop")

	// Call StopKeepalive() - blocks until in-flight ReceiveMessage returns
	socket.StopKeepalive()

	// Once Stop returns, inFlightRead must be 0
	assert.Equal(t, int32(0), conn.inFlightRead.Load(), "in-flight ReceiveMessage count must be 0 after Stop()")

	// 4. Send final ack
	err := socket.getConn().SendStandbyStatusUpdate(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: finalTargetLSN,
		WALFlushPosition: finalTargetLSN,
		ClientTime:       time.Now(),
	})
	require.NoError(t, err)

	assert.True(t, finalAckSent.Load(), "final ack should have been sent")
}

// TestAcknowledgeLSNContextCancelReturnsCtxErr asserts that when parent context is canceled,
// AcknowledgeLSN returns ctx.Err() directly instead of "LSN not updated after 5 minutes" with ErrNonRetryable.
func TestAcknowledgeLSNContextCancelReturnsCtxErr(t *testing.T) {
	conn := newFakeReplicationConn()
	ctx, cancel := context.WithCancel(context.Background())

	socket := &Socket{
		ConfirmedFlushLSN: pglogrepl.LSN(0x1000),
		ClientXLogPos:     pglogrepl.LSN(0x2000),
		ReplicationSlot:   "test_slot",
	}
	socket.SetReplicationConn(conn)

	rawDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer rawDB.Close()
	db := sqlx.NewDb(rawDB, "sqlmock")

	// Cancel context while AcknowledgeLSN is polling
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err = AcknowledgeLSN(ctx, db, socket, false)
	require.Error(t, err)

	assert.ErrorIs(t, err, context.Canceled, "must return context.Canceled")
	assert.NotErrorIs(t, err, constants.ErrNonRetryable, "must NOT return ErrNonRetryable on context cancellation")
	assert.NotContains(t, err.Error(), "LSN not updated after 5 minutes", "must NOT report 5-minute timeout on context cancel")
	_ = mock
}
