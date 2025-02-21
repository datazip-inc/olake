package waljs

import (
	"context"

	"github.com/datazip-inc/olake/protocol"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

type Snapshotter struct {
	tx     pgx.Tx
	stream protocol.Stream
}

func NewSnapshotter(stream protocol.Stream, batchSize int) *Snapshotter {
	return &Snapshotter{
		stream: stream,
	}
}

func (s *Snapshotter) Prepare(conn *pgx.Conn) error {
	tx, err := conn.BeginTx(context.TODO(), pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})

	s.tx = tx

	return err
}

func (s *Snapshotter) ReleaseSnapshot() error {
	return s.tx.Commit(context.Background())
}

func (s *Snapshotter) CloseConn() error {
	if s.tx != nil {
		return s.tx.Conn().Close(context.Background())
	}

	return nil
}
