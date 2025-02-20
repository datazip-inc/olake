package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// Simple Full Refresh Sync; Loads table fully
func (p *Postgres) backfill(pool *protocol.WriterPool, stream protocol.Stream) error {
	backfillCtx := context.TODO()
	tx, err := p.client.BeginTx(backfillCtx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return err
	}

	defer tx.Rollback()

	stmt := jdbc.PostgresFullRefresh(stream)

	setter := jdbc.NewReader(context.TODO(), stmt, p.config.BatchSize, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return tx.Query(query, args...)
	})

	insert, err := pool.NewThread(backfillCtx, stream)
	if err != nil {
		return err
	}
	defer insert.Close()

	return setter.Capture(func(rows *sql.Rows) error {
		// Create a map to hold column names and values
		record := make(types.Record)

		// Scan the row into the map
		err := utils.MapScan(rows, record)
		if err != nil {
			return fmt.Errorf("failed to mapScan record data: %s", err)
		}

		// generate olake id
		olakeID := utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
		// insert record
		exit, err := insert.Insert(types.CreateRawRecord(olakeID, record, 0))
		if err != nil {
			return err
		}
		if exit {
			return nil
		}

		return nil
	})
}

// Incremental Sync based on a Cursor Value
func (p *Postgres) incrementalSync(pool *protocol.WriterPool, stream protocol.Stream) error {
	incrementalCtx := context.TODO()
	tx, err := p.client.BeginTx(incrementalCtx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return err
	}

	defer tx.Rollback()

	args := []any{}
	statement := jdbc.PostgresWithoutState(stream)

	// intialState := stream.InitialState()
	// if intialState != nil {
	// 	logger.Debugf("Using Initial state for stream %s : %v", stream.ID(), intialState)
	// 	statement = jdbc.PostgresWithState(stream)
	// 	args = append(args, intialState)
	// }
	insert, err := pool.NewThread(incrementalCtx, stream)
	if err != nil {
		return err
	}
	defer insert.Close()
	setter := jdbc.NewReader(context.Background(), statement, p.config.BatchSize, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return tx.Query(query, args...)
	}, args...)
	return setter.Capture(func(rows *sql.Rows) error {
		// Create a map to hold column names and values
		record := make(types.Record)

		// Scan the row into the map
		err := utils.MapScan(rows, record)
		if err != nil {
			return fmt.Errorf("failed to mapScan record data: %s", err)
		}

		// create olakeID
		olakeID := utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...)

		// insert record
		exit, err := insert.Insert(types.CreateRawRecord(olakeID, record, 0))
		if err != nil {
			return err
		}
		if exit {
			return nil
		}
		// TODO: Postgres State Management
		// err = p.UpdateState(stream, record)
		// if err != nil {
		// 	return err
		// }

		return nil
	})
}
