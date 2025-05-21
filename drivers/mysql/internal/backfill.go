package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// backfill implements full refresh sync mode for MySQL
// func (m *MySQL) Backfill(backfillCtx context.Context, backfilledStreams chan string, pool *protocol.WriterPool, stream protocol.Stream) error {
// 	// check for primary key if backfill is suported or not
// 	if stream.GetStream().AvailableCursorFields.Len() > 1 {
// 		return fmt.Errorf("backfill not supported, more then one primary key found in stream[%s]", stream.ID())
// 	}

// 	// Process chunks concurrently

// 	return m.Driver.Backfill(backfillCtx, backfilledStreams, pool, stream)
// }

func (m *MySQL) ChunkIterator(ctx context.Context, stream protocol.Stream, chunk types.Chunk, OnMessage protocol.BackfillMsgFn) (err error) {
	// Begin transaction with repeatable read isolation
	return jdbc.WithIsolation(ctx, m.client, func(tx *sql.Tx) error {
		// Build query for the chunk
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey
		pkColumn := pkColumns.Array()[0]
		// Get chunks from state or calculate new ones
		stmt := jdbc.MysqlChunkScanQuery(stream, pkColumn, chunk)
		setter := jdbc.NewReader(ctx, stmt, 0, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})
		// Capture and process rows
		return setter.Capture(func(rows *sql.Rows) error {
			record := make(types.Record)
			err := jdbc.MapScan(rows, record, nil)
			if err != nil {
				return fmt.Errorf("failed to mapScan record data: %s", err)
			}
			return OnMessage(record)
		})
	})
}

func (m *MySQL) GetOrSplitChunks(ctx context.Context, pool *protocol.WriterPool, stream protocol.Stream) ([]types.Chunk, error) {
	var approxRowCount int64
	approxRowCountQuery := jdbc.MySQLTableRowsQuery()
	err := m.client.QueryRow(approxRowCountQuery, stream.Name()).Scan(&approxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
	}
	pool.AddRecordsToSync(approxRowCount)
	// Get primary key column

	stateChunks := m.State.GetChunks(stream.Self())
	var splitChunks []types.Chunk
	if stateChunks == nil || stateChunks.Len() == 0 {
		chunks := types.NewSet[types.Chunk]()
		if err := m.splitChunks(ctx, stream, chunks); err != nil {
			return nil, fmt.Errorf("failed to calculate chunks: %s", err)
		}
		splitChunks = chunks.Array()
		m.State.SetChunks(stream.Self(), chunks)
	} else {
		splitChunks = stateChunks.Array()
	}
	return splitChunks, nil
}

func (m *MySQL) splitChunks(ctx context.Context, stream protocol.Stream, chunks *types.Set[types.Chunk]) error {
	return jdbc.WithIsolation(ctx, m.client, func(tx *sql.Tx) error {
		// Get primary key column using the provided function
		pkColumn := stream.GetStream().SourceDefinedPrimaryKey.Array()[0]
		// Get table extremes
		minVal, maxVal, err := m.getTableExtremes(stream, pkColumn, tx)
		if err != nil {
			return err
		}
		if minVal == nil {
			return nil
		}
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(minVal),
		})

		logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

		// Calculate optimal chunk size based on table statistics
		chunkSize, err := m.calculateChunkSize(stream)
		if err != nil {
			return fmt.Errorf("failed to calculate chunk size: %w", err)
		}

		// Generate chunks based on range
		query := jdbc.NextChunkEndQuery(stream, pkColumn, chunkSize)

		currentVal := minVal
		for {
			var nextValRaw interface{}
			err := tx.QueryRow(query, currentVal, chunkSize).Scan(&nextValRaw)
			if err != nil && err == sql.ErrNoRows || nextValRaw == nil {
				break
			} else if err != nil {
				return fmt.Errorf("failed to get next chunk end: %w", err)
			}
			if currentVal != nil && nextValRaw == nil {
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(currentVal),
					Max: utils.ConvertToString(nextValRaw),
				})
			}
			currentVal = nextValRaw
		}
		if currentVal != nil {
			chunks.Insert(types.Chunk{
				Min: utils.ConvertToString(currentVal),
				Max: nil,
			})
		}

		return nil
	})
}

func (m *MySQL) getTableExtremes(stream protocol.Stream, pkColumn string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQuery(stream, pkColumn)
	err = tx.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return "", "", err
	}
	return min, max, err
}
func (m *MySQL) calculateChunkSize(stream protocol.Stream) (int, error) {
	var totalRecords int
	query := jdbc.MySQLTableRowsQuery()
	err := m.client.QueryRow(query, stream.Name()).Scan(&totalRecords)
	if err != nil {
		return 0, fmt.Errorf("failed to get estimated records count:%v", err)
	}
	// number of chunks based on max threads
	return totalRecords / (m.config.MaxThreads * 8), nil
}
