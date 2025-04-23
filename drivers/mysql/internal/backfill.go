package driver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// backfill implements full refresh sync mode for MySQL
func (m *MySQL) backfill(pool *protocol.WriterPool, stream protocol.Stream) error {
	// Use a context for the backfill operation
	backfillCtx := context.TODO()

	// Get approximate row count and inform the pool
	var approxRowCount int64
	approxRowCountQuery := jdbc.MySQLTableRowsQuery()
	err := m.client.QueryRow(approxRowCountQuery, stream.Name()).Scan(&approxRowCount)
	if err != nil {
		return fmt.Errorf("failed to get approx row count: %s", err)
	}
	pool.AddRecordsToSync(approxRowCount)
	// Get primary key column
	pkColumns := stream.GetStream().SourceDefinedPrimaryKey
	if pkColumns.Len() == 0 {
		return fmt.Errorf("no primary key defined for stream %s", stream.Name())
	}
	// currently only support single primary key chunking
	pkColumn := pkColumns.Array()[0]
	// Get chunks from state or calculate new ones
	stateChunks := m.State.GetChunks(stream.Self())
	var splitChunks []types.Chunk
	if stateChunks == nil || stateChunks.Len() == 0 {
		chunks := types.NewSet[types.Chunk]()
		if err := m.splitChunks(stream, chunks); err != nil {
			return fmt.Errorf("failed to calculate chunks: %s", err)
		}
		splitChunks = chunks.Array()
		m.State.SetChunks(stream.Self(), chunks)
	} else {
		splitChunks = stateChunks.Array()
	}

	// Sort chunks by Min value to ensure consistent processing order
	sort.Slice(splitChunks, func(i, j int) bool {
		return utils.CompareInterfaceValue(splitChunks[i].Min, splitChunks[j].Min) < 0
	})

	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(splitChunks))

	// Process chunks concurrently
	processChunk := func(ctx context.Context, chunk types.Chunk, number int) (err error) {
		// Track batch start time for logging
		batchStartTime := time.Now()
		waitChannel := make(chan error, 1)
		insert, err := pool.NewThread(backfillCtx, stream, protocol.WithErrorChannel(waitChannel))
		if err != nil {
			return fmt.Errorf("failed to create writer thread: %s", err)
		}
		defer func() {
			insert.Close()
			if err == nil {
				// Wait for chunk completion
				err = <-waitChannel
			}
			// Log completion and update state if successful
			if err == nil {
				logger.Infof("chunk[%d] with min[%v]-max[%v] completed in %0.2f seconds", number, chunk.Min, chunk.Max, time.Since(batchStartTime).Seconds())
				m.State.RemoveChunk(stream.Self(), chunk)
			}
		}()
		// Begin transaction with repeatable read isolation
		return jdbc.WithIsolation(backfillCtx, m.client, func(tx *sql.Tx) error {
			// Build query for the chunk
			stmt := jdbc.MysqlChunkScanQuery(stream, pkColumn, chunk)
			setter := jdbc.NewReader(backfillCtx, stmt, 0, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				return tx.QueryContext(ctx, query, args...)
			})
			// Capture and process rows
			return setter.Capture(func(rows *sql.Rows) error {
				//crrate a map to hold column names and values
				record := make(types.Record)
				//scan the row into map
				err := jdbc.MapScan(rows, record, nil)
				if err != nil {
					return fmt.Errorf("failed to mapScan record data: %s", err)
				}
				// TODO : create hash from all keys if primary key not present
				//genrate olake id
				olakeID := utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
				//insert record
				err = insert.Insert(types.CreateRawRecord(olakeID, record, "r", time.Unix(0, 0)))
				if err != nil {
					return err
				}
				return nil
			})
		})
	}

	return utils.Concurrent(backfillCtx, splitChunks, m.config.MaxThreads, processChunk)
}

func (m *MySQL) splitChunks(stream protocol.Stream, chunks *types.Set[types.Chunk]) error {
	return jdbc.WithIsolation(context.Background(), m.client, func(tx *sql.Tx) error {
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
