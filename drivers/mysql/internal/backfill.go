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
	err := m.db.QueryRow(approxRowCountQuery, stream.Name()).Scan(&approxRowCount)
	if err != nil {
		return fmt.Errorf("failed to get approx row count: %s", err)
	}
	pool.AddRecordsToSync(approxRowCount)

	// Get chunks from state or calculate new ones
	stateChunks := m.State.GetChunks(stream.Self())
	var splitChunks []types.Chunk
	if stateChunks == nil || stateChunks.Len() == 0 {
		chunks := types.NewSet[types.Chunk]()
		if err := m.calculateChunks(stream, chunks); err != nil {
			return fmt.Errorf("failed to calculate chunks: %s", err)
		}
		splitChunks = chunks.Array()
		m.State.SetChunks(stream.Self(), types.NewSet(splitChunks...))
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
		// Begin transaction with repeatable read isolation
		tx, err := m.db.BeginTx(backfillCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// Get primary key column
		pkColumn := getPrimaryKeyColumn(m.db, stream.Name())
		if pkColumn == "" {
			return fmt.Errorf("no primary key found for stream %s", stream.ID())
		}

		// Build query for the chunk
		stmt := jdbc.ChunkDataQuery(stream, pkColumn)
		setter := jdbc.NewReader(backfillCtx, stmt, 0, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		}, chunk.Min, chunk.Max)

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

		// Capture and process rows
		return setter.Capture(func(rows *sql.Rows) error {
			columns, err := rows.Columns()
			if err != nil {
				return fmt.Errorf("failed to get columns: %w", err)
			}

			for rows.Next() {
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("failed to scan row: %w", err)
				}

				record := make(map[string]interface{})
				for i, col := range columns {
					if b, ok := values[i].([]byte); ok {
						record[col] = string(b)
						continue
					}
					record[col] = values[i]
				}

				// Calculate record hash using primary key
				recordHash := utils.GetKeysHash(record, pkColumn)
				// Create and insert record
				err = insert.Insert(types.CreateRawRecord(recordHash, record, time.Now().UnixNano()))
				if err != nil {
					return fmt.Errorf("failed to insert record: %w", err)
				}
			}

			return rows.Err()
		})
	}

	return utils.Concurrent(backfillCtx, splitChunks, m.config.MaxThreads, processChunk)
}

func (m *MySQL) calculateChunks(stream protocol.Stream, chunks *types.Set[types.Chunk]) error {
	return m.withIsolation(context.Background(), func(tx *sql.Tx) error {
		// Get primary key column using the provided function
		pkColumn := getPrimaryKeyColumn(m.db, stream.Name())
		if pkColumn == "" {
			return fmt.Errorf("no primary key found for stream %s", stream.ID())
		}

		// Get table extremes
		minVal, maxVal, err := m.getTableExtremes(stream, pkColumn, tx)
		if err != nil {
			return err
		}
		chunks.Insert(types.Chunk{
			Min: "",
			Max: convertToString(minVal),
		})

		logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), minVal, maxVal)

		// Calculate optimal chunk size based on table statistics
		chunkSize, err := m.calculateChunksSize(stream)
		if err != nil {
			return fmt.Errorf("failed to calculate chunk size: %w", err)
		}

		// Generate chunks based on range
		query := jdbc.NextChunkEndQuery(stream, pkColumn, chunkSize)

		var currentVal interface{} = minVal
		for {
			var nextValRaw interface{}
			err := tx.QueryRow(query, currentVal, chunkSize).Scan(&nextValRaw)
			if err != nil || nextValRaw == nil {
				// Add final chunk
				chunks.Insert(types.Chunk{
					Min: convertToString(currentVal),
					Max: convertToString(maxVal),
				})
				break
			}
			nextVal := convertToString(nextValRaw)
			chunks.Insert(types.Chunk{
				Min: convertToString(currentVal),
				Max: nextVal,
			})
			currentVal = nextVal
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
	return convertToString(min), convertToString(max), err
}

// Helper function to convert MySQL results to string
func convertToString(value interface{}) string {
	switch v := value.(type) {
	case []byte:
		return string(v) // Convert byte slice to string
	case string:
		return v // Already a string
	default:
		return fmt.Sprintf("%v", v) // Fallback
	}
}

func getPrimaryKeyColumn(db *sql.DB, table string) string {
	query := jdbc.MySQLPrimaryKeyQuery()
	var pkColumn string
	err := db.QueryRow(query, table).Scan(&pkColumn)
	if err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		logger.Errorf("Error getting primary key for table %s: %v", table, err)
		return ""
	}
	return pkColumn
}

func (m *MySQL) calculateChunksSize(stream protocol.Stream) (int, error) {
	var totalRecords int
	query := jdbc.MySQLTableRowsQuery()
	err := m.db.QueryRow(query, stream.Name()).Scan(&totalRecords)
	if err != nil {
		return 0, fmt.Errorf("failed to get estimated records count:%v", err)
	}
	return totalRecords / (m.config.MaxThreads * 8), nil
}

func (m *MySQL) withIsolation(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
