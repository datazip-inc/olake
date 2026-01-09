package driver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// ChunkIterator implements snapshot iteration over MSSQL chunks.
func (m *MSSQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, onMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.MSSQL,
		Stream: stream,
		State:  m.state,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, m.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during MSSQL chunk iteration: %s", err)
	}

	// SQL Server doesn't support read-only transactions
	// Use repeatable read isolation without read-only flag
	return jdbc.WithIsolation(ctx, m.client, false, func(tx *sql.Tx) error {
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		chunkColumn := stream.Self().StreamMetadata.ChunkColumn

		logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)

		// Build query for the chunk
		stmt := ""
		if chunkColumn != "" {
			stmt = jdbc.MSSQLChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt = jdbc.MSSQLChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			stmt = jdbc.MSSQLPhysLocChunkScanQuery(stream, chunk, filter)
		}

		logger.Debugf("Executing chunk query: %s", stmt)

		reader := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})

		return reader.Capture(func(rows *sql.Rows) error {
			record := make(types.Record)
			if err := jdbc.MapScan(rows, record, m.dataTypeConverter); err != nil {
				return fmt.Errorf("failed to scan record data as map: %s", err)
			}
			return onMessage(ctx, record)
		})
	})
}

// GetOrSplitChunks splits a table into chunks using PK seek or %%physloc%% fallback.
func (m *MSSQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var (
		approxRowCount int64
		avgRowSize     any
	)

	rowStatsQuery := jdbc.MSSQLTableRowStatsQuery()
	err := m.client.QueryRowContext(ctx, rowStatsQuery, stream.Namespace(), stream.Name()).Scan(&approxRowCount, &avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count and avg row size: %s", err)
	}

	if approxRowCount == 0 {
		logger.Warnf("Table %s is empty, skipping chunking", stream.ID())
		return types.NewSet[types.Chunk](), nil
	}

	pool.AddRecordsToSyncStats(approxRowCount)

	// avgRowSize is returned as []uint8 which is converted to float64
	avgRowSizeFloat, err := typeutils.ReformatFloat64(avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get avg row size: %s", err)
	}
	chunkSize := int64(math.Ceil(float64(constants.EffectiveParquetSize) / avgRowSizeFloat))
	chunks := types.NewSet[types.Chunk]()
	chunkColumn := stream.Self().StreamMetadata.ChunkColumn
	pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()

	// Use chunkColumn if provided, otherwise use PK columns
	if chunkColumn != "" {
		pkColumns = []string{chunkColumn}
		logger.Debugf("Stream %s: Using chunkColumn=%s for chunking", stream.ID(), chunkColumn)
	} else if len(pkColumns) > 0 {
		logger.Debugf("Stream %s: Using PK columns=%v for chunking", stream.ID(), pkColumns)
	} else {
		logger.Debugf("Stream %s: No PK or chunkColumn, will use %%physloc%% chunking", stream.ID())
	}

	// Split via primary key when available
	splitViaPrimaryKey := func(stream types.StreamInterface, chunks *types.Set[types.Chunk], pkCols []string) error {
		return jdbc.WithIsolation(ctx, m.client, false, func(tx *sql.Tx) error {
			sort.Strings(pkCols)

			if len(pkCols) == 0 {
				return nil
			}

			// Get the minimum and maximum values for the primary key columns
			minVal, maxVal, err := m.getTableExtremesMSSQL(ctx, stream, pkCols, tx)
			if err != nil {
				return fmt.Errorf("failed to get table extremes: %s", err)
			}
			// Skip if table is empty
			if minVal == nil {
				return nil
			}

			// Create the first chunk from the beginning up to the minimum value
			chunks.Insert(types.Chunk{
				Min: nil,
				Max: utils.ConvertToString(minVal),
			})

			logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

			// Build query to find the next chunk boundary
			query := jdbc.MSSQLNextChunkEndQuery(stream, pkCols, chunkSize)
			currentVal := minVal

			for {
				// Split the current composite key value into individual column parts
				columns := strings.Split(utils.ConvertToString(currentVal), ",")

				// Build query arguments for composite key comparison
				args := make([]interface{}, 0)
				for colIdx := 0; colIdx < len(pkCols); colIdx++ {
					for partIdx := 0; partIdx <= colIdx && partIdx < len(columns); partIdx++ {
						args = append(args, strings.TrimSpace(columns[partIdx]))
					}
				}

				// Query for the next chunk boundary value
				var nextValRaw interface{}
				err := tx.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
				// Stop if we've reached the end of the table
				if err == sql.ErrNoRows || nextValRaw == nil {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to get next chunk end: %s", err)
				}

				// Create a chunk between current and next boundary
				if currentVal != nil {
					chunks.Insert(types.Chunk{
						Min: utils.ConvertToString(currentVal),
						Max: utils.ConvertToString(nextValRaw),
					})
				}

				currentVal = nextValRaw
			}

			// Create the final chunk from the last value to the end
			if currentVal != nil {
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(currentVal),
					Max: nil,
				})
			}

			return nil
		})
	}

	// Split using physical location when no primary key is available.
	// %%physloc%% returns the physical location (file_id, page_id, slot_id) of a row as binary.
	// We iteratively find chunk boundaries by querying for the N-th row (N = chunkSize) where
	// physloc > current, creating evenly-sized chunks: [nil, min], [min, next1], ..., [last, nil]
	splitViaPhysLoc := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		// SQL Server doesn't support read-only transactions
		// Use repeatable read isolation without read-only flag
		return jdbc.WithIsolation(ctx, m.client, false, func(tx *sql.Tx) error {
			// Get the minimum and maximum physical location values
			// These define the boundaries of our table for chunking
			minVal, maxVal, err := m.getPhysLocExtremes(ctx, stream, tx)
			if err != nil {
				return fmt.Errorf("failed to get %%physloc%% extremes: %s", err)
			}
			// Skip if table is empty (no rows to chunk)
			if minVal == nil || maxVal == nil {
				return nil
			}

			// Start from the minimum physloc value
			current := minVal
			chunks.Insert(types.Chunk{
				Min: nil,
				Max: utils.ConvertToString(current),
			})

			// Iteratively find chunk boundaries until we reach the end of the table
			for {
				var next any
				// This gives us the next chunk boundary, ensuring each chunk has ~chunkSize rows
				query := jdbc.MSSQLPhysLocNextChunkEndQuery(stream, chunkSize)

				err := tx.QueryRowContext(ctx, query, current).Scan(&next)
				// End of table reached: no more rows with physloc > current
				if err == sql.ErrNoRows || next == nil {
					chunks.Insert(types.Chunk{Min: utils.ConvertToString(current), Max: nil})
					break
				}
				if err != nil {
					return fmt.Errorf("failed to get next %%physloc%% chunk end: %s", err)
				}

				// Safety check: Compare binary values to detect if we've reached the end
				// This handles edge cases where the query might return the same value
				//
				// Note: physloc values are []byte (binary), so we use bytes.Equal for comparison
				if currentBytes, ok := current.([]byte); ok {
					if nextBytes, ok2 := next.([]byte); ok2 {
						if bytes.Equal(currentBytes, nextBytes) {
							// Reached maximum value, create final chunk
							chunks.Insert(types.Chunk{Min: utils.ConvertToString(current), Max: nil})
							break
						}
					}
				}

				// Create a chunk between current and next boundary
				// This chunk will contain approximately chunkSize rows
				// Example: If current = A and next = D, chunk [A, D) contains rows A, B, C
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(current),
					Max: utils.ConvertToString(next),
				})
				// Move to the next boundary for the next iteration
				current = next
			}

			return nil
		})
	}

	if len(pkColumns) > 0 {
		logger.Debugf("Stream %s: Using PK-based chunking with columns: %v", stream.ID(), pkColumns)
		err = splitViaPrimaryKey(stream, chunks, pkColumns)
	} else {
		logger.Debugf("Stream %s: Using %%physloc%% chunking (no PK or chunkColumn available)", stream.ID())
		err = splitViaPhysLoc(stream, chunks)
	}

	return chunks, err
}

// getTableExtremes returns MIN and MAX key values for the given PK columns
func (m *MSSQL) getTableExtremesMSSQL(ctx context.Context, stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMSSQL(stream, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// getPhysLocExtremes returns MIN and MAX %%physloc%% values for the table.
func (m *MSSQL) getPhysLocExtremes(ctx context.Context, stream types.StreamInterface, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MSSQLPhysLocExtremesQuery(stream)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}
