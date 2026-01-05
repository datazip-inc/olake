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

	// Split using physical location when no primary key is available
	splitViaPhysLoc := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		// SQL Server doesn't support read-only transactions
		return jdbc.WithIsolation(ctx, m.client, false, func(tx *sql.Tx) error {
			// Get the minimum and maximum physical location values
			minVal, maxVal, err := m.getPhysLocExtremes(ctx, stream, tx)
			if err != nil {
				return fmt.Errorf("failed to get %%physloc%% extremes: %s", err)
			}
			// Skip if table is empty
			if minVal == nil || maxVal == nil {
				return nil
			}

			// Split the table using physical location boundaries
			if err := m.splitViaPhysLocNextQuery(ctx, tx, stream, minVal, chunks, chunkSize); err != nil {
				return fmt.Errorf("failed to split via %%physloc%%: %s", err)
			}
			return nil
		})
	}

	if len(pkColumns) > 0 {
		logger.Debugf("Stream %s: Using PK-based chunking with columns: %v", stream.ID(), pkColumns)
		if err := splitViaPrimaryKey(stream, chunks, pkColumns); err != nil {
			return nil, fmt.Errorf("failed to split chunks via primary key for stream %s: %w", stream.ID(), err)
		}
	} else {
		logger.Debugf("Stream %s: Using %%physloc%% chunking (no PK or chunkColumn available)", stream.ID())
		if err := splitViaPhysLoc(stream, chunks); err != nil {
			return nil, fmt.Errorf("failed to split chunks via %%physloc%% for stream %s: %w", stream.ID(), err)
		}
	}
	return chunks, nil
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

// splitViaPhysLocNextQuery uses %%physloc%% to find next chunk boundaries.
func (m *MSSQL) splitViaPhysLocNextQuery(ctx context.Context, tx *sql.Tx, stream types.StreamInterface, min interface{}, chunks *types.Set[types.Chunk], chunkSize int64) error {
	current := min
	chunks.Insert(types.Chunk{
		Min: nil,
		Max: utils.ConvertToString(current),
	})

	for {
		var next any
		query := jdbc.MSSQLPhysLocNextChunkEndQuery(stream, chunkSize)

		err := tx.QueryRowContext(ctx, query, current).Scan(&next)
		if err == sql.ErrNoRows || next == nil {
			chunks.Insert(types.Chunk{Min: utils.ConvertToString(current), Max: nil})
			break
		}
		if err != nil {
			return fmt.Errorf("failed to get next %%physloc%% chunk end: %s", err)
		}

		// Compare binary values
		if currentBytes, ok := current.([]byte); ok {
			if nextBytes, ok2 := next.([]byte); ok2 {
				if bytes.Equal(currentBytes, nextBytes) {
					chunks.Insert(types.Chunk{Min: utils.ConvertToString(current), Max: nil})
					break
				}
			}
		}

		chunks.Insert(types.Chunk{
			Min: utils.ConvertToString(current),
			Max: utils.ConvertToString(next),
		})
		current = next
	}
	return nil
}
