package driver

import (
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

		stmt, err := jdbc.MSSQLChunkScanQuery(stream, chunk, filter, chunkColumn, pkColumns)
		if err != nil {
			return err
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

// GetOrSplitChunks splits a table into chunks using PK seek or ROW_NUMBER() fallback.
func (m *MSSQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	var avgRowSize any
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

	// Split via primary key when available
	splitViaPrimaryKey := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		// SQL Server doesn't support read-only transactions
		return jdbc.WithIsolation(ctx, m.client, false, func(tx *sql.Tx) error {
			pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
			if chunkColumn != "" {
				pkColumns = []string{chunkColumn}
			}

			// Normalise column order for stability (important for composite keys).
			sort.Strings(pkColumns)

			// Composite primary key / composite chunk column support.
			if len(pkColumns) > 1 && chunkColumn == "" {
				minVal, maxVal, err := m.getCompositeTableExtremes(ctx, stream, pkColumns, tx)
				if err != nil {
					return fmt.Errorf("failed to get MSSQL composite table extremes: %s", err)
				}
				if minVal == nil {
					return nil
				}

				// First chunk: (-inf, minVal]
				chunks.Insert(types.Chunk{
					Min: nil,
					Max: utils.ConvertToString(minVal),
				})

				logger.Infof("Stream %s MSSQL composite extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

				query := jdbc.MSSQLCompositeNextChunkEndQuery(stream, pkColumns, chunkSize)
				currentVal := minVal

				for {
					// Split the current composite value into individual column parts.
					columns := strings.Split(utils.ConvertToString(currentVal), ",")

					// Build arguments for lexicographic > comparison, same pattern as MySQL.
					args := make([]interface{}, 0)
					for colIdx := 0; colIdx < len(pkColumns); colIdx++ {
						for partIdx := 0; partIdx <= colIdx && partIdx < len(columns); partIdx++ {
							args = append(args, strings.TrimSpace(columns[partIdx]))
						}
					}

					var nextValRaw interface{}
					if err := tx.QueryRowContext(ctx, query, args...).Scan(&nextValRaw); err != nil {
						if err == sql.ErrNoRows {
							break
						}
						return fmt.Errorf("failed to get MSSQL next composite chunk end: %s", err)
					}
					if nextValRaw == nil {
						break
					}

					if currentVal != nil {
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
			}

			// Single-column PK or explicit chunk column.
			if len(pkColumns) == 0 {
				return nil
			}

			pk := pkColumns[0]
			minVal, maxVal, err := m.getTableExtremes(ctx, stream, pk, tx)
			if err != nil {
				return fmt.Errorf("failed to get table extremes: %s", err)
			}
			if minVal == nil || maxVal == nil {
				return nil
			}

			// evenly distribute by numeric difference when possible
			pkType, _ := stream.Schema().GetType(pk)
			if pkType == types.Int64 || pkType == types.Int32 {
				splits, err := m.splitViaBatchSize(minVal, maxVal, int(chunkSize))
				if err != nil {
					return fmt.Errorf("failed to split MSSQL batch-size chunks: %s", err)
				}
				for _, c := range splits.Array() {
					chunks.Insert(c)
				}
				return nil
			}

			// Non-numeric PK: approximate using next-boundary query pattern.
			if err := m.splitViaNextQuery(ctx, tx, stream, pk, minVal, chunks, chunkSize); err != nil {
				return fmt.Errorf("failed to split via next query: %s", err)
			}
			return nil
		})
	}

	// Row number based splitting when no PK is available
	splitViaRowNumber := func(chunks *types.Set[types.Chunk]) error {
		// row_number-based splitting: 1..N with fixed chunk size.
		var start int64
		for start = 0; start < approxRowCount; start += chunkSize {
			end := start + chunkSize
			if end >= approxRowCount {
				chunks.Insert(types.Chunk{
					Min: start,
					Max: nil,
				})
				break
			}
			chunks.Insert(types.Chunk{
				Min: start,
				Max: end,
			})
		}
		return nil
	}

	if stream.GetStream().SourceDefinedPrimaryKey.Len() > 0 || chunkColumn != "" {
		if err := splitViaPrimaryKey(stream, chunks); err != nil {
			return nil, err
		}
	} else {
		if err := splitViaRowNumber(chunks); err != nil {
			return nil, err
		}
	}
	return chunks, nil
}

func (m *MSSQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, column string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMSSQL(stream, column)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// getCompositeTableExtremes returns MIN and MAX composite key values for the given PK columns.
// The values are returned as a single concatenated string in the same format used by
// MSSQLCompositeNextChunkEndQuery, enabling consistent lexicographic chunking.
func (m *MSSQL) getCompositeTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMSSQLComposite(stream, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// splitViaBatchSize evenly splits numeric PK space into ranges.
func (m *MSSQL) splitViaBatchSize(min, max interface{}, dynamicChunkSize int) (*types.Set[types.Chunk], error) {
	splits := types.NewSet[types.Chunk]()
	chunkStart := min
	chunkEnd, err := utils.AddConstantToInterface(min, dynamicChunkSize)
	if err != nil {
		return nil, fmt.Errorf("failed to split MSSQL batch-size chunks: %s", err)
	}

	for typeutils.Compare(chunkEnd, max) <= 0 {
		splits.Insert(types.Chunk{Min: chunkStart, Max: chunkEnd})
		chunkStart = chunkEnd
		newChunkEnd, err := utils.AddConstantToInterface(chunkEnd, dynamicChunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to split MSSQL batch-size chunks: %s", err)
		}
		chunkEnd = newChunkEnd
	}
	splits.Insert(types.Chunk{Min: chunkStart, Max: nil})
	return splits, nil
}

// splitViaNextQuery uses a paginated NEXT VALUE query when PK is non-numeric.
func (m *MSSQL) splitViaNextQuery(ctx context.Context, tx *sql.Tx, stream types.StreamInterface, pk string, min interface{}, chunks *types.Set[types.Chunk], chunkSize int64) error {
	current := min
	for {
		var next any
		query := jdbc.MSSQLNextChunkEndQuery(stream, pk, chunkSize)

		err := tx.QueryRowContext(ctx, query, current).Scan(&next)
		if err == sql.ErrNoRows || next == nil {
			chunks.Insert(types.Chunk{Min: current, Max: nil})
			break
		}
		if typeutils.Compare(next, current) == 0 {
			chunks.Insert(types.Chunk{Min: current, Max: nil})
			break
		}

		chunks.Insert(types.Chunk{Min: current, Max: next})
		current = next
	}
	return nil
}
