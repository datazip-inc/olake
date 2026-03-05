package driver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

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
		sort.Strings(pkColumns)
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

		setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})

		return jdbc.MapScanConcurrent(setter, m.dataTypeConverter, onMessage)
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
		var hasRows bool
		existsQuery := jdbc.MSSQLTableExistsQuery(stream)
		err := m.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)
		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run UPDATE STATISTICS to update table statistics", stream.ID())
		}

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
			minVal, maxVal, err := m.getTableExtremes(ctx, stream, pkCols, tx)
			if err != nil {
				return fmt.Errorf("failed to get table extremes: %s", err)
			}
			// Skip if table is empty
			if minVal == nil {
				return nil
			}

			columnType := ""
			if len(pkCols) == 1 {
				columnType, err = m.getColumnTypeMSSQL(ctx, stream, pkCols[0], tx)
				if err != nil {
					return fmt.Errorf("failed to get table column type: %s", err)
				}
			}

			// Create the first chunk from the beginning up to the minimum value
			chunks.Insert(types.Chunk{
				Min: nil,
				Max: normalizeBoundaryValue(minVal, pkCols, columnType),
			})

			logger.Infof(
				"Stream %s extremes - min: %v, max: %v", stream.ID(),
				normalizeBoundaryValue(minVal, pkCols, columnType),
				normalizeBoundaryValue(maxVal, pkCols, columnType),
			)

			// Build query to find the next chunk boundary
			query := jdbc.MSSQLNextChunkEndQuery(stream, pkCols, chunkSize)
			currentVal := minVal

			for {
				// Split the current composite key value into individual column parts
				columns := strings.Split(normalizeBoundaryValue(currentVal, pkCols, columnType), ",")

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
						Min: normalizeBoundaryValue(currentVal, pkCols, columnType),
						Max: normalizeBoundaryValue(nextValRaw, pkCols, columnType),
					})
				}

				currentVal = nextValRaw
			}

			// Create the final chunk from the last value to the end
			if currentVal != nil {
				chunks.Insert(types.Chunk{
					Min: normalizeBoundaryValue(currentVal, pkCols, columnType),
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
	//
	// All physloc values are hex-encoded before storing in chunks to ensure valid UTF-8 chunk values.
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
				Max: utils.HexEncode(minVal),
			})

			// Iteratively find chunk boundaries until we reach the end of the table
			for {
				var next []byte
				// This gives us the next chunk boundary, ensuring each chunk has ~chunkSize rows
				query := jdbc.MSSQLPhysLocNextChunkEndQuery(stream, chunkSize)

				err := tx.QueryRowContext(ctx, query, current).Scan(&next)
				// End of table reached: no more rows with physloc > current
				if err == sql.ErrNoRows || next == nil {
					chunks.Insert(types.Chunk{Min: utils.HexEncode(current), Max: nil})
					break
				}
				if err != nil {
					return fmt.Errorf("failed to get next %%physloc%% chunk end: %s", err)
				}

				if bytes.Equal(current, next) {
					chunks.Insert(types.Chunk{Min: utils.HexEncode(current), Max: nil})
					break
				}

				// Create a chunk between current and next boundary
				// This chunk will contain approximately chunkSize rows
				// Example: If current = A and next = D, chunk [A, D) contains rows A, B, C
				chunks.Insert(types.Chunk{
					Min: utils.HexEncode(current),
					Max: utils.HexEncode(next),
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

// getColumnTypeMSSQL returns SQL data type for the requested column.
func (m *MSSQL) getColumnTypeMSSQL(ctx context.Context, stream types.StreamInterface, column string, tx *sql.Tx) (string, error) {
	var dataType string
	err := tx.QueryRowContext(ctx, jdbc.MSSQLColumnTypeQuery(), stream.Namespace(), stream.Name(), column).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return dataType, nil
}

// normalizeBoundaryValue converts key boundary values into a stable SQL-safe string form
// before storing them in chunk state and reusing them as parameters for next-boundary queries.
func normalizeBoundaryValue(value any, pkCols []string, columnType string) string {
	// Typed normalization is only for single-key chunking.
	// Composite keys are already materialized as a single CONCAT string.
	if len(pkCols) != 1 {
		return utils.ConvertToString(value)
	}

	columnType = strings.ToLower(columnType)

	switch v := value.(type) {
	case time.Time:
		// SQL Server datetime types are timezone-naive, but Go scans them as time.Time with location.
		// We normalize to SQL Server-compatible literal formats so chunk boundaries round-trip
		// safely through state serialization and remain stable for subsequent boundary comparisons.
		switch columnType {
		case "date":
			return v.Format("2006-01-02")
		case "time":
			return v.Format("15:04:05.9999999")
		case "datetime", "datetime2", "smalldatetime":
			return v.Format("2006-01-02 15:04:05.9999999")
		}
	case []byte:
		switch columnType {
		case "uniqueidentifier":
			if uuid, converted := formatUniqueIdentifierBytes(v); converted {
				return uuid
			}
		case "numeric", "decimal", "money", "smallmoney":
			return string(v)
		default:
			// For non-UUID byte values, encode as hex string to avoid corruption.
			return utils.HexEncode(v)
		}
	}
	return utils.ConvertToString(value)
}

// getTableExtremes returns MIN and MAX key values for the given PK columns
func (m *MSSQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMSSQL(stream, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// getPhysLocExtremes returns MIN and MAX %%physloc%% values for the table.
func (m *MSSQL) getPhysLocExtremes(ctx context.Context, stream types.StreamInterface, tx *sql.Tx) (min, max []byte, err error) {
	query := jdbc.MSSQLPhysLocExtremesQuery(stream)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// formatUniqueIdentifierBytes converts SQL Server's mixed-endian UNIQUEIDENTIFIER
// byte layout to canonical RFC4122 UUID string representation.
func formatUniqueIdentifierBytes(v []byte) (string, bool) {
	if len(v) != 16 {
		return "", false
	}

	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		v[3], v[2], v[1], v[0], // first 4 bytes (little-endian)
		v[5], v[4], // next 2 bytes (little-endian)
		v[7], v[6], // next 2 bytes (little-endian)
		v[8], v[9], // next 2 bytes (big-endian)
		v[10], v[11], v[12], v[13], v[14], v[15], // last 6 bytes (big-endian)
	), true
}
