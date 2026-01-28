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

func (m *MySQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.MySQL,
		Stream: stream,
		State:  m.state,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, m.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}
	// Begin transaction with repeatable read isolation
	return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
		// Build query for the chunk
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		chunkColumn := stream.Self().StreamMetadata.ChunkColumn
		sort.Strings(pkColumns)

		logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)
		// Get cxhunks from state or calculate new ones
		var stmt string

		// FULL TABLE SCAN (prevents `WHERE ` SQL syntax error)
		if chunk.Min == nil && chunk.Max == nil && filter == "" {
			stmt = fmt.Sprintf(
				"SELECT * FROM `%s`.`%s`",
				stream.Namespace(),
				stream.Name(),
			)
		} else {
			if chunkColumn != "" {
				stmt = jdbc.MysqlChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
			} else if len(pkColumns) > 0 {
				stmt = jdbc.MysqlChunkScanQuery(stream, pkColumns, chunk, filter)
			} else {
				stmt = jdbc.MysqlLimitOffsetScanQuery(stream, chunk, filter)
			}
		}

		logger.Debugf("Executing chunk query: %s", stmt)
		setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})
		// Capture and process rows
		return setter.Capture(func(rows *sql.Rows) error {
			record := make(types.Record)
			err := jdbc.MapScan(rows, record, m.dataTypeConverter)
			if err != nil {
				return fmt.Errorf("failed to scan record data as map: %s", err)
			}
			return OnMessage(ctx, record)
		})
	})
}

func (m *MySQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	var avgRowSize any
	approxRowCountQuery := jdbc.MySQLTableRowStatsQuery()
	err := m.client.QueryRowContext(ctx, approxRowCountQuery, stream.Name()).Scan(&approxRowCount, &avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count and avg row size: %s", err)
	}

	if approxRowCount == 0 {
		var hasRows bool
		existsQuery := jdbc.MySQLTableExistsQuery(stream)
		err := m.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)

		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run ANALYZE TABLE to update table statistics", stream.ID())
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
	if chunkColumn != "" {
		pkColumns = []string{chunkColumn}
	}
	sort.Strings(pkColumns)

	// only meaningful for single-column PK
	var (
		ok   bool
		step int64
		minVal any
		maxVal any
	)

	if stream.GetStream().SourceDefinedPrimaryKey.Len() > 0 || chunkColumn != "" {
		err = jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {

			var err error
			minVal, maxVal, err = m.getTableExtremes(ctx, stream, pkColumns, tx)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get table extremes: %s", err)
		}
	}

	if len(pkColumns) == 1 {
		ok, step = shouldUseEvenDistribution(minVal, maxVal, approxRowCount, chunkSize)
	}

	//  EVEN distribution
	if len(pkColumns) == 1 && ok {
		logger.Infof("Splitting evenly for stream %s using step %d (min: %v, max: %v)", stream.ID(), step, minVal, maxVal)
		splitEvenlyForInt(minVal, maxVal, chunks, step)
		if err != nil {
			return nil, fmt.Errorf("failed to split evenly: %s", err)
		}

	} else if len(pkColumns) > 0 {
		logger.Infof("Splitting via PK for stream %s", stream.ID())
		err = splitViaPrimaryKey(
			ctx,
			m,
			stream,
			chunks,
			chunkColumn,
			chunkSize,
			minVal,
			maxVal,
			pkColumns,
		)

		//LIMIT / OFFSET fallback (no PK / no chunk column)
	} else {
		logger.Infof("Using Limit/Offset chunking for stream %s", stream.ID())
		err = limitOffsetChunking(
			ctx,
			m,
			chunks,
			chunkSize,
			approxRowCount,
		)
	}
	logger.Infof("Generated %d chunks for stream %s", chunks.Len(), stream.ID())
	return chunks, err
}

func (m *MySQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMySQL(stream, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

func limitOffsetChunking(ctx context.Context, m *MySQL, chunks *types.Set[types.Chunk], chunkSize int64, approxRowCount int64) error {
	return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(chunkSize),
		})
		lastChunk := chunkSize
		for lastChunk < approxRowCount {
			chunks.Insert(types.Chunk{
				Min: utils.ConvertToString(lastChunk),
				Max: utils.ConvertToString(lastChunk + chunkSize),
			})
			lastChunk += chunkSize
		}
		chunks.Insert(types.Chunk{
			Min: utils.ConvertToString(lastChunk),
			Max: nil,
		})
		return nil
	})
}

func splitViaPrimaryKey(ctx context.Context, m *MySQL, stream types.StreamInterface, chunks *types.Set[types.Chunk], chunkColumn string, chunkSize int64, minVal any, maxVal any, pkColumns []string) error {
	return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(minVal),
		})

		logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

		// Generate chunks based on range
		query := jdbc.NextChunkEndQuery(stream, pkColumns, chunkSize)
		currentVal := minVal
		for {
			// Split the current value into parts
			columns := strings.Split(utils.ConvertToString(currentVal), ",")

			// Create args array with the correct number of arguments for the query
			args := make([]interface{}, 0)
			for columnIndex := 0; columnIndex < len(pkColumns); columnIndex++ {
				// For each column combination in the WHERE clause, we need to add the necessary parts
				for partIndex := 0; partIndex <= columnIndex && partIndex < len(columns); partIndex++ {
					args = append(args, columns[partIndex])
				}
			}
			var nextValRaw interface{}
			err := tx.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
			if err == sql.ErrNoRows || nextValRaw == nil {
				break
			} else if err != nil {
				return fmt.Errorf("failed to get next chunk end: %s", err)
			}
			if currentVal != nil && nextValRaw != nil {
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

func shouldUseEvenDistribution(
	minVal any,
	maxVal any,
	approxRowCount int64,
	chunkSize int64,
) (bool, int64) {

	if approxRowCount == 0 {
		return false, 0
	}

	minF, err1 := typeutils.ReformatFloat64(minVal)
	maxF, err2 := typeutils.ReformatFloat64(maxVal)
	if err1 != nil || err2 != nil {
		return false, 0
	}
	if maxF < minF {
		return false, 0
	}
	// (max - min + 1) / rowCount(int64)
	distributionFactor := (maxF - minF + 1) / float64(approxRowCount)

	// margin check
	if distributionFactor < constants.DistributionLower ||
		distributionFactor > constants.DistributionUpper {
		return false, 0
	}

	// dynamic chunk size or pk_steps
	dynamicChunkSize := int64(math.Max(distributionFactor*float64(chunkSize), 1))

	return true, dynamicChunkSize
}

func splitEvenlyForInt(minVal, maxVal any, chunks *types.Set[types.Chunk], step int64) {
	start, _ := typeutils.ReformatFloat64(minVal)
	end, _ := typeutils.ReformatFloat64(maxVal)
	logger.Infof("splitEvenlyForInt start=%v end=%v step=%d", start, end, step)
	if start+float64(step) > end {
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: nil,
		})
		logger.Infof("Generated single chunk: %+v", chunks)
		return
	}

	prev := start

	for next := start + float64(step); next <= end; next += float64(step) {
		chunks.Insert(types.Chunk{
			Min: utils.ConvertToString(prev),
			Max: utils.ConvertToString(next),
		})
		prev = next
	}

	chunks.Insert(types.Chunk{
		Min: utils.ConvertToString(prev),
		Max: nil,
	})
	logger.Infof("Generated chunk: %+v", chunks)
}
