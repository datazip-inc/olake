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

		if chunk.PartitionName != "" {
			logger.Debugf("Starting backfill from %v to %v for partition %s with filter: %s, args: %v", chunk.Min, chunk.Max, chunk.PartitionName, filter, args)
		} else {
			logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)
		}
		// Get chunks from state or calculate new ones
		stmt := ""
		if chunkColumn != "" {
			stmt = jdbc.MysqlChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt = jdbc.MysqlChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			stmt = jdbc.MysqlLimitOffsetScanQuery(stream, chunk, filter)
		}
		if chunk.PartitionName != "" {
			logger.Debugf("Executing chunk query with partition pruning (partition: %s): %s", chunk.PartitionName, stmt)
		} else {
			logger.Debugf("Executing chunk query: %s", stmt)
		}
		setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})
		return jdbc.MapScanConcurrent(setter, m.dataTypeConverter, OnMessage)
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

	// detect if the table is partitioned
	var partitionCount int
	partitionQuery := jdbc.MySQLIsPartitionedQuery()
	err = m.client.QueryRowContext(ctx, partitionQuery, stream.Namespace(), stream.Name()).Scan(&partitionCount)
	if err != nil {
		return nil, fmt.Errorf("failed to detect table partitioning: %s", err)
	}

	// Step 2: Use partition-aware chunking if table is partitioned
	if partitionCount > 0 {
		logger.Infof("Table %s is partitioned with %d partitions, using partition-aware chunking", stream.ID(), partitionCount)
		return m.splitPartitionedTableIntoChunks(ctx, stream, chunkSize, chunkColumn, chunks)
	}

	// Takes the user defined batch size as chunkSize
	// TODO: common-out the chunking logic for db2, mssql, mysql
	splitViaPrimaryKey := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
			// Get primary key column using the provided function
			pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
			if chunkColumn != "" {
				pkColumns = []string{chunkColumn}
			}
			sort.Strings(pkColumns)
			// Get table extremes
			minVal, maxVal, err := m.getTableExtremes(ctx, stream, pkColumns, tx)
			if err != nil {
				return fmt.Errorf("failed to get table extremes: %s", err)
			}
			if minVal == nil {
				return nil
			}
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
	limitOffsetChunking := func(chunks *types.Set[types.Chunk]) error {
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

	if stream.GetStream().SourceDefinedPrimaryKey.Len() > 0 || chunkColumn != "" {
		err = splitViaPrimaryKey(stream, chunks)
	} else {
		err = limitOffsetChunking(chunks)
	}
	return chunks, err
}

func (m *MySQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMySQL(stream, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// splitPartitionedTableIntoChunks implements partition-aware chunking for MySQL partitioned tables.
// This approach chunks each partition independently, leveraging partition pruning for better performance.
func (m *MySQL) splitPartitionedTableIntoChunks(ctx context.Context, stream types.StreamInterface, chunkSize int64, chunkColumn string, chunks *types.Set[types.Chunk]) (*types.Set[types.Chunk], error) {
	err := jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
		// Get partition names
		partitionQuery := jdbc.MySQLListPartitionsQuery()
		rows, err := tx.QueryContext(ctx, partitionQuery, stream.Namespace(), stream.Name())
		if err != nil {
			return fmt.Errorf("failed to list partitions: %s", err)
		}
		defer rows.Close()

		var partitionNames []string
		for rows.Next() {
			var partitionName string
			if err := rows.Scan(&partitionName); err != nil {
				return fmt.Errorf("failed to scan partition name: %s", err)
			}
			partitionNames = append(partitionNames, partitionName)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating partitions: %s", err)
		}

		if len(partitionNames) == 0 {
			return fmt.Errorf("table marked as partitioned but no partitions found")
		}

		// Get primary key columns
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		if chunkColumn != "" {
			pkColumns = []string{chunkColumn}
		}
		sort.Strings(pkColumns)

		if len(pkColumns) == 0 {
			return fmt.Errorf("partitioned table without primary key cannot use partition-aware chunking")
		}

		logger.Infof("Chunking %d partitions for table %s", len(partitionNames), stream.ID())

		// Process each partition independently
		for _, partitionName := range partitionNames {
			partitionMin, _, err := m.getPartitionExtremes(ctx, stream, partitionName, pkColumns, tx)
			if err != nil {
				return fmt.Errorf("failed to get extremes for partition %s: %s", partitionName, err)
			}

			// Skip empty partitions
			if partitionMin == nil {
				logger.Debugf("Partition %s is empty, skipping", partitionName)
				continue
			}

			// Create the first chunk for this partition (covering everything before its min)
			// This ensures completeness even if boundaries are fuzzy.
			chunks.Insert(types.Chunk{
				Min:           nil,
				Max:           utils.ConvertToString(partitionMin),
				PartitionName: partitionName,
			})

			// Generate chunks within this partition using keyset pagination
			query := jdbc.NextChunkEndQueryPartition(stream, partitionName, pkColumns, chunkSize)
			currentVal := partitionMin
			for {
				columns := strings.Split(utils.ConvertToString(currentVal), ",")
				args := make([]interface{}, 0)
				for columnIndex := 0; columnIndex < len(pkColumns); columnIndex++ {
					for partIndex := 0; partIndex <= columnIndex && partIndex < len(columns); partIndex++ {
						args = append(args, columns[partIndex])
					}
				}

				var nextValRaw interface{}
				err := tx.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
				if err == sql.ErrNoRows || nextValRaw == nil {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to get next chunk end for partition %s: %s", partitionName, err)
				}

				nextValStr := utils.ConvertToString(nextValRaw)
				chunks.Insert(types.Chunk{
					Min:           utils.ConvertToString(currentVal),
					Max:           nextValStr,
					PartitionName: partitionName,
				})
				currentVal = nextValRaw
			}

			// Add the final unbounded chunk for this partition.
			// This covers the partition's maximum value and anything remaining.
			if currentVal != nil {
				chunks.Insert(types.Chunk{
					Min:           utils.ConvertToString(currentVal),
					Max:           nil,
					PartitionName: partitionName,
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chunks, nil
}

// getPartitionExtremes gets the MIN and MAX primary key values for a specific partition.
func (m *MySQL) getPartitionExtremes(ctx context.Context, stream types.StreamInterface, partitionName string, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMySQLPartition(stream, partitionName, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}
