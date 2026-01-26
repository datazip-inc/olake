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
		// Get chunks from state or calculate new ones
		stmt := ""
		if chunkColumn != "" {
			stmt = jdbc.MysqlChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt = jdbc.MysqlChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			stmt = jdbc.MysqlLimitOffsetScanQuery(stream, chunk, filter)
		}
		logger.Debugf("Executing chunk query: %s", stmt)
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
			// No partitions found, fall back to non-partitioned logic
			return fmt.Errorf("table marked as partitioned but no partitions found")
		}

		// Get primary key columns
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		if chunkColumn != "" {
			pkColumns = []string{chunkColumn}
		}
		sort.Strings(pkColumns)

		if len(pkColumns) == 0 {
			// No primary key, fall back to limit/offset chunking
			return fmt.Errorf("partitioned table without primary key cannot use partition-aware chunking")
		}

		logger.Infof("Chunking %d partitions for table %s", len(partitionNames), stream.ID())

		// Track partition max values to ensure they're all covered at the end
		partitionMaxValues := make(map[string]bool)
		// Track partition max objects (not just strings) to find global max without re-querying
		var partitionMaxObjects []any

		// Process each partition independently
		for _, partitionName := range partitionNames {
			partitionMin, partitionMax, err := m.getPartitionExtremes(ctx, stream, partitionName, pkColumns, tx)
			if err != nil {
				return fmt.Errorf("failed to get extremes for partition %s: %s", partitionName, err)
			}

			// Skip empty partitions
			if partitionMin == nil {
				logger.Debugf("Partition %s is empty, skipping", partitionName)
				continue
			}

			// Track this partition's max value
			if partitionMax != nil {
				partitionMaxStr := utils.ConvertToString(partitionMax)
				partitionMaxValues[partitionMaxStr] = true
				partitionMaxObjects = append(partitionMaxObjects, partitionMax)
			}

			logger.Debugf("Partition %s extremes - min: %v, max: %v", partitionName, utils.ConvertToString(partitionMin), utils.ConvertToString(partitionMax))

			// Create first chunk for this partition (rows before partitionMin, should be empty but include for completeness)
			if chunks.Len() == 0 {
				chunks.Insert(types.Chunk{
					Min: nil,
					Max: utils.ConvertToString(partitionMin),
				})
			}

			// Generate chunks within this partition
			query := jdbc.NextChunkEndQueryPartition(stream, partitionName, pkColumns, chunkSize)
			currentVal := partitionMin
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
					return fmt.Errorf("failed to get next chunk end for partition %s: %s", partitionName, err)
				}

				// Check if we've exceeded the partition's max value
				if utils.ConvertToString(nextValRaw) > utils.ConvertToString(partitionMax) {
					break
				}

				if currentVal != nil && nextValRaw != nil {
					chunks.Insert(types.Chunk{
						Min: utils.ConvertToString(currentVal),
						Max: utils.ConvertToString(nextValRaw),
					})
				}
				currentVal = nextValRaw
			}

			// Add final chunk for this partition to ensure we don't miss data at partition boundary
			// The loop breaks when nextValRaw > partitionMax, so currentVal might be < partitionMax
			// We need to create a chunk covering [currentVal, partitionMax] (exclusive upper bound)
			// Note: partitionMax itself will be covered by a final chunk created after all partitions
			if currentVal != nil {
				currentValStr := utils.ConvertToString(currentVal)
				partitionMaxStr := utils.ConvertToString(partitionMax)
				// If currentVal < partitionMax, we need to cover the gap
				if currentValStr < partitionMaxStr {
					chunks.Insert(types.Chunk{
						Min: currentValStr,
						Max: partitionMaxStr,
					})
				}
			}
		}

		// After processing all partitions, ensure all partition max values are covered.
		// Since chunks use exclusive upper bounds (pk < max), partition max values are not
		// included in chunks like [currentVal, partitionMax]. We need to create chunks that
		// include each partition max. To avoid duplication, we'll find the minimum partition
		// max that needs coverage and create a single chunk [minUncoveredMax, nil] to cover
		// all partition maxes from that point onwards.
		// Calculate global max from already-collected partition maxes (no need to re-query)
		var globalMax any
		for _, partitionMax := range partitionMaxObjects {
			if partitionMax != nil {
				partitionMaxStr := utils.ConvertToString(partitionMax)
				if globalMax == nil || partitionMaxStr > utils.ConvertToString(globalMax) {
					globalMax = partitionMax
				}
			}
		}

		if globalMax != nil && len(partitionMaxValues) > 0 {
			globalMaxStr := utils.ConvertToString(globalMax)
			chunkList := chunks.Array()

			// Find which partition max values are not covered by existing chunks
			// A partition max is covered if:
			// 1. It's within a bounded chunk [min, max) where partitionMax >= min AND partitionMax < max
			// 2. It's covered by an unbounded chunk [min, nil) where partitionMax >= min
			// 3. There's a chunk that starts exactly at partitionMax (next chunk's min == partitionMax)
			uncoveredMaxes := make([]string, 0)
			for partitionMaxStr := range partitionMaxValues {
				covered := false
				for _, chunk := range chunkList {
					if chunk.Max == nil {
						// Unbounded chunk [min, nil) covers everything from min onwards
						if chunk.Min == nil || partitionMaxStr >= chunk.Min.(string) {
							covered = true
							break
						}
					} else {
						// Bounded chunk [min, max) - partitionMax is covered if it's in range
						chunkMinStr := chunk.Min.(string)
						chunkMaxStr := chunk.Max.(string)
						// partitionMax is covered if: min <= partitionMax < max
						if partitionMaxStr >= chunkMinStr && partitionMaxStr < chunkMaxStr {
							covered = true
							break
						}
						// Also check if there's a chunk that starts exactly at partitionMax
						// (meaning partitionMax is covered by the next chunk)
						if chunkMinStr == partitionMaxStr {
							covered = true
							break
						}
					}
				}
				if !covered {
					uncoveredMaxes = append(uncoveredMaxes, partitionMaxStr)
				}
			}

			// If there are uncovered partition maxes, create a single chunk to cover them all
			// We'll use the minimum uncovered max to minimize overlap
			if len(uncoveredMaxes) > 0 {
				// Sort to find the minimum
				sort.Strings(uncoveredMaxes)
				minUncoveredMax := uncoveredMaxes[0]

				// Check if there's already an unbounded chunk that covers this range
				// to avoid duplication
				hasCoveringUnboundedChunk := false
				for _, chunk := range chunkList {
					if chunk.Max == nil {
						// Unbounded chunk [min, nil) - check if it starts at or before minUncoveredMax
						if chunk.Min == nil || minUncoveredMax >= chunk.Min.(string) {
							hasCoveringUnboundedChunk = true
							break
						}
					}
				}

				if !hasCoveringUnboundedChunk {
					// Create a single chunk [minUncoveredMax, nil] to cover all uncovered partition maxes
					// This avoids duplication while ensuring all partition max values are included
					chunks.Insert(types.Chunk{
						Min: minUncoveredMax,
						Max: nil,
					})
				}
			} else {
				// All partition maxes are covered, but we still need to ensure globalMax is covered
				// Check if globalMax is covered
				globalMaxCovered := false
				for _, chunk := range chunkList {
					if chunk.Max == nil {
						if chunk.Min == nil || globalMaxStr >= chunk.Min.(string) {
							globalMaxCovered = true
							break
						}
					} else {
						chunkMinStr := chunk.Min.(string)
						chunkMaxStr := chunk.Max.(string)
						if globalMaxStr >= chunkMinStr && globalMaxStr < chunkMaxStr {
							globalMaxCovered = true
							break
						}
					}
				}
				if !globalMaxCovered {
					// Create chunk to cover globalMax
					chunks.Insert(types.Chunk{
						Min: globalMaxStr,
						Max: nil,
					})
				}
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

