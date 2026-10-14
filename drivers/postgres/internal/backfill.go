package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func (p *Postgres) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.Postgres,
		Stream: stream,
		State:  p.state,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %w", err)
	}

	filter, err := jdbc.SQLFilter(stream, p.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %w", err)
	}
	tx, err := p.client.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			logger.Warnf("failed to rollback transaction: %s", err)
		}
	}()

	logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)

	chunkColumn := stream.Self().StreamMetadata.ChunkColumn
	chunkColumn = utils.Ternary(chunkColumn == "", "ctid", chunkColumn).(string)
	stmt := jdbc.PostgresChunkScanQuery(stream, chunkColumn, chunk, filter)
	setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, _ ...any) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	})

	return jdbc.MapScanConcurrent(setter, p.dataTypeConverter, OnMessage, pgColumnSizer)
}

func (p *Postgres) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	approxRowCountQuery := jdbc.PostgresRowCountQuery(stream)
	err := p.client.QueryRowContext(ctx, approxRowCountQuery).Scan(&approxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %w", err)
	}
	pool.AddRecordsToSyncStats(approxRowCount)
	return p.splitTableIntoChunks(ctx, stream)
}

// TODO: add functional tests for splitTableIntoChunks, covering TOAST-heavy, partitioned and pre-PG 14 tables.
func (p *Postgres) splitTableIntoChunks(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	generateCTIDRanges := func(stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		blockSizeQuery := jdbc.PostgresBlockSizeQuery()
		var blockSize uint32
		err := p.client.QueryRowContext(ctx, blockSizeQuery).Scan(&blockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to get block size: %w", err)
		}

		//  Step 1: Detect if the table is partitioned
		var partitionCount int
		partitionQuery := jdbc.PostgresIsPartitionedQuery(stream)
		err = p.client.QueryRowContext(ctx, partitionQuery).Scan(&partitionCount)
		if err != nil {
			return nil, fmt.Errorf("failed to detect table partitioning: %w", err)
		}

		//  Step 2: Non-partitioned
		if partitionCount == 0 {
			var heapBytes, tableBytes int64
			relationSizeQuery := jdbc.PostgresRelationSizeQuery(stream)
			err := p.client.QueryRowContext(ctx, relationSizeQuery).Scan(&heapBytes, &tableBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to get relation size: %w", err)
			}

			relPages := uint32(math.Ceil(float64(heapBytes) / float64(blockSize)))
			batchSize := uint32(p.sizer.pagesPerChunk(float64(blockSize), bytesPerHeapPage(blockSize, heapBytes, tableBytes), heapBytes))
			relPages = utils.Ternary(relPages == 0, uint32(1), relPages).(uint32)

			chunks := types.NewSet[types.Chunk]()
			for start := uint32(0); start < relPages; start += batchSize {
				end := start + batchSize
				if end >= relPages {
					end = ^uint32(0)
				}
				chunks.Insert(types.Chunk{
					Min: fmt.Sprintf("'(%d,0)'", start),
					Max: fmt.Sprintf("'(%d,0)'", end),
				})
			}
			return chunks, nil
		}

		//  Step 3: Partitioned table
		partitions, maxPageCountAcrossPartitions, err := p.loadPartitionPages(ctx, stream, blockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to load partition pages: %w", err)
		}

		var scannedHeapBytes int64
		for _, partition := range partitions {
			scannedHeapBytes += partition.HeapBytes
		}

		chunks := types.NewSet[types.Chunk]()
		for start := int64(0); start < maxPageCountAcrossPartitions; {
			// size the chunk from the partitions that still have pages at its start
			heapPageBytes, effectivePageSize := partitionPageBytes(partitions, blockSize, start)
			end := start + int64(p.sizer.pagesPerChunk(heapPageBytes, effectivePageSize, scannedHeapBytes))
			if end >= maxPageCountAcrossPartitions {
				end = int64(^uint32(0))
			}

			chunks.Insert(types.Chunk{
				Min: fmt.Sprintf("'(%d,0)'", start),
				Max: fmt.Sprintf("'(%d,0)'", end),
			})
			start = end
		}

		return chunks, nil
	}

	splitViaBatchSize := func(minVal, maxVal interface{}, dynamicChunkSize int) (*types.Set[types.Chunk], error) {
		splits := types.NewSet[types.Chunk]()
		chunkStart := minVal
		chunkEnd, err := utils.AddConstantToInterface(minVal, dynamicChunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to split batch size chunks: %w", err)
		}

		for typeutils.Compare(chunkEnd, maxVal) <= 0 {
			splits.Insert(types.Chunk{Min: chunkStart, Max: chunkEnd})
			chunkStart = chunkEnd
			newChunkEnd, err := utils.AddConstantToInterface(chunkEnd, dynamicChunkSize)
			if err != nil {
				return nil, fmt.Errorf("failed to split batch size chunks: %w", err)
			}
			chunkEnd = newChunkEnd
		}
		splits.Insert(types.Chunk{Min: chunkStart, Max: nil})
		return splits, nil
	}

	splitViaNextQuery := func(minVal interface{}, stream types.StreamInterface, chunkColumn string) (*types.Set[types.Chunk], error) {
		chunkStart := minVal
		splits := types.NewSet[types.Chunk]()
		for {
			chunkEnd, err := p.nextChunkEnd(ctx, stream, chunkStart, chunkColumn)
			if err != nil {
				return nil, fmt.Errorf("failed to split chunks based on next query size: %w", err)
			}
			if chunkEnd == nil || chunkEnd == chunkStart {
				splits.Insert(types.Chunk{Min: chunkStart, Max: nil})
				break
			}

			splits.Insert(types.Chunk{Min: chunkStart, Max: chunkEnd})
			chunkStart = chunkEnd
		}
		return splits, nil
	}

	chunkColumn := stream.Self().StreamMetadata.ChunkColumn
	if chunkColumn != "" {
		var minValue, maxValue interface{}
		minMaxRowCountQuery := jdbc.MinMaxQuery(stream, chunkColumn)
		// TODO: Fails on UUID type (Good First Issue)
		err := p.client.QueryRowContext(ctx, minMaxRowCountQuery).Scan(&minValue, &maxValue)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch table min max: %w", err)
		}
		if minValue == maxValue {
			return types.NewSet(types.Chunk{Min: minValue, Max: nil}), nil
		}

		_, contains := utils.ArrayContains(stream.GetStream().SourceDefinedPrimaryKey.Array(), func(element string) bool {
			return element == chunkColumn
		})
		if !contains {
			return nil, fmt.Errorf("provided split column is not a primary key")
		}

		chunkColType, _ := stream.Schema().GetType(chunkColumn)
		// evenly distribution only available for float and int types
		if chunkColType == types.Int64 || chunkColType == types.Float64 {
			return splitViaBatchSize(minValue, maxValue, 10000)
		}
		return splitViaNextQuery(minValue, stream, chunkColumn)
	}
	return generateCTIDRanges(stream)
}

func (p *Postgres) nextChunkEnd(ctx context.Context, stream types.StreamInterface, previousChunkEnd interface{}, chunkColumn string) (interface{}, error) {
	var chunkEnd interface{}
	nextChunkEnd := jdbc.PostgresNextChunkEndQuery(stream, chunkColumn, previousChunkEnd)
	err := p.client.QueryRowContext(ctx, nextChunkEnd).Scan(&chunkEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to query[%s] next chunk end: %w", nextChunkEnd, err)
	}
	return chunkEnd, nil
}

type PartitionPage struct {
	Name       string
	Pages      int64
	HeapBytes  int64
	TableBytes int64
}

// bytesPerHeapPage is the table data (heap + TOAST) behind one heap page. ctid ranges count heap pages
// only, so a TOAST-heavy table would otherwise get chunks far above the target size.
func bytesPerHeapPage(blockSize uint32, heapBytes, tableBytes int64) float64 {
	if heapBytes <= 0 || tableBytes <= heapBytes {
		return float64(blockSize)
	}
	return float64(blockSize) * float64(tableBytes) / float64(heapBytes)
}

// postgresMinVersionPG12 is the server_version_num for PostgreSQL 12.0.
// pg_partition_tree() (PG 12+) is preferred; older versions use a recursive CTE.
const postgresMinVersionPG12 = 120000

func (p *Postgres) loadPartitionPages(ctx context.Context, stream types.StreamInterface, blockSize uint32) ([]PartitionPage, int64, error) {
	var query string
	if p.version >= postgresMinVersionPG12 {
		logger.Debugf("using pg_partition_tree for partition page discovery (server_version_num=%d)", p.version)
		query = jdbc.PostgresPartitionPagesPG12(stream)
	} else {
		logger.Debugf("using recursive CTE for partition page discovery (server_version_num=%d)", p.version)
		query = jdbc.PostgresPartitionPages(stream)
	}

	rows, err := p.client.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to load partition pages: %w", err)
	}
	defer rows.Close()

	var maxPageCountAcrossPartitions int64
	var partitions []PartitionPage
	for rows.Next() {
		var partition PartitionPage
		if err := rows.Scan(&partition.Name, &partition.HeapBytes, &partition.TableBytes); err != nil {
			return nil, 0, err
		}
		// ctid block numbers are uint32, so never plan past the last one
		partition.Pages = min(int64(math.Ceil(1.05*float64(partition.HeapBytes/int64(blockSize)))), int64(^uint32(0)))
		maxPageCountAcrossPartitions = max(maxPageCountAcrossPartitions, partition.Pages)
		partitions = append(partitions, partition)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate partition pages: %w", err)
	}

	if maxPageCountAcrossPartitions == 0 {
		logger.Warnf("all partitions are empty for stream[%s], skipping chunking", stream.ID())
		return partitions, 0, nil
	}

	return partitions, maxPageCountAcrossPartitions, nil
}

// partitionPageBytes sums the heap bytes and the effective page size (heap + TOAST bytes) behind one page offset
// over partitions with more than 'end' pages, since a ctid chunk reads the same page range from each of them.
func partitionPageBytes(partitions []PartitionPage, blockSize uint32, end int64) (heapPageBytes, effectivePageSize float64) {
	for _, p := range partitions {
		if p.Pages > end {
			heapPageBytes += float64(blockSize)
			effectivePageSize += bytesPerHeapPage(blockSize, p.HeapBytes, p.TableBytes)
		}
	}
	if heapPageBytes == 0 {
		return float64(blockSize), float64(blockSize)
	}
	return heapPageBytes, effectivePageSize
}
