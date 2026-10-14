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

func (p *Postgres) splitTableIntoChunks(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	generateCTIDRanges := func(stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		blockSizeQuery := jdbc.PostgresBlockSizeQuery()
		var blockSize uint32
		err := p.client.QueryRowContext(ctx, blockSizeQuery).Scan(&blockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to get block size: %w", err)
		}

		var serverVersionNum int
		err = p.client.QueryRowContext(ctx, jdbc.PostgresServerVersionNum()).Scan(&serverVersionNum)
		if err != nil {
			return nil, fmt.Errorf("failed to detect postgres server version: %w", err)
		}
		// Before PG 14 every ctid chunk is a full heap scan (no TID range scan), so more chunks mean more full scans.
		// There chunks keep the heap-only size and the relpages count; PG 14+ counts TOAST and the live heap size.
		tidRangeScan := serverVersionNum >= postgresMinVersionPG14

		//  Step 1: Detect if the table is partitioned
		var partitionCount int
		partitionQuery := jdbc.PostgresIsPartitionedQuery(stream)
		err = p.client.QueryRowContext(ctx, partitionQuery).Scan(&partitionCount)
		if err != nil {
			return nil, fmt.Errorf("failed to detect table partitioning: %w", err)
		}

		//  Step 2: Non-partitioned
		if partitionCount == 0 {
			var relPages uint32
			var heapBytes, tableBytes int64
			relPagesQuery := jdbc.PostgresRelPageCount(stream)
			err := p.client.QueryRowContext(ctx, relPagesQuery).Scan(&relPages, &heapBytes, &tableBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to get relPages: %w", err)
			}

			if tidRangeScan {
				relPages = uint32(math.Ceil(float64(heapBytes) / float64(blockSize)))
			}
			batchSize := uint32(math.Ceil(float64(constants.EffectiveParquetSize) / bytesPerHeapPage(blockSize, heapBytes, tableBytes, tidRangeScan)))
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
		partitions, maxPageCountAcrossPartitions, err := loadPartitionPages(ctx, p.client.DB, stream, serverVersionNum)
		if err != nil {
			return nil, fmt.Errorf("failed to load partition pages: %w", err)
		}

		batchSize := int64(math.Ceil(float64(constants.EffectiveParquetSize) / partitionBytesPerPage(partitions, blockSize, 0, tidRangeScan)))

		chunks := types.NewSet[types.Chunk]()
		for start := int64(0); start < maxPageCountAcrossPartitions; start += batchSize {
			lastChunkEnd := start + batchSize
			batchSize = int64(math.Ceil(float64(constants.EffectiveParquetSize) / partitionBytesPerPage(partitions, blockSize, lastChunkEnd, tidRangeScan)))
			end := start + batchSize
			if end >= maxPageCountAcrossPartitions {
				end = int64(^uint32(0))
			}

			chunks.Insert(types.Chunk{
				Min: fmt.Sprintf("'(%d,0)'", start),
				Max: fmt.Sprintf("'(%d,0)'", end),
			})
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

// bytesPerHeapPage is the table data behind one heap page, TOAST included when countTOAST is set. ctid ranges and
// relpages count heap pages only, so a TOAST-heavy table would otherwise get chunks far above the target size.
func bytesPerHeapPage(blockSize uint32, heapBytes, tableBytes int64, countTOAST bool) float64 {
	if !countTOAST || heapBytes <= 0 || tableBytes <= heapBytes {
		return float64(blockSize)
	}
	return float64(blockSize) * float64(tableBytes) / float64(heapBytes)
}

// postgresMinVersionPG12 is the server_version_num for PostgreSQL 12.0.
// pg_partition_tree() (PG 12+) is preferred; older versions use a recursive CTE.
const postgresMinVersionPG12 = 120000

// postgresMinVersionPG14 is the server_version_num for PostgreSQL 14.0, the first with TID range scans.
const postgresMinVersionPG14 = 140000

func loadPartitionPages(ctx context.Context, db *sql.DB, stream types.StreamInterface, serverVersionNum int) ([]PartitionPage, int64, error) {
	var query string
	if serverVersionNum >= postgresMinVersionPG12 {
		logger.Debugf("using pg_partition_tree for partition page discovery (server_version_num=%d)", serverVersionNum)
		query = jdbc.PostgresPartitionPagesPG12(stream)
	} else {
		logger.Debugf("using recursive CTE for partition page discovery (server_version_num=%d)", serverVersionNum)
		query = jdbc.PostgresPartitionPages(stream)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to load partition pages: %w", err)
	}
	defer rows.Close()

	var maxPageCountAcrossPartitions int64
	var partitions []PartitionPage
	for rows.Next() {
		var p PartitionPage
		if err := rows.Scan(&p.Name, &p.Pages, &p.HeapBytes, &p.TableBytes); err != nil {
			return nil, 0, err
		}
		maxPageCountAcrossPartitions = max(maxPageCountAcrossPartitions, p.Pages)
		partitions = append(partitions, p)
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

// partitionBytesPerPage sums bytesPerHeapPage over partitions with more than 'end' pages, since a ctid
// chunk reads the same page range from each of them.
func partitionBytesPerPage(partitions []PartitionPage, blockSize uint32, end int64, countTOAST bool) float64 {
	var sum float64
	for _, p := range partitions {
		if p.Pages > end {
			sum += bytesPerHeapPage(blockSize, p.HeapBytes, p.TableBytes, countTOAST)
		}
	}
	return utils.Ternary(sum == 0, float64(blockSize), sum).(float64)
}
