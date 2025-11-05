package driver

import (
	"context"
	"database/sql"
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
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, p.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}
	tx, err := p.client.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)

	chunkColumn := stream.Self().StreamMetadata.ChunkColumn
	chunkColumn = utils.Ternary(chunkColumn == "", "ctid", chunkColumn).(string)
	stmt := jdbc.PostgresChunkScanQuery(stream, chunkColumn, chunk, filter)
	setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	})

	return setter.Capture(func(rows *sql.Rows) error {
		// Create a map to hold column names and values
		record := make(types.Record)

		// Scan the row into the map
		err := jdbc.MapScan(rows, record, p.dataTypeConverter)
		if err != nil {
			return fmt.Errorf("failed to scan record data as map: %s", err)
		}

		return OnMessage(ctx, record)
	})
}

func (p *Postgres) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	approxRowCountQuery := jdbc.PostgresRowCountQuery(stream)
	err := p.client.QueryRowContext(ctx, approxRowCountQuery).Scan(&approxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
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
			return nil, fmt.Errorf("failed to get block size: %s", err)
		}

		//  Step 1: Detect if the table is partitioned
		var partitionCount int
		partitionQuery := jdbc.PostgresIsPartitionedQuery(stream)
		err = p.client.QueryRowContext(ctx, partitionQuery).Scan(&partitionCount)
		if err != nil {
			return nil, fmt.Errorf("failed to detect table partitioning: %s", err)
		}

		//  Step 2: Non-partitioned
		if partitionCount == 0 {
			var relPages uint32
			relPagesQuery := jdbc.PostgresRelPageCount(stream)
			err := p.client.QueryRowContext(ctx, relPagesQuery).Scan(&relPages)
			if err != nil {
				return nil, fmt.Errorf("failed to get relPages: %s", err)
			}

			batchSize := uint32(math.Ceil(float64(constants.EffectiveParquetSize) / float64(blockSize)))
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
		partitions, maxPageCountAcrossPartitions, err := loadPartitionPages(ctx, p.client.DB, stream)
		if err != nil {
			return nil, fmt.Errorf("failed to load partition pages: %s", err)
		}

		batchPages := uint32(math.Ceil(float64(constants.EffectiveParquetSize) / float64(blockSize)))
		partionsInRange := PartitionPagesGreaterThan(partitions, 0)
		batchSize := uint32(math.Ceil(float64(batchPages) / float64(partionsInRange)))

		chunks := types.NewSet[types.Chunk]()
		for start := uint32(0); start < maxPageCountAcrossPartitions; start += batchSize {
			lastChunkEnd := start + batchSize
			partionsInRange := PartitionPagesGreaterThan(partitions, lastChunkEnd)
			batchSize = uint32(math.Ceil(float64(batchPages) / float64(partionsInRange)))
			end := start + batchSize
			if end >= maxPageCountAcrossPartitions {
				end = ^uint32(0)
			}

			chunks.Insert(types.Chunk{
				Min: fmt.Sprintf("'(%d,0)'", start),
				Max: fmt.Sprintf("'(%d,0)'", end),
			})

		}

		return chunks, nil
	}

	splitViaBatchSize := func(min, max interface{}, dynamicChunkSize int) (*types.Set[types.Chunk], error) {
		splits := types.NewSet[types.Chunk]()
		chunkStart := min
		chunkEnd, err := utils.AddConstantToInterface(min, dynamicChunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to split batch size chunks: %s", err)
		}

		for typeutils.Compare(chunkEnd, max) <= 0 {
			splits.Insert(types.Chunk{Min: chunkStart, Max: chunkEnd})
			chunkStart = chunkEnd
			newChunkEnd, err := utils.AddConstantToInterface(chunkEnd, dynamicChunkSize)
			if err != nil {
				return nil, fmt.Errorf("failed to split batch size chunks: %s", err)
			}
			chunkEnd = newChunkEnd
		}
		splits.Insert(types.Chunk{Min: chunkStart, Max: nil})
		return splits, nil
	}

	splitViaNextQuery := func(min interface{}, stream types.StreamInterface, chunkColumn string) (*types.Set[types.Chunk], error) {
		chunkStart := min
		splits := types.NewSet[types.Chunk]()
		for {
			chunkEnd, err := p.nextChunkEnd(ctx, stream, chunkStart, chunkColumn)
			if err != nil {
				return nil, fmt.Errorf("failed to split chunks based on next query size: %s", err)
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
			return nil, fmt.Errorf("failed to fetch table min max: %s", err)
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
		// evenly distirbution only available for float and int types
		if chunkColType == types.Int64 || chunkColType == types.Float64 {
			return splitViaBatchSize(minValue, maxValue, 10000)
		}
		return splitViaNextQuery(minValue, stream, chunkColumn)
	} else {
		return generateCTIDRanges(stream)
	}
}

func (p *Postgres) nextChunkEnd(ctx context.Context, stream types.StreamInterface, previousChunkEnd interface{}, chunkColumn string) (interface{}, error) {
	var chunkEnd interface{}
	nextChunkEnd := jdbc.PostgresNextChunkEndQuery(stream, chunkColumn, previousChunkEnd)
	err := p.client.QueryRowContext(ctx, nextChunkEnd).Scan(&chunkEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to query[%s] next chunk end: %s", nextChunkEnd, err)
	}
	return chunkEnd, nil
}

type PartitionPage struct {
	Name  string
	Pages uint32
}

// loadPartitionPages fetches partition-level relpages using PostgresPartitionPages query.
func loadPartitionPages(ctx context.Context, db *sql.DB, stream types.StreamInterface) ([]PartitionPage, uint32, error) {
	query := jdbc.PostgresPartitionPages(stream)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to load partition pages: %s", err)
	}
	var maxPageCountAcrossPartitions uint32
	defer rows.Close()

	var partitions []PartitionPage
	for rows.Next() {
		var p PartitionPage
		if err := rows.Scan(&p.Name, &p.Pages); err != nil {
			return nil, 0, err
		}
		maxPageCountAcrossPartitions = max(maxPageCountAcrossPartitions, p.Pages)
		partitions = append(partitions, p)
	}

	if len(partitions) == 0 {
		partitions = append(partitions, PartitionPage{Name: stream.Name(), Pages: 1})
	}
	return partitions, maxPageCountAcrossPartitions, nil
}

// PartitionPagesGreaterThan returns how many partitions have pages greater than the given 'end' page.
func PartitionPagesGreaterThan(partitions []PartitionPage, end uint32) int {
	count := 0
	for _, p := range partitions {
		if p.Pages > end {
			count++
		}
	}
	return utils.Ternary(count == 0, 1, count).(int)
}
