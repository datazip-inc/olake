package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

func (p *Postgres) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	parsedFilter, err := p.getParsedFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}
	tx, err := p.client.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	chunkColumn := stream.Self().StreamMetadata.ChunkColumn
	chunkColumn = utils.Ternary(chunkColumn == "", "ctid", chunkColumn).(string)
	stmt := jdbc.PostgresChunkScanQuery(stream, chunkColumn, chunk, parsedFilter)
	setter := jdbc.NewReader(ctx, stmt, p.config.BatchSize, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return tx.Query(query, args...)
	})

	return setter.Capture(func(rows *sql.Rows) error {
		// Create a map to hold column names and values
		record := make(types.Record)

		// Scan the row into the map
		err := jdbc.MapScan(rows, record, p.dataTypeConverter)
		if err != nil {
			return fmt.Errorf("failed to scan record data as map: %s", err)
		}
		return OnMessage(record)
	})
}

func (p *Postgres) GetOrSplitChunks(_ context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	approxRowCountQuery := jdbc.PostgresRowCountQuery(stream)
	// TODO: use ctx while querying
	err := p.client.QueryRow(approxRowCountQuery).Scan(&approxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
	}
	pool.AddRecordsToSync(approxRowCount)
	parsedFilter, err := p.getParsedFilter(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter during chunk splitting: %s", err)
	}
	return p.splitTableIntoChunks(stream, parsedFilter)
}

func (p *Postgres) splitTableIntoChunks(stream types.StreamInterface, parsedFilter string) (*types.Set[types.Chunk], error) {
	generateCTIDRanges := func(stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		var relPages uint32
		relPagesQuery := jdbc.PostgresRelPageCount(stream)
		err := p.client.QueryRow(relPagesQuery).Scan(&relPages)
		if err != nil {
			return nil, fmt.Errorf("failed to get relPages: %s", err)
		}
		relPages = utils.Ternary(relPages == uint32(0), uint32(1), relPages).(uint32)
		chunks := types.NewSet[types.Chunk]()
		batchSize := uint32(p.config.BatchSize)
		for start := uint32(0); start < relPages; start += batchSize {
			end := start + batchSize
			if end >= relPages {
				end = ^uint32(0) // Use max uint32 value for the last range
			}
			chunks.Insert(types.Chunk{Min: fmt.Sprintf("'(%d,0)'", start), Max: fmt.Sprintf("'(%d,0)'", end)})
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

		for utils.CompareInterfaceValue(chunkEnd, max) <= 0 {
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
			chunkEnd, err := p.nextChunkEnd(stream, chunkStart, chunkColumn, parsedFilter)
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
		if parsedFilter != "" {
			minMaxRowCountQuery = fmt.Sprintf("%s WHERE %s", minMaxRowCountQuery, parsedFilter)
		}
		// TODO: Fails on UUID type (Good First Issue)
		err := p.client.QueryRow(minMaxRowCountQuery).Scan(&minValue, &maxValue)
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
			return splitViaBatchSize(minValue, maxValue, p.config.BatchSize)
		}
		return splitViaNextQuery(minValue, stream, chunkColumn)
	} else {
		return generateCTIDRanges(stream)
	}
}

func (p *Postgres) nextChunkEnd(stream types.StreamInterface, previousChunkEnd interface{}, chunkColumn string, parsedFilter string) (interface{}, error) {
	var chunkEnd interface{}
	nextChunkEnd := jdbc.PostgresNextChunkEndQuery(stream, chunkColumn, previousChunkEnd, p.config.BatchSize, parsedFilter)
	err := p.client.QueryRow(nextChunkEnd).Scan(&chunkEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to query[%s] next chunk end: %s", nextChunkEnd, err)
	}
	return chunkEnd, nil
}

func (p *Postgres) getParsedFilter(stream types.StreamInterface) (string, error) {
	filter := stream.Self().StreamMetadata.Filter
	if filter == "" {
		return "", nil
	}
	parsedFilter, err := abstract.ParseFilter(filter)
	if err != nil {
		return "", fmt.Errorf("failed to parse filter: %s", err)
	}
	postgresFilter, _ := jdbc.SQLFilter(parsedFilter, p.Type())
	return postgresFilter, nil
}
