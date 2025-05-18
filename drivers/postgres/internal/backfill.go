package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// Simple Full Refresh Sync; Loads table fully
func (p *Postgres) Backfill(ctx context.Context, backfilledStreams chan string, pool *protocol.WriterPool, stream protocol.Stream) error {
	streamIterator := func(ctx context.Context, chunk types.Chunk, callback base.BackfillMessageProcessFunc) error {
		tx, err := p.client.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		splitColumn := stream.Self().StreamMetadata.SplitColumn
		splitColumn = utils.Ternary(splitColumn == "", "ctid", splitColumn).(string)
		stmt := jdbc.PostgresChunkScanQuery(stream, splitColumn, chunk)
		setter := jdbc.NewReader(ctx, stmt, p.config.BatchSize, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return tx.Query(query, args...)
		})

		return setter.Capture(func(rows *sql.Rows) error {
			// Create a map to hold column names and values
			record := make(types.Record)

			// Scan the row into the map
			err := jdbc.MapScan(rows, record, p.dataTypeConverter)
			if err != nil {
				return fmt.Errorf("failed to mapScan record data: %s", err)
			}
			return callback(record)
		})
	}
	return p.Driver.Backfill(ctx, p.getOrSplitChunks, streamIterator, backfilledStreams, pool, stream)
}

func (p *Postgres) getOrSplitChunks(pool *protocol.WriterPool, stream protocol.Stream) ([]types.Chunk, error) {
	var approxRowCount int64
	approxRowCountQuery := jdbc.PostgresRowCountQuery(stream)
	err := p.client.QueryRow(approxRowCountQuery).Scan(&approxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
	}
	pool.AddRecordsToSync(approxRowCount)

	stateChunks := p.State.GetChunks(stream.Self())
	var splitChunks []types.Chunk
	if stateChunks == nil {
		// check for data distribution
		// TODO: remove chunk intersections where chunks can be {0, 100} {100, 200}. Need to {0, 99} {100, 200}
		splitChunks, err = p.splitTableIntoChunks(stream)
		if err != nil {
			return nil, fmt.Errorf("failed to start backfill: %s", err)
		}
		p.State.SetChunks(stream.Self(), types.NewSet(splitChunks...))
	} else {
		splitChunks = stateChunks.Array()
	}
	return splitChunks, nil
}

func (p *Postgres) splitTableIntoChunks(stream protocol.Stream) ([]types.Chunk, error) {
	generateCTIDRanges := func(stream protocol.Stream) ([]types.Chunk, error) {
		var relPages uint32
		relPagesQuery := jdbc.PostgresRelPageCount(stream)
		err := p.client.QueryRow(relPagesQuery).Scan(&relPages)
		if err != nil {
			return nil, fmt.Errorf("failed to get relPages: %s", err)
		}
		relPages = utils.Ternary(relPages == uint32(0), uint32(1), relPages).(uint32)
		var chunks []types.Chunk
		batchSize := uint32(p.config.BatchSize)
		for start := uint32(0); start < relPages; start += batchSize {
			end := start + batchSize
			if end >= relPages {
				end = ^uint32(0) // Use max uint32 value for the last range
			}
			chunks = append(chunks, types.Chunk{Min: fmt.Sprintf("'(%d,0)'", start), Max: fmt.Sprintf("'(%d,0)'", end)})
		}
		return chunks, nil
	}

	splitViaBatchSize := func(min, max interface{}, dynamicChunkSize int) ([]types.Chunk, error) {
		var splits []types.Chunk
		chunkStart := min
		chunkEnd, err := utils.AddConstantToInterface(min, dynamicChunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to split batch size chunks: %s", err)
		}

		for utils.CompareInterfaceValue(chunkEnd, max) <= 0 {
			splits = append(splits, types.Chunk{Min: chunkStart, Max: chunkEnd})
			chunkStart = chunkEnd
			newChunkEnd, err := utils.AddConstantToInterface(chunkEnd, dynamicChunkSize)
			if err != nil {
				return nil, fmt.Errorf("failed to split batch size chunks: %s", err)
			}
			chunkEnd = newChunkEnd
		}
		return append(splits, types.Chunk{Min: chunkStart, Max: nil}), nil
	}

	splitViaNextQuery := func(min interface{}, stream protocol.Stream, splitColumn string) ([]types.Chunk, error) {
		chunkStart := min
		var splits []types.Chunk

		for {
			chunkEnd, err := p.nextChunkEnd(stream, chunkStart, splitColumn)
			if err != nil {
				return nil, fmt.Errorf("failed to split chunks based on next query size: %s", err)
			}
			if chunkEnd == nil || chunkEnd == chunkStart {
				// No more new chunks
				break
			}

			splits = append(splits, types.Chunk{Min: chunkStart, Max: chunkEnd})
			chunkStart = chunkEnd
		}
		return splits, nil
	}

	splitColumn := stream.Self().StreamMetadata.SplitColumn
	if splitColumn != "" {
		var minValue, maxValue interface{}
		minMaxRowCountQuery := jdbc.MinMaxQuery(stream, splitColumn)
		// TODO: Fails on UUID type (Good First Issue)
		err := p.client.QueryRow(minMaxRowCountQuery).Scan(&minValue, &maxValue)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch table min max: %s", err)
		}
		if minValue == maxValue {
			return []types.Chunk{{Min: minValue, Max: maxValue}}, nil
		}

		_, contains := utils.ArrayContains(stream.GetStream().SourceDefinedPrimaryKey.Array(), func(element string) bool {
			return element == splitColumn
		})
		if !contains {
			return nil, fmt.Errorf("provided split column is not a primary key")
		}

		splitColType, _ := stream.Schema().GetType(splitColumn)
		// evenly distirbution only available for float and int types
		if splitColType == types.Int64 || splitColType == types.Float64 {
			return splitViaBatchSize(minValue, maxValue, p.config.BatchSize)
		}
		return splitViaNextQuery(minValue, stream, splitColumn)
	} else {
		return generateCTIDRanges(stream)
	}
}

func (p *Postgres) nextChunkEnd(stream protocol.Stream, previousChunkEnd interface{}, splitColumn string) (interface{}, error) {
	var chunkEnd interface{}
	nextChunkEnd := jdbc.PostgresNextChunkEndQuery(stream, splitColumn, previousChunkEnd, p.config.BatchSize)
	err := p.client.QueryRow(nextChunkEnd).Scan(&chunkEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to query[%s] next chunk end: %s", nextChunkEnd, err)
	}
	return chunkEnd, nil
}
