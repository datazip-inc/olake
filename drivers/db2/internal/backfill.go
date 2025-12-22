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

func (d *DB2) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.DB2,
		Stream: stream,
		State:  d.state,
	}

	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, d.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	// if PK present then PK based chunking else RID based chunking
	pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()

	var stmt string
	if len(pkColumns) > 0 || stream.Self().StreamMetadata.ChunkColumn != "" {
		stmt = jdbc.DB2PKChunkScanQuery(stream, pkColumns, chunk, filter)
	} else {
		stmt = jdbc.DB2RidChunkScanQuery(stream, chunk, filter)
	}

	logger.Debugf("Starting backfill for %s with chunk %v using query: %s", stream.ID(), chunk, stmt)

	reader := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
		return d.client.QueryContext(ctx, query, args...)
	})

	return reader.Capture(func(rows *sql.Rows) error {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, d.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record data as map: %s", err)
		}
		return OnMessage(ctx, record)
	})
}

func (d *DB2) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// Approx Row Count
	var approxRowCount int64
	rowCountQuery := jdbc.DB2ApproxRowCountQuery(stream)
	if err := d.client.QueryRowContext(ctx, rowCountQuery).Scan(&approxRowCount); err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
	}
	if approxRowCount == -1 {
		var hasRows bool
		existsQuery := jdbc.DB2TableExistQuery(stream)
		err := d.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)

		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run command:\tRUNSTATS ON TABLE %s.%s WITH DISTRIBUTION AND DETAILED INDEXES ALL;\t to update table statistics", stream.ID(), stream.Namespace(), stream.Name())
		}

		logger.Warnf("Table %s is empty, skipping chunking", stream.ID())
		return types.NewSet[types.Chunk](), nil
	}
	pool.AddRecordsToSyncStats(approxRowCount)
	return d.splitTableIntoChunks(ctx, stream)
}

func (d *DB2) splitTableIntoChunks(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// split chunks via primary key
	splitViaPrimaryKey := func(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		// avg row size
		var avgRowSize any
		avgRowSizeQuery := jdbc.DB2AvgRowSizeQuery(stream)
		err := d.client.QueryRowContext(ctx, avgRowSizeQuery).Scan(&avgRowSize)
		if err != nil {
			logger.Errorf("Failed to get avg row size for stream %s", stream.ID(), err)
		}
		avgRowSizeFloat, err := typeutils.ReformatFloat64(avgRowSize)

		// chunk size
		chunkSize := int64(math.Ceil(float64(constants.EffectiveParquetSize) / avgRowSizeFloat))
		chunkColumn := stream.Self().StreamMetadata.ChunkColumn
		chunks := types.NewSet[types.Chunk]()

		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		if chunkColumn != "" {
			pkColumns = []string{chunkColumn}
		}
		sort.Strings(pkColumns)

		// table extremes
		minVal, maxVal, err := d.getTableExtremes(ctx, stream, pkColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to get table extremes: %s", err)
		}
		if minVal == nil {
			return nil, nil
		}

		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(minVal),
		})

		logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

		// chunks generation based on range
		query := jdbc.DB2NextChunkEndQuery(stream, pkColumns, chunkSize)
		currentVal := minVal
		for {
			columns := strings.Split(utils.ConvertToString(currentVal), ",")

			args := make([]interface{}, 0)
			for columnIndex := 0; columnIndex < len(pkColumns); columnIndex++ {
				// For each column combination in the WHERE clause, we need to add the necessary parts
				for partIndex := 0; partIndex <= columnIndex && partIndex < len(columns); partIndex++ {
					args = append(args, columns[partIndex])
				}
			}
			var nextValRaw interface{}
			err := d.client.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
			if err == sql.ErrNoRows || nextValRaw == nil {
				break
			} else if err != nil {
				return nil, fmt.Errorf("failed to get next chunk end: %s", err)
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

		return chunks, nil
	}
	// split chunks via physical identifier RID()
	splitViaRID := func(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		var minRID, maxRID int64
		if err := d.client.QueryRowContext(ctx, jdbc.DB2MinMaxRidQuery(stream)).Scan(&minRID, &maxRID); err != nil {
			return nil, fmt.Errorf("failed to get the min and max rid: %s", err)
		}

		// pages size and number of pages
		var pageSize, nPages int64
		pageStatsQuery := jdbc.DB2PageStatsQuery(stream)
		if err := d.client.QueryRowContext(ctx, pageStatsQuery).Scan(&pageSize, &nPages); err != nil {
			return nil, fmt.Errorf("failed to get the page size and number of pages: %s", err)
		}

		// pages to be in a chunk
		pagesPerChunk := int64(math.Ceil(float64(int64(constants.EffectiveParquetSize)) / float64(pageSize)))
		nPages = utils.Ternary(nPages <= 0, int64(1), nPages).(int64)

		// number of chunks
		targetChunkCount := int64(math.Ceil(float64(nPages) / float64(pagesPerChunk)))
		targetChunkCount = utils.Ternary(targetChunkCount <= 0, int64(1), targetChunkCount).(int64)

		totalRidRange := maxRID - minRID
		// distance between start and end RID of a chunk
		ridInterval := int64(math.Ceil(float64(totalRidRange) / float64(targetChunkCount)))
		chunks := types.NewSet[types.Chunk]()
		for start := minRID; start <= maxRID; start += ridInterval {
			end := start + ridInterval
			if end > maxRID+1 {
				end = maxRID + 1
			}
			chunks.Insert(types.Chunk{Min: start, Max: end})
		}

		return chunks, nil
	}
	if stream.GetStream().SourceDefinedPrimaryKey.Len() > 0 || stream.Self().StreamMetadata.ChunkColumn != "" {
		return splitViaPrimaryKey(ctx, stream)
	}
	return splitViaRID(ctx, stream)
}

func (d *DB2) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string) (min, max any, err error) {
	query := jdbc.DB2MinMaxPKQuery(stream, pkColumns)
	err = d.client.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}
