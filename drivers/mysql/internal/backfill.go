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
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) (err error) {
	parsedFilter, err := m.getParsedFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to parse filter: %s", err)
	}
	// Begin transaction with repeatable read isolation
	return jdbc.WithIsolation(ctx, m.client, func(tx *sql.Tx) error {
		// Build query for the chunk
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey
		pkColumn := pkColumns.Array()[0]
		// Get chunks from state or calculate new ones
		stmt := jdbc.MysqlChunkScanQuery(stream, pkColumn, chunk, parsedFilter)
		setter := jdbc.NewReader(ctx, stmt, 0, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})
		// Capture and process rows
		return setter.Capture(func(rows *sql.Rows) error {
			record := make(types.Record)
			err := jdbc.MapScan(rows, record, m.dataTypeConverter)
			if err != nil {
				return fmt.Errorf("failed to scan record data as map: %s", err)
			}
			return OnMessage(record)
		})
	})
}

func (m *MySQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	approxRowCountQuery := jdbc.MySQLTableRowsQuery()
	err := m.client.QueryRow(approxRowCountQuery, stream.Name()).Scan(&approxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
	}
	pool.AddRecordsToSync(approxRowCount)

	parsedFilter, err := m.getParsedFilter(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter: %s", err)
	}

	chunks := types.NewSet[types.Chunk]()
	err = jdbc.WithIsolation(ctx, m.client, func(tx *sql.Tx) error {
		// Get primary key column using the provided function
		pkColumn := stream.GetStream().SourceDefinedPrimaryKey.Array()[0]
		// Get table extremes
		minVal, maxVal, err := m.getTableExtremes(stream, pkColumn, tx, parsedFilter)
		if err != nil {
			return err
		}
		if minVal == nil {
			return nil
		}
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(minVal),
		})

		logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

		// Calculate optimal chunk size based on table statistics
		chunkSize, err := m.calculateChunkSize(stream)
		if err != nil {
			return fmt.Errorf("failed to calculate chunk size: %s", err)
		}

		// Generate chunks based on range
		query := jdbc.NextChunkEndQuery(stream, pkColumn, chunkSize, parsedFilter)

		currentVal := minVal
		for {
			var nextValRaw interface{}
			err := tx.QueryRow(query, currentVal).Scan(&nextValRaw)
			if err != nil && err == sql.ErrNoRows || nextValRaw == nil {
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
	return chunks, err
}

func (m *MySQL) getTableExtremes(stream types.StreamInterface, pkColumn string, tx *sql.Tx, parsedFilter string) (min, max any, err error) {
	query := jdbc.MinMaxQuery(stream, pkColumn)
	if parsedFilter != "" {
		query = fmt.Sprintf("%s WHERE %s", query, parsedFilter)
	}
	err = tx.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return "", "", err
	}
	return min, max, err
}
func (m *MySQL) calculateChunkSize(stream types.StreamInterface) (int, error) {
	var totalRecords int
	query := jdbc.MySQLTableRowsQuery()
	err := m.client.QueryRow(query, stream.Name()).Scan(&totalRecords)
	if err != nil {
		return 0, fmt.Errorf("failed to get estimated records count: %s", err)
	}
	// number of chunks based on max threads
	return totalRecords / (m.config.MaxThreads * 8), nil
}

func (m *MySQL) getParsedFilter(stream types.StreamInterface) (string, error) {
	filter := stream.Self().StreamMetadata.Filter
	if filter == "" {
		return "", nil // Return an empty string if no filter
	}
	parsedFilter, err := jdbc.ParseFilter(filter, "mysql") // Use jdbc package for SQL parsing
	if err != nil {
		return "", fmt.Errorf("failed to parse filter: %s", err)
	}
	return parsedFilter, nil
}
