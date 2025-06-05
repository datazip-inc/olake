package driver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) (err error) {
	// Begin transaction with repeatable read isolation
	return jdbc.WithIsolation(ctx, m.client, func(tx *sql.Tx) error {
		// Build query for the chunk
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		chunkColumn := stream.Self().StreamMetadata.ChunkColumn
		// Get chunks from state or calculate new ones
		stmt := ""
		if len(pkColumns) > 0 || chunkColumn != "" {
			if chunkColumn != "" {
				stmt = jdbc.MysqlChunkScanQuery(stream, []string{chunkColumn}, chunk)
			} else {
				sort.Strings(pkColumns)
				stmt = jdbc.MysqlChunkScanQuery(stream, pkColumns, chunk)
			}
		} else {
			stmt = jdbc.MysqlLimitOffsetScanQuery(stream, chunk)
		}
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

	chunks := types.NewSet[types.Chunk]()
	chunkColumn := stream.Self().StreamMetadata.ChunkColumn

	// Takes the user defined batch size as chunkSize
	chunkSize := m.config.BatchSize

	splitViaPrimaryKey := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		return jdbc.WithIsolation(ctx, m.client, func(tx *sql.Tx) error {
			// Get primary key column using the provided function

			pkColumns := []string{chunkColumn}
			if chunkColumn == "" {
				pkColumns = stream.GetStream().SourceDefinedPrimaryKey.Array()
				sort.Strings(pkColumns)
			}
			// Get table extremes
			minVal, maxVal, err := m.getTableExtremes(stream, pkColumns, tx)
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

			// Generate chunks based on range
			query := jdbc.NextChunkEndQuery(stream, pkColumns, chunkSize)

			currentVal := minVal

			for {
				// Split the current value into parts
				parts := strings.Split(utils.ConvertToString(currentVal), ",")

				// Create args array with the correct number of arguments for the query
				args := make([]interface{}, 0)
				for i := 0; i < len(pkColumns); i++ {
					// For each column combination in the WHERE clause, we need to add the necessary parts
					for j := 0; j <= i; j++ {
						if j < len(parts) {
							args = append(args, parts[j])
						}
					}
				}

				var nextValRaw interface{}
				err := tx.QueryRow(query, args...).Scan(&nextValRaw)
				if err != nil && err == sql.ErrNoRows || nextValRaw == nil {
					break
				} else if err != nil {
					return fmt.Errorf("failed to get next chunk end: %w", err)
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

	limitOffsetChunking := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		return jdbc.WithIsolation(context.Background(), m.client, func(tx *sql.Tx) error {

			if err != nil {
				return fmt.Errorf("failed to calculate chunk size: %w", err)
			}

			query := jdbc.CalculateTotalRows(stream)
			var totalRows uint64
			logger.Infof("Query for total rows: %s", query)
			err = m.client.QueryRow(query).Scan(&totalRows)
			if err != nil {
				return fmt.Errorf("failed to calculate total Rows: %w", err)
			}

			chunks.Insert(types.Chunk{
				Min: nil,
				Max: utils.ConvertToString(chunkSize),
			})
			lastChunk := uint64(chunkSize)
			for lastChunk < totalRows {
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(lastChunk),
					Max: utils.ConvertToString(lastChunk + uint64(chunkSize)),
				})
				lastChunk += uint64(chunkSize)
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
		err = limitOffsetChunking(stream, chunks)
	}
	return chunks, err
}

func (m *MySQL) getTableExtremes(stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := jdbc.MinMaxQueryMySQL(stream, pkColumns)
	err = tx.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return "", "", err
	}
	return min, max, err
}
