package driver

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// ChunkIterator implements the abstract.DriverInterface
func (o *Oracle) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	//TODO: Verify the requirement of Transaction in Oracle Sync and remove if not required
	// Begin transaction with default isolation
	filter, err := jdbc.SQLFilter(stream, o.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	tx, err := o.client.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer tx.Rollback()

	stmt := jdbc.OracleChunkScanQuery(stream, chunk, filter)
	// Use transaction for queries
	setter := jdbc.NewReader(ctx, stmt, 0, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		// TODO: Add support for user defined datatypes in OracleDB
		return tx.QueryContext(ctx, query)
	})

	return setter.Capture(func(rows *sql.Rows) error {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, o.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}
		return OnMessage(record)
	})
}

func (o *Oracle) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	splitViaRowId := func(stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		var currentSCN string
		query := jdbc.OracleCurrentSCNQuery()
		err := o.client.QueryRow(query).Scan(&currentSCN)
		if err != nil {
			return nil, fmt.Errorf("failed to get current SCN: %s", err)
		}

		// TODO: Add implementation of AddRecordsToSync function which expects total number of records to be synced
		query = jdbc.OracleEmptyCheckQuery(stream)
		err = o.client.QueryRow(query).Scan(new(interface{}))
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Warnf("Table %s.%s is empty skipping chunking", stream.Namespace(), stream.Name())
				return types.NewSet[types.Chunk](), nil
			}
			return nil, fmt.Errorf("failed to check for rows: %s", err)
		}

		query = jdbc.OracleBlockSizeQuery()
		var blockSize int64
		err = o.client.QueryRow(query).Scan(&blockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to get block size: %s", err)
		}
		blocksPerChunk := int64(math.Ceil(float64(constants.EffectiveParquetSize) / float64(blockSize)))

		taskName := fmt.Sprintf("chunk_%s_%s_%s", stream.Namespace(), stream.Name(), time.Now().Format("20060102150405.000000"))
		query = jdbc.OracleTaskCreationQuery(taskName)
		_, err = o.client.ExecContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("failed to create task: %s", err)
		}
		defer func(taskName string) {
			stmt := jdbc.OracleChunkTaskCleanerQuery(taskName)
			_, err := o.client.ExecContext(ctx, stmt)
			if err != nil {
				logger.Warnf("failed to clean up chunk task: %s", err)
			}
		}(taskName)

		// TODO: Research about filteration during chunk creation and CREATE_CHUNKS_BY_SQL strategy
		query = jdbc.OracleChunkCreationQuery(stream, blocksPerChunk, taskName)
		_, err = o.client.ExecContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunks: %s", err)
		}

		chunks := types.NewSet[types.Chunk]()
		chunkQuery := jdbc.OracleChunkRetrievalQuery(taskName)
		rows, err := o.client.QueryContext(ctx, chunkQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve chunks: %s", err)
		}
		defer rows.Close()

		// Collect all start rowids first
		var startRowIDs []string
		for rows.Next() {
			var chunkID int
			var startRowID, endRowID string
			err := rows.Scan(&chunkID, &startRowID, &endRowID)
			if err != nil {
				return nil, fmt.Errorf("failed to scan chunk %d: %s", chunkID, err)
			}
			startRowIDs = append(startRowIDs, startRowID)
		}

		for idx, startRowID := range startRowIDs {
			var maxRowID interface{}

			if idx < len(startRowIDs)-1 {
				maxRowID = startRowIDs[idx+1]
			} else {
				maxRowID = nil
			}

			chunks.Insert(types.Chunk{
				Min: fmt.Sprintf("%s,%s", currentSCN, startRowID),
				Max: maxRowID,
			})
		}

		return chunks, rows.Err()
	}
	return splitViaRowId(stream)
}
