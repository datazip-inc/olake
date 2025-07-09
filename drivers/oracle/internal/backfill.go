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
	"github.com/datazip-inc/olake/utils/logger"
)

// ChunkIterator implements the abstract.DriverInterface
func (o *Oracle) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) (any, error) {
	//TODO: Verify the requirement of Transaction in Oracle Sync and remove if not required
	// Begin transaction with default isolation
	tx, err := o.client.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer tx.Rollback()

	stmt := jdbc.OracleChunkScanQuery(stream, chunk)
	// Use transaction for queries
	setter := jdbc.NewReader(ctx, stmt, 0, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		// TODO: Add support for user defined datatypes in OracleDB
		return tx.QueryContext(ctx, query)
	})

	err = setter.Capture(func(rows *sql.Rows) error {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, o.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}
		return OnMessage(record)
	})
	return nil, err
}

func (o *Oracle) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	splitViaRowId := func(stream types.StreamInterface) (*types.Set[types.Chunk], error) {
		var currentSCN string
		query := jdbc.OracleCurrentSCNQuery()
		err := o.client.QueryRow(query).Scan(&currentSCN)
		if err != nil {
			return nil, fmt.Errorf("failed to get current SCN: %s", err)
		}

		query = jdbc.OracleEmptyCheckQuery(stream)
		err = o.client.QueryRow(query).Scan(new(interface{}))
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Warnf("Table %s.%s is empty skipping chunking", stream.Namespace(), stream.Name())
				return types.NewSet[types.Chunk](), nil
			}
			return nil, fmt.Errorf("failed to check for rows: %s", err)
		}

		var minRowId, maxRowId string
		var totalRows int64
		query = jdbc.OracleMinMaxCountQuery(stream, currentSCN)
		err = o.client.QueryRow(query).Scan(&minRowId, &maxRowId, &totalRows)
		if err != nil {
			return nil, fmt.Errorf("failed to get min-max row id and total rows: %s", err)
		}

		chunks := types.NewSet[types.Chunk]()
		currRowId := minRowId
		rowsPerChunk, err := o.getChunkSize(stream, totalRows)
		if err != nil {
			return nil, fmt.Errorf("failed to get chunk size: %s", err)
		}

		for {
			// TODO: Remove use of count of all rows in chunk
			nextRowIdQuery := jdbc.NextRowIDQuery(stream, currentSCN, currRowId, rowsPerChunk)
			var nextRowId string
			var rowCount int64
			err = o.client.QueryRow(nextRowIdQuery).Scan(&nextRowId, &rowCount)
			if err != nil {
				return nil, fmt.Errorf("failed to get next row id: %s", err)
			}
			// Appending the SCN to chunk boundaries, this will be used during chunk itearation
			if (rowCount < rowsPerChunk) || (nextRowId == maxRowId) {
				chunks.Insert(types.Chunk{
					Min: currentSCN + "," + currRowId,
					Max: nil,
				})
				break
			}
			chunks.Insert(types.Chunk{
				Min: currentSCN + "," + currRowId,
				Max: currentSCN + "," + nextRowId,
			})
			currRowId = nextRowId
		}
		return chunks, nil
	}
	return splitViaRowId(stream)
}

func (o *Oracle) getChunkSize(stream types.StreamInterface, totalRows int64) (int64, error) {
	query := jdbc.OracleTableSizeQuery(stream)
	var totalTableSize int64
	err := o.client.QueryRow(query).Scan(&totalTableSize)
	if err != nil {
		return 0, fmt.Errorf("failed to get total size of table: %s", err)
	}

	avgRowSize := math.Ceil(float64(totalTableSize) / float64(totalRows))
	rowsPerParquet := int64(math.Ceil(float64(constants.EffectiveParquetSize) / float64(avgRowSize)))

	return rowsPerParquet, nil
}
