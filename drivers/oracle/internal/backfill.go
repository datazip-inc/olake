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
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// ChunkIterator implements the abstract.DriverInterface
func (o *Oracle) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.Oracle,
		Stream: stream,
		State:  o.state,
		Client: o.client,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, o.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	tx, err := o.client.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer tx.Rollback()

	logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)

	stmt, err := jdbc.OracleChunkScanQuery(stream, chunk, filter)
	if err != nil {
		return fmt.Errorf("failed to build chunk scan query: %s", err)
	}
	setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
		// TODO: Add support for user defined datatypes in OracleDB
		return tx.QueryContext(ctx, query, args...)
	})

	return jdbc.MapScanConcurrent(setter, o.dataTypeConverter, OnMessage)
}

func (o *Oracle) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// Get approximate row count from Oracle statistics for progress tracking
	var approxRowCount int64
	var avgRowSize float64
	approxRowStatsQuery := jdbc.OracleTableRowStatsQuery()
	err := o.client.QueryRowContext(ctx, approxRowStatsQuery, stream.Namespace(), stream.Name()).Scan(&approxRowCount, &avgRowSize)
	if err != nil {
		logger.Debugf("Table statistics not available for %s.%s, progress tracking disabled. Run DBMS_STATS.GATHER_TABLE_STATS to enable.", stream.Namespace(), stream.Name())
	}
	// If the oracle table stats are outdated we can get avg row size as 0 so we are assuming 300 bytes
	avgRowSize = utils.Ternary(avgRowSize > 0, avgRowSize, 300).(float64)
	pool.AddRecordsToSyncStats(approxRowCount)

	query := jdbc.OracleEmptyCheckQuery(stream)
	err = o.client.QueryRowContext(ctx, query).Scan(new(interface{}))
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Warnf("Table %s.%s is empty, skipping chunking", stream.Namespace(), stream.Name())
			return types.NewSet[types.Chunk](), nil
		}
		return nil, fmt.Errorf("failed to check for rows: %s", err)
	}

	chunks, err := o.splitViaRowId(ctx, stream)
	// If the fast RowID strategy fails for ANY reason (Read-Only, Permissions, etc.) fallback to Table Iteration strategy
	if err != nil {
		logger.Debugf("DBMS Parallel Execute strategy failed (%v). Automatically falling back to Table Iteration strategy for stream [%s.%s]", err, stream.Namespace(), stream.Name())

		return o.splitViaTableIteration(ctx, stream, avgRowSize)
	}
	return chunks, nil
}

// TODO: Make this function more efficient by using a single query to get all the rowids in the table using one query
func (o *Oracle) splitViaTableIteration(ctx context.Context, stream types.StreamInterface, avgRowSize float64) (*types.Set[types.Chunk], error) {
	chunks := types.NewSet[types.Chunk]()

	var minRowId, maxRowId string
	query := jdbc.OracleMinMaxRowIDQuery(stream)
	err := o.client.QueryRowContext(ctx, query).Scan(&minRowId, &maxRowId)
	if err != nil {
		return nil, fmt.Errorf("failed to get min-max row id: %s", err)
	}
	rowsPerChunk := int64(math.Ceil(float64(constants.EffectiveParquetSize) / avgRowSize))

	currRowId := minRowId
	chunks.Insert(types.Chunk{
		Min: nil,
		Max: currRowId,
	})

	for {
		var nextRowId string
		var rowCount int64

		nextRowIdQuery := jdbc.NextRowIDQuery(stream, currRowId, rowsPerChunk)
		err = o.client.QueryRowContext(ctx, nextRowIdQuery).Scan(&nextRowId, &rowCount)
		if err != nil {
			return nil, fmt.Errorf("failed to get next row id: %s", err)
		}

		if rowCount < rowsPerChunk || nextRowId == maxRowId {
			chunks.Insert(types.Chunk{
				Min: currRowId,
				Max: nil,
			})
			break
		}

		chunks.Insert(types.Chunk{
			Min: currRowId,
			Max: nextRowId,
		})

		currRowId = nextRowId
	}

	return chunks, nil
}

// splitViaRowId chunks a table by using DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID strategy
func (o *Oracle) splitViaRowId(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	logger.Debugf("Chunking via DBMS_PARALLEL_EXECUTE for %s.%s", stream.Namespace(), stream.Name())
	query := jdbc.OracleBlockSizeQuery()
	var blockSize int64
	err := o.client.QueryRowContext(ctx, query).Scan(&blockSize)
	if err != nil || blockSize == 0 {
		logger.Warnf("failed to get block size from query, switching to default block size value 8192")
		blockSize = 8192
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
		err = rows.Scan(&chunkID, &startRowID, &endRowID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan chunk %d: %s", chunkID, err)
		}
		startRowIDs = append(startRowIDs, startRowID)
	}

	chunks.Insert(types.Chunk{
		Min: nil,
		Max: startRowIDs[0],
	})

	for idx, startRowID := range startRowIDs {
		var maxRowID interface{}

		if idx < len(startRowIDs)-1 {
			maxRowID = startRowIDs[idx+1]
		} else {
			maxRowID = nil
		}

		chunks.Insert(types.Chunk{
			Min: startRowID,
			Max: maxRowID,
		})
	}

	return chunks, rows.Err()
}

// TODO: Add support for oracle extents based chunking strategy
/*
splitViaExtents manually chunks a table by using extents of the table in OracleDB
func (o *Oracle) splitViaExtents(ctx context.Context, stream types.StreamInterface, blocksPerChunk int64) (*types.Set[types.Chunk], error) {
	chunks := types.NewSet[types.Chunk]()

	// 1. Get the Table's Data Object ID, Fetch all physical extents for the table
	rows, err := o.client.QueryContext(ctx, jdbc.OracleExtentsQuery(), stream.Namespace(), stream.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch extents: %s", err)
	}
	defer rows.Close()

	var (
		startRowIDs   []string
		currentBlocks int64 = 0
		isFirstChunk        = true
	)

	// 2. Iterate through extents and group them into chunks
	for rows.Next() {
		var fileID, blockID, blocks, objectID int64
		err = rows.Scan(&fileID, &blockID, &blocks, &objectID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan extents: %s", err)
		}

		// If this is the start of a new chunk, generate its starting ROWID
		if isFirstChunk || currentBlocks == 0 {
			var startRowID string
			// Uses DBMS_ROWID.ROWID_CREATE to convert file/block to ROWID
			err = o.client.QueryRowContext(ctx, jdbc.OracleRowIDCreateQuery(), objectID, fileID, blockID).Scan(&startRowID)
			if err != nil {
				return nil, err
			}
			startRowIDs = append(startRowIDs, startRowID)
			isFirstChunk = false
		}

		currentBlocks += blocks

		// If we hit our block limit, reset the counter so the next loop starts a new chunk
		if currentBlocks >= blocksPerChunk {
			currentBlocks = 0
		}
	}

	chunks.Insert(types.Chunk{
		Min: nil,
		Max: startRowIDs[0],
	})

	// 3. Format the startRowIDs into the Min/Max Chunk structure
	for idx, startRowID := range startRowIDs {
		var maxRowID interface{}
		if idx < len(startRowIDs)-1 {
			maxRowID = startRowIDs[idx+1]
		} else {
			maxRowID = nil
		}

		chunks.Insert(types.Chunk{
			Min: startRowID,
			Max: maxRowID,
		})
	}

	return chunks, nil
}
*/
