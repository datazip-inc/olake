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
)

// DB2 TABLESAMPLE SYSTEM clamp bounds. DB2 allows (0, 100) exclusive; 0.01 is the practical
// floor to ensure at least some pages are sampled. 50 caps worst-case I/O on huge tables.
const (
	db2SampleSystemPercentMin = 0.01
	db2SampleSystemPercentMax = 50.0
	// db2SampleRowsPerChunkMultiplier gives each chunk boundary ~10x sampled RIDs to pick from,
	// producing evenly-spaced boundaries even when page sampling is clustered.
	db2SampleRowsPerChunkMultiplier int64 = 10
	// db2DefaultAvgRowSize is used when SYSCAT.TABLES.AVGROWSIZE is 0 (stale stats).
	db2DefaultAvgRowSize = 300
)

// ChunkIterator implements snapshot iteration over a DB2 chunk.
// Dispatch is type-based on chunk.Min: string values come from PK-based chunking,
// int64/nil values come from TABLESAMPLE or RID-interval chunking.
func (d *DB2) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.DB2,
		Stream: stream,
		State:  d.state,
		Client: d.client,
	}

	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, d.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
	sort.Strings(pkColumns)
	chunkColumn := stream.Self().StreamMetadata.ChunkColumn

	var stmt string
	if isStringChunk(chunk) {
		// PK-based chunk: Min/Max are string PK values
		if chunkColumn != "" {
			stmt = jdbc.DB2PKChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt = jdbc.DB2PKChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			// Guard: should not happen — PK chunking only runs when PK exists
			stmt = jdbc.DB2RidChunkScanQuery(stream, chunk, filter)
		}
	} else {
		// TABLESAMPLE or RID-interval chunk: Min/Max are int64 RID values
		stmt = jdbc.DB2RidChunkScanQuery(stream, chunk, filter)
	}

	// begin transaction for chunk iteration (DB2 driver defaults to cursor stability / read committed)
	tx, err := d.client.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer tx.Rollback()

	logger.Debugf("Starting backfill for %s with chunk %v using query: %s", stream.ID(), chunk, stmt)

	setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	})

	return jdbc.MapScanConcurrent(setter, d.dataTypeConverter, OnMessage)
}

// isStringChunk reports whether a chunk was produced by PK-based chunking (boundaries are
// string PK values) rather than RID-based chunking (boundaries are int64 RID values).
func isStringChunk(chunk types.Chunk) bool {
	if chunk.Min != nil {
		_, ok := chunk.Min.(string)
		return ok
	}
	if chunk.Max != nil {
		_, ok := chunk.Max.(string)
		return ok
	}
	return false
}

// GetOrSplitChunks verifies the table is non-empty, fetches page metadata, and delegates to
// splitTableIntoChunks which implements the full three-tier chunking strategy.
func (d *DB2) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// Use SYSCAT.TABLES.CARD for approximate row count (progress tracking + empty-table check).
	var approxRowCount int64
	rowCountQuery := jdbc.DB2ApproxRowCountQuery(stream)
	if err := d.client.QueryRowContext(ctx, rowCountQuery).Scan(&approxRowCount); err != nil {
		return nil, fmt.Errorf("failed to get approx row count: %s", err)
	}
	if approxRowCount == -1 || approxRowCount == 0 {
		var hasRows bool
		existsQuery := jdbc.DB2TableStatsExistQuery(stream)
		err := d.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)
		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run CLP command:\tRUNSTATS ON TABLE %s.%s AND INDEXES ALL;\t to update table statistics", stream.ID(), stream.Namespace(), stream.Name())
		}

		logger.Warnf("Table %s is empty, skipping chunking", stream.ID())
		return types.NewSet[types.Chunk](), nil
	}
	pool.AddRecordsToSyncStats(approxRowCount)

	// Fetch page metadata needed by all chunking tiers.
	// approxRowCount (CARD) is passed in so getPageStats can estimate nPages without
	// an extra round-trip when NPAGES is stale.
	pageSize, nPages, err := d.getPageStats(ctx, stream, approxRowCount)
	if err != nil {
		return nil, err
	}

	return d.splitTableIntoChunks(ctx, stream, pageSize, nPages)
}

// getPageStats fetches PAGESIZE and NPAGES for chunk planning.
// card is the already-fetched SYSCAT.TABLES.CARD value (approxRowCount from GetOrSplitChunks).
//
// When NPAGES is stale/zero, nPages is estimated as ceil(card * avgRowSize / pageSize)
// using AVGROWSIZE from SYSCAT.TABLES — no extra round-trip for CARD, no table scan.
func (d *DB2) getPageStats(ctx context.Context, stream types.StreamInterface, card int64) (pageSize, nPages int64, err error) {
	if err = d.client.QueryRowContext(ctx, jdbc.DB2PageStatsQuery(stream)).Scan(&pageSize, &nPages); err != nil {
		return 0, 0, fmt.Errorf("failed to get page stats from SYSCAT: %s", err)
	}

	if pageSize <= 0 {
		pageSize = 4096 // DB2 minimum page size; avoids division by zero
	}

	if nPages > 0 {
		return pageSize, nPages, nil
	}

	// NPAGES is 0 or -1 — stale stats. Use the already-fetched CARD + AVGROWSIZE to estimate.
	logger.Debugf("SYSCAT.TABLES.NPAGES is %d (stale) for [%s.%s], estimating from CARD+AVGROWSIZE",
		nPages, stream.Namespace(), stream.Name())

	var avgRowSize int64
	if err = d.client.QueryRowContext(ctx, jdbc.DB2AvgRowSizeQuery(stream)).Scan(&avgRowSize); err != nil {
		return 0, 0, fmt.Errorf("failed to read AVGROWSIZE from SYSCAT for [%s.%s]: %s", stream.Namespace(), stream.Name(), err)
	}

	if avgRowSize <= 0 {
		avgRowSize = db2DefaultAvgRowSize
	}
	nPages = int64(math.Ceil(float64(card*avgRowSize) / float64(pageSize)))
	if nPages <= 0 {
		nPages = 1
	}
	logger.Debugf("SYSCAT CARD estimate: nPages=%d for [%s.%s] (card=%d, avgRowSize=%d, pageSize=%d)",
		nPages, stream.Namespace(), stream.Name(), card, avgRowSize, pageSize)

	return pageSize, nPages, nil
}

// splitTableIntoChunks implements the three-tier chunking strategy:
//
//	Tier 1 (primary)   — TABLESAMPLE SYSTEM: samples n% of physica`l pages, picks evenly-spaced
//	                      RID boundaries. Equivalent to Oracle's SAMPLE BLOCK fallback.
//	Tier 2 (fallback)  — PK-based: iterative seek on primary key / chunk column.
//	                      Only runs when a PK or chunk column is configured.
//	Tier 3 (final)     — RID interval: full-scan MIN/MAX RID + equal integer intervals.
//	                      The original approach; kept as a universal safety net.
func (d *DB2) splitTableIntoChunks(ctx context.Context, stream types.StreamInterface, pageSize, nPages int64) (*types.Set[types.Chunk], error) {
	pagesPerChunk := int64(math.Ceil(float64(constants.EffectiveParquetSize) / float64(pageSize)))
	pagesPerChunk = utils.Ternary(pagesPerChunk <= 0, int64(1), pagesPerChunk).(int64)

	safeNPages := utils.Ternary(nPages <= 0, int64(1), nPages).(int64)
	numberOfChunks := int64(math.Ceil(float64(safeNPages) / float64(pagesPerChunk)))
	numberOfChunks = utils.Ternary(numberOfChunks <= 0, int64(1), numberOfChunks).(int64)

	// Tier 1: TABLESAMPLE SYSTEM
	chunks, err := d.splitViaTablesample(ctx, stream, safeNPages, numberOfChunks)
	if err == nil {
		return chunks, nil
	}
	logger.Debugf("TABLESAMPLE SYSTEM strategy failed for [%s.%s]: %v. Trying PK chunking.", stream.Namespace(), stream.Name(), err)

	// Tier 2: PK-based chunking
	hasPK := stream.GetStream().SourceDefinedPrimaryKey.Len() > 0
	hasChunkCol := stream.Self().StreamMetadata.ChunkColumn != ""
	if hasPK || hasChunkCol {
		chunks, err = d.splitViaPrimaryKey(ctx, stream)
		if err == nil {
			return chunks, nil
		}
		logger.Debugf("PK chunking failed for [%s.%s]: %v. Falling back to RID interval.", stream.Namespace(), stream.Name(), err)
	}

	// Tier 3: RID interval (original behavior)
	return d.splitViaRIDInterval(ctx, stream, pageSize, nPages)
}

// splitViaTablesample samples n% of physical pages using TABLESAMPLE SYSTEM and builds evenly-spaced
// RID chunk boundaries from the sampled rows. This mirrors Oracle's SAMPLE BLOCK approach:
// only a fraction of pages are read at planning time, making it safe for multi-billion-row tables.
//
// Requires DB2 LUW 9.7+. Returns an error on unsupported platforms so the caller can fall back.
func (d *DB2) splitViaTablesample(ctx context.Context, stream types.StreamInterface, nPages, numberOfChunks int64) (*types.Set[types.Chunk], error) {
	minSampleRows := numberOfChunks * db2SampleRowsPerChunkMultiplier
	samplePercent := float64(minSampleRows) / float64(nPages) * 100.0
	samplePercent = math.Max(db2SampleSystemPercentMin, math.Min(db2SampleSystemPercentMax, samplePercent))

	logger.Debugf("Sampling %.4f%% of pages from [%s.%s] for chunk boundaries (nPages=%d, chunks=%d)",
		samplePercent, stream.Namespace(), stream.Name(), nPages, numberOfChunks)

	rows, err := d.client.QueryContext(ctx, jdbc.DB2TablesampleRidQuery(stream, samplePercent))
	if err != nil {
		return nil, fmt.Errorf("TABLESAMPLE SYSTEM query failed: %w", err)
	}
	defer rows.Close()

	var sampledRIDs []int64
	for rows.Next() {
		var rid int64
		if err := rows.Scan(&rid); err != nil {
			return nil, fmt.Errorf("failed to scan sampled RID: %w", err)
		}
		sampledRIDs = append(sampledRIDs, rid)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate sampled RIDs: %w", err)
	}

	if int64(len(sampledRIDs)) < numberOfChunks {
		return nil, fmt.Errorf("TABLESAMPLE returned %d rows, need at least %d for %d chunks",
			len(sampledRIDs), numberOfChunks, numberOfChunks)
	}

	// Pick evenly-spaced boundaries from the already-sorted sample (ORDER BY rid in the query).
	step := float64(len(sampledRIDs)) / float64(numberOfChunks)
	startRIDs := make([]int64, 0, numberOfChunks)
	for i := int64(0); i < numberOfChunks; i++ {
		idx := int(float64(i) * step)
		startRIDs = append(startRIDs, sampledRIDs[idx])
	}

	return buildChunksFromStartRIDs(startRIDs), nil
}

// buildChunksFromStartRIDs constructs the half-open [Min, Max) chunk set from a sorted slice
// of start RID values. The first chunk has Min=nil (unbounded below) and the last has Max=nil
// (unbounded above), matching the pattern used by Oracle's buildChunksFromStartRowIDs.
func buildChunksFromStartRIDs(startRIDs []int64) *types.Set[types.Chunk] {
	chunks := types.NewSet[types.Chunk]()
	if len(startRIDs) == 0 {
		return chunks
	}

	chunks.Insert(types.Chunk{Min: nil, Max: startRIDs[0]})

	for idx, rid := range startRIDs {
		var maxRID interface{}
		if idx < len(startRIDs)-1 {
			maxRID = startRIDs[idx+1]
		}
		chunks.Insert(types.Chunk{Min: rid, Max: maxRID})
	}

	return chunks
}

// splitViaPrimaryKey chunks the table by iteratively seeking along the primary key (or chunk
// column), producing string-boundary chunks. Chunk.Min and Chunk.Max are string PK values which
// ChunkIterator routes to DB2PKChunkScanQuery.
func (d *DB2) splitViaPrimaryKey(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var avgRowSize int64
	if err := d.client.QueryRowContext(ctx, jdbc.DB2AvgRowSizeQuery(stream)).Scan(&avgRowSize); err != nil {
		logger.Debugf("Failed to get AVGROWSIZE for [%s.%s]: %v. Using default %d bytes.", stream.Namespace(), stream.Name(), err, db2DefaultAvgRowSize)
	}
	if avgRowSize <= 0 {
		avgRowSize = db2DefaultAvgRowSize
	}

	chunkSize := int64(math.Ceil(float64(constants.EffectiveParquetSize) / float64(avgRowSize)))
	chunkSize = utils.Ternary(chunkSize <= 0, int64(1), chunkSize).(int64)

	chunkColumn := stream.Self().StreamMetadata.ChunkColumn
	pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
	if chunkColumn != "" {
		pkColumns = []string{chunkColumn}
	}
	sort.Strings(pkColumns)

	minVal, maxVal, err := d.getTableExtremes(ctx, stream, pkColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to get table extremes: %s", err)
	}
	if minVal == nil {
		return types.NewSet[types.Chunk](), nil
	}

	chunks := types.NewSet[types.Chunk]()
	chunks.Insert(types.Chunk{
		Min: nil,
		Max: utils.ConvertToString(minVal),
	})

	logger.Infof("Stream %s PK extremes — min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

	query := jdbc.DB2NextChunkEndQuery(stream, pkColumns, chunkSize)
	currentVal := minVal
	for {
		columns := strings.Split(utils.ConvertToString(currentVal), ",")

		args := make([]interface{}, 0, len(pkColumns)*(len(pkColumns)+1)/2)
		for columnIndex := 0; columnIndex < len(pkColumns); columnIndex++ {
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

// splitViaRIDInterval is the original chunking approach kept as the final fallback:
// it performs a full table scan to find MIN/MAX RID values and then divides the
// integer range into equal-sized intervals. This may produce skewed chunks when the
// RID space has gaps (deleted rows, table REORG), but works on any DB2 version.
func (d *DB2) splitViaRIDInterval(ctx context.Context, stream types.StreamInterface, pageSize, nPages int64) (*types.Set[types.Chunk], error) {
	logger.Debugf("Using RID interval fallback for [%s.%s]", stream.Namespace(), stream.Name())

	var minRID, maxRID int64
	if err := d.client.QueryRowContext(ctx, jdbc.DB2MinMaxRidQuery(stream)).Scan(&minRID, &maxRID); err != nil {
		return nil, fmt.Errorf("failed to get the min and max RID: %s", err)
	}

	pagesPerChunk := int64(math.Ceil(float64(constants.EffectiveParquetSize) / float64(pageSize)))
	safeNPages := utils.Ternary(nPages <= 0, int64(1), nPages).(int64)
	numberOfChunks := int64(math.Ceil(float64(safeNPages) / float64(pagesPerChunk)))
	numberOfChunks = utils.Ternary(numberOfChunks <= 0, int64(1), numberOfChunks).(int64)

	totalRidRange := maxRID - minRID
	ridInterval := int64(math.Ceil(float64(totalRidRange) / float64(numberOfChunks)))
	ridInterval = utils.Ternary(ridInterval <= 0, int64(1), ridInterval).(int64)

	chunks := types.NewSet[types.Chunk]()
	for start := minRID; start <= maxRID; start += ridInterval {
		end := start + ridInterval
		end = utils.Ternary(end > maxRID+1, maxRID+1, end).(int64)
		chunks.Insert(types.Chunk{Min: start, Max: end})
	}

	return chunks, nil
}

func (d *DB2) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string) (min, max any, err error) {
	query := jdbc.DB2MinMaxPKQuery(stream, pkColumns)
	err = d.client.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}
