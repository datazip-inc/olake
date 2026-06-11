package driver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// usableBytesPerPage is an upper bound for in-row payload per 8KB page
// (IN_ROW_DATA max row size). Using the ceiling yields smaller chunks.
const usableBytesPerPage = 8060

// ChunkIterator implements snapshot iteration over MSSQL chunks.
func (m *MSSQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, onMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.MSSQL,
		Stream: stream,
		State:  m.state,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, m.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during MSSQL chunk iteration: %s", err)
	}

	keyCols := stream.GetStream().SourceDefinedPrimaryKey.Array()
	if chunkColumn := stream.Self().StreamMetadata.ChunkColumn; chunkColumn != "" {
		keyCols = []string{chunkColumn}
	}
	sort.Strings(keyCols)

	logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)

	var stmt string
	if len(keyCols) > 0 {
		stmt = jdbc.MSSQLChunkScanQuery(stream, keyCols, chunk, filter)
	} else {
		stmt = jdbc.MSSQLPhysLocChunkScanQuery(stream, chunk, filter)
	}

	logger.Debugf("Executing chunk query: %s", stmt)

	tx, err := m.client.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer tx.Rollback()

	setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	})

	return jdbc.MapScanConcurrent(setter, m.dataTypeConverter, onMessage)
}

// GetOrSplitChunks splits a table into chunks using PK seek or %%physloc%% fallback.
func (m *MSSQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var (
		approxRowCount int64
		avgRowSize     any
	)

	rowStatsQuery := jdbc.MSSQLTableRowStatsQuery()
	err := m.client.QueryRowContext(ctx, rowStatsQuery, stream.Namespace(), stream.Name()).Scan(&approxRowCount, &avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count and avg row size: %s", err)
	}

	if approxRowCount == 0 {
		var hasRows bool
		existsQuery := jdbc.MSSQLTableExistsQuery(stream)
		err := m.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)
		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run UPDATE STATISTICS to update table statistics", stream.ID())
		}

		logger.Warnf("Table %s is empty, skipping chunking", stream.ID())
		return types.NewSet[types.Chunk](), nil
	}

	pool.AddRecordsToSyncStats(approxRowCount)

	// avgRowSize is returned as []uint8 which is converted to float64
	avgRowSizeFloat, err := typeutils.ReformatFloat64(avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get avg row size: %s", err)
	}
	chunkSize := int64(math.Ceil(float64(constants.EffectiveParquetSize) / avgRowSizeFloat))
	numberOfChunks := max(int64(math.Ceil(float64(approxRowCount)/float64(chunkSize))), int64(1))

	// Use chunkColumn if provided, otherwise use PK columns
	keyCols := stream.GetStream().SourceDefinedPrimaryKey.Array()
	if chunkColumn := stream.Self().StreamMetadata.ChunkColumn; chunkColumn != "" {
		keyCols = []string{chunkColumn}
	}
	sort.Strings(keyCols)

	if len(keyCols) > 0 {
		pkChunks, pkErr := m.splitViaPKSample(ctx, stream, keyCols, approxRowCount, numberOfChunks)
		if pkErr == nil && pkChunks.Len() > 0 {
			logger.Infof("Stream %s: sampling produced %d chunks", stream.ID(), pkChunks.Len())
			return pkChunks, nil
		}
		logger.Debugf("Stream %s: sampling failed (%v), falling back to iterative split", stream.ID(), pkErr)
		return m.splitViaPrimaryKey(ctx, stream, keyCols, chunkSize)
	}

	// TODO: Addition of a non physloc based strategy when no primary key is present
	logger.Warnf("Stream %s has no primary key, physloc based strategy will be used. It's advised to use default thread count to avoid overwhelming the database", stream.ID())
	if m.probeIAMWalkCapability(ctx) {
		logger.Debugf("Stream %s: no key columns, attempting IAM walk", stream.ID())
		iamChunks, iamErr := m.splitViaIAMWalk(ctx, stream)
		if iamErr == nil && iamChunks.Len() > 0 {
			return iamChunks, nil
		}
		logger.Debugf("Stream %s: IAM walk failed (%v), using iterative physloc split", stream.ID(), iamErr)
	}

	return m.splitViaPhysLoc(ctx, stream, chunkSize)
}

// physlocSortKey encodes (file_id, page_id) as a uint64 that sorts identically
// to SQL Server's byte-by-byte BINARY(8) comparison of the equivalent %%physloc%%.
// slot_id is fixed at 0xFFFF ("end of page") so chunk predicates split cleanly between pages.
// Sorting []uint64 with < is cheaper than sorting [][]byte with bytes.Compare.
func physlocSortKey(fileID, pageID int32) uint64 {
	var b [8]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(pageID))
	binary.LittleEndian.PutUint16(b[4:6], uint16(fileID))
	binary.LittleEndian.PutUint16(b[6:8], 0xFFFF)
	return binary.BigEndian.Uint64(b[:])
}

// physLocBytes converts a sort key back to the 8-byte %%physloc%% wire format
// that SQL Server understands in chunk boundary predicates.
func physLocBytes(key uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, key)
	return b
}

// splitViaPrimaryKey divides the table into chunks by walking PK boundaries.
func (m *MSSQL) splitViaPrimaryKey(ctx context.Context, stream types.StreamInterface, pkCols []string, chunkSize int64) (*types.Set[types.Chunk], error) {
	sort.Strings(pkCols)
	if len(pkCols) == 0 {
		return types.NewSet[types.Chunk](), nil
	}
	// Get the minimum and maximum values for the primary key columns
	minVal, maxVal, err := m.getTableExtremes(ctx, stream, pkCols)
	if err != nil {
		return nil, fmt.Errorf("failed to get table extremes: %s", err)
	}
	// Skip if table is empty
	if minVal == nil {
		return types.NewSet[types.Chunk](), nil
	}

	columnType := ""
	if len(pkCols) == 1 {
		columnType, err = m.getColumnTypeMSSQL(ctx, stream, pkCols[0])
		if err != nil {
			return nil, fmt.Errorf("failed to get table column type: %s", err)
		}
	}

	logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(),
		normalizeBoundaryValue(minVal, pkCols, columnType),
		normalizeBoundaryValue(maxVal, pkCols, columnType),
	)

	chunks := types.NewSet[types.Chunk]()
	// Create the first chunk from the beginning up to the minimum value
	chunks.Insert(types.Chunk{Min: nil, Max: normalizeBoundaryValue(minVal, pkCols, columnType)})

	// For composite PKs, the boundary is a comma-separated CONCAT string.
	// The nested loop reconstructs the partial-key args that MSSQLNextChunkEndQuery expects.
	query := jdbc.MSSQLNextChunkEndQuery(stream, pkCols, chunkSize)
	currentVal := minVal
	for {
		// Split the current composite key value into individual column parts
		columns := strings.Split(normalizeBoundaryValue(currentVal, pkCols, columnType), ",")

		// Build query arguments for composite key comparison
		args := make([]interface{}, 0)
		for colIdx := 0; colIdx < len(pkCols); colIdx++ {
			for partIdx := 0; partIdx <= colIdx && partIdx < len(columns); partIdx++ {
				args = append(args, strings.TrimSpace(columns[partIdx]))
			}
		}

		// Query for the next chunk boundary value
		var nextValRaw interface{}
		err := m.client.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
		// Stop if we've reached the end of the table
		if err == sql.ErrNoRows || nextValRaw == nil {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get next chunk end: %s", err)
		}
		// Create a chunk between current and next boundary
		if currentVal != nil {
			chunks.Insert(types.Chunk{
				Min: normalizeBoundaryValue(currentVal, pkCols, columnType),
				Max: normalizeBoundaryValue(nextValRaw, pkCols, columnType),
			})
		}
		currentVal = nextValRaw
	}

	// Create the final chunk from the last value to the end
	if currentVal != nil {
		chunks.Insert(types.Chunk{Min: normalizeBoundaryValue(currentVal, pkCols, columnType), Max: nil})
	}
	return chunks, nil
}

// splitViaPhysLoc iteratively walks %%physloc%% boundaries.
// Used as a last-resort fallback for heap tables with no PK and no IAM walk access.
func (m *MSSQL) splitViaPhysLoc(ctx context.Context, stream types.StreamInterface, chunkSize int64) (*types.Set[types.Chunk], error) {
	// Get the minimum and maximum physical location values
	// These define the boundaries of our table for chunking
	minVal, maxVal, err := m.getPhysLocExtremes(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to get %%physloc%% extremes: %s", err)
	}
	// Skip if table is empty (no rows to chunk)
	if minVal == nil || maxVal == nil {
		return types.NewSet[types.Chunk](), nil
	}

	// Start from the minimum physloc value
	chunks := types.NewSet[types.Chunk]()
	current := minVal
	chunks.Insert(types.Chunk{Min: nil, Max: utils.HexEncode(minVal)})

	query := jdbc.MSSQLPhysLocNextChunkEndQuery(stream, chunkSize)
	// Iteratively find chunk boundaries until we reach the end of the table
	for {
		var next []byte
		err := m.client.QueryRowContext(ctx, query, current).Scan(&next)
		// End of table reached: no more rows with physloc > current
		if err == sql.ErrNoRows || next == nil || bytes.Equal(current, next) {
			chunks.Insert(types.Chunk{Min: utils.HexEncode(current), Max: nil})
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get next %%physloc%% chunk end: %s", err)
		}
		chunks.Insert(types.Chunk{Min: utils.HexEncode(current), Max: utils.HexEncode(next)})
		current = next
	}
	return chunks, nil
}

// splitViaPKSample estimates chunk boundaries by TABLESAMPLE-ing PK column values.
// It reads a small percentage of rows (no full table scan), sorts the sampled
// PK values, and picks evenly-spaced boundaries in Go.
func (m *MSSQL) splitViaPKSample(ctx context.Context, stream types.StreamInterface, pkCols []string, approxRowCount, numberOfChunks int64) (*types.Set[types.Chunk], error) {
	samplePercent := utils.ComputeSamplePercent(approxRowCount, numberOfChunks)

	logger.Debugf("TABLESAMPLE PK sampling %.4f%% of rows from [%s.%s] for chunk boundaries (approxRows=%d, chunks=%d)",
		samplePercent, stream.Namespace(), stream.Name(), approxRowCount, numberOfChunks)

	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLPKSampleBoundaryQuery(stream, pkCols, samplePercent))
	if err != nil {
		return nil, fmt.Errorf("PK TABLESAMPLE query failed: %s", err)
	}
	defer rows.Close()

	// Resolve the database type of the first (or only) PK column from the
	// result-set metadata so normalizeBoundaryValue can format time/UUID/etc.
	// correctly. For composite PKs the CONCAT result is VARCHAR and no
	// type-specific formatting is needed (normalizeBoundaryValue falls through
	// to ConvertToString for len(pkCols) > 1 anyway).
	columnType := ""
	if len(pkCols) == 1 {
		if colTypes, ctErr := rows.ColumnTypes(); ctErr == nil && len(colTypes) > 0 {
			columnType = strings.ToLower(colTypes[0].DatabaseTypeName())
		}
	}

	var samples []string
	for rows.Next() {
		var val any
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("failed to scan PK sample: %s", err)
		}
		samples = append(samples, normalizeBoundaryValue(val, pkCols, columnType))
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate PK samples: %s", err)
	}

	if int64(len(samples)) < numberOfChunks {
		return nil, fmt.Errorf("PK TABLESAMPLE returned %d rows, need at least %d",
			len(samples), numberOfChunks)
	}

	chunks := types.NewSet[types.Chunk]()
	step := float64(len(samples)) / float64(numberOfChunks)
	var prev any = nil
	for i := int64(0); i < numberOfChunks; i++ {
		idx := min(int(float64(i)*step), len(samples)-1)
		curr := samples[idx]
		chunks.Insert(types.Chunk{Min: prev, Max: curr})
		prev = curr
	}
	chunks.Insert(types.Chunk{Min: prev, Max: nil})

	return chunks, nil
}

// splitViaIAMWalk plans chunks for any heap or clustered table by reading
// only the table's Index Allocation Map pages via
// sys.dm_db_database_page_allocations.
func (m *MSSQL) splitViaIAMWalk(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var objectID int64
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLObjectIDQuery(), stream.Namespace(), stream.Name()).Scan(&objectID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve object_id for IAM walk: %s", err)
	}

	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLIAMWalkQuery(), objectID)
	if err != nil {
		return nil, fmt.Errorf("failed to run IAM walk query: %s", err)
	}
	defer rows.Close()

	pages := make([]uint64, 0, 1024)
	for rows.Next() {
		var fileID, pageID int32
		if err := rows.Scan(&fileID, &pageID); err != nil {
			return nil, fmt.Errorf("failed to scan IAM walk page: %s", err)
		}
		pages = append(pages, physlocSortKey(fileID, pageID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate IAM walk rows: %s", err)
	}

	total := int64(len(pages))
	if total == 0 {
		return nil, fmt.Errorf("IAM walk returned no allocated pages")
	}

	// Sort defensively — the DMF does not guarantee any output order.
	slices.Sort(pages)

	pagesPerChunk := max(constants.EffectiveParquetSize/usableBytesPerPage, 1)

	// Emit one open→closed chunk every pagesPerChunk pages. The trailing chunk
	// is open-ended. If the table fits in one chunk this produces just {nil, nil}.
	chunks := types.NewSet[types.Chunk]()
	var prev any = nil
	for i := pagesPerChunk; i < total; i += pagesPerChunk {
		boundary := utils.HexEncode(physLocBytes(pages[i]))
		chunks.Insert(types.Chunk{Min: prev, Max: boundary})
		prev = boundary
	}
	chunks.Insert(types.Chunk{Min: prev, Max: nil})

	return chunks, nil
}

// probeIAMWalkCapability checks whether IAM walk can run on this server/login.
// It is cheap (two round-trips) so we call it per stream instead of caching.
func (m *MSSQL) probeIAMWalkCapability(ctx context.Context) bool {
	var majorVersion, engineEdition int
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLIAMWalkServerPropertiesQuery()).Scan(&majorVersion, &engineEdition)
	if err != nil {
		logger.Debugf("IAM walk probe: failed to read server properties: %s", err)
		return false
	}
	// SQL Server 2012 (version 11) is the first version to support IAM walk.
	if majorVersion < 11 {
		logger.Debugf("IAM walk probe: SQL Server major version %d < 11, IAM walk unsupported", majorVersion)
		return false
	}
	// EngineEdition 5 = Azure SQL Database, 8 = Azure SQL Managed Instance.
	// sys.dm_db_database_page_allocations is blocked on both.
	if engineEdition == 5 || engineEdition == 8 {
		logger.Debugf("IAM walk probe: EngineEdition %d (Azure SQL DB/MI) blocks the DMF", engineEdition)
		return false
	}

	// Permission probe: TOP 0 evaluates the DMF without returning any rows.
	// Failure here means the current login lacks VIEW DATABASE STATE.
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLIAMWalkPermissionQuery())
	if err != nil {
		logger.Debugf("IAM walk probe: permission test failed (likely missing VIEW DATABASE STATE): %s", err)
		return false
	}
	rows.Close()
	return true
}

// getColumnTypeMSSQL returns SQL data type for the requested column.
func (m *MSSQL) getColumnTypeMSSQL(ctx context.Context, stream types.StreamInterface, column string) (string, error) {
	var dataType string
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLColumnTypeQuery(), stream.Namespace(), stream.Name(), column).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return dataType, nil
}

// normalizeBoundaryValue converts key boundary values into a stable SQL-safe string form
// before storing them in chunk state and reusing them as parameters for next-boundary queries.
func normalizeBoundaryValue(value any, pkCols []string, columnType string) string {
	// Typed normalization is only for single-key chunking.
	// Composite keys are already materialized as a single CONCAT string.
	if len(pkCols) != 1 {
		return utils.ConvertToString(value)
	}

	columnType = strings.ToLower(columnType)

	switch v := value.(type) {
	case time.Time:
		// SQL Server datetime types are timezone-naive, but Go scans them as time.Time with location.
		// We normalize to SQL Server-compatible literal formats so chunk boundaries round-trip
		// safely through state serialization and remain stable for subsequent boundary comparisons.
		switch columnType {
		case "date":
			return v.Format("2006-01-02")
		case "time":
			return v.Format("15:04:05.9999999")
		case "datetime", "datetime2", "smalldatetime":
			return v.Format("2006-01-02 15:04:05.9999999")
		}
	case []byte:
		switch columnType {
		case "uniqueidentifier":
			if uuid, converted := formatUniqueIdentifierBytes(v); converted {
				return uuid
			}
		case "numeric", "decimal", "money", "smallmoney":
			return string(v)
		default:
			// For non-UUID byte values, encode as hex string to avoid corruption.
			return utils.HexEncode(v)
		}
	}
	return utils.ConvertToString(value)
}

// getTableExtremes returns MIN and MAX key values for the given PK columns.
func (m *MSSQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string) (min, max any, err error) {
	query := jdbc.MinMaxQueryMSSQL(stream, pkColumns)
	err = m.client.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// getPhysLocExtremes returns MIN and MAX %%physloc%% values for the table.
func (m *MSSQL) getPhysLocExtremes(ctx context.Context, stream types.StreamInterface) (min, max []byte, err error) {
	query := jdbc.MSSQLPhysLocExtremesQuery(stream)
	err = m.client.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// formatUniqueIdentifierBytes converts SQL Server's mixed-endian UNIQUEIDENTIFIER
// byte layout to canonical RFC4122 UUID string representation.
func formatUniqueIdentifierBytes(v []byte) (string, bool) {
	if len(v) != 16 {
		return "", false
	}

	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		v[3], v[2], v[1], v[0], // first 4 bytes (little-endian)
		v[5], v[4], // next 2 bytes (little-endian)
		v[7], v[6], // next 2 bytes (little-endian)
		v[8], v[9], // next 2 bytes (big-endian)
		v[10], v[11], v[12], v[13], v[14], v[15], // last 6 bytes (big-endian)
	), true
}
