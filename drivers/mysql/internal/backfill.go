package driver

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func (m *MySQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.MySQL,
		Stream: stream,
		State:  m.state,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, m.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}
	// Begin transaction with repeatable read isolation
	return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
		// Build query for the chunk
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		chunkColumn := stream.Self().StreamMetadata.ChunkColumn
		sort.Strings(pkColumns)

		logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)
		// Get chunks from state or calculate new ones
		stmt := ""
		if chunkColumn != "" {
			stmt = jdbc.MysqlChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt = jdbc.MysqlChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			stmt = jdbc.MysqlLimitOffsetScanQuery(stream, chunk, filter)
		}
		logger.Debugf("Executing chunk query: %s", stmt)
		setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})
		return jdbc.MapScanConcurrent(setter, m.dataTypeConverter, OnMessage)
	})
}

// TODO: Separate chunking-related logic from this function so the individual components can be unit tested independently.
func (m *MySQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var (
		approxRowCount      int64
		avgRowSize          any
		approxTableSize     int64
		columnCollationType string
		dataMaxLength       sql.NullInt64
	)

	tableStatsQuery := jdbc.MySQLTableStatsQuery()
	err := m.client.QueryRowContext(ctx, tableStatsQuery, stream.Name()).Scan(&approxRowCount, &avgRowSize, &approxTableSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch TableStats query for table=%s: %s", stream.Name(), err)
	}

	if approxRowCount == 0 {
		var hasRows bool
		existsQuery := jdbc.MySQLTableExistsQuery(stream)
		err := m.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)

		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run ANALYZE TABLE to update table statistics", stream.ID())
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
	chunks := types.NewSet[types.Chunk]()
	chunkColumn := stream.Self().StreamMetadata.ChunkColumn

	var (
		minVal any // table min PK
		maxVal any // table max PK
	)

	pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
	if chunkColumn != "" {
		pkColumns = []string{chunkColumn}
	}
	sort.Strings(pkColumns)

	if len(pkColumns) > 0 {
		minVal, maxVal, err = m.getTableExtremes(ctx, stream, pkColumns)
		if err != nil {
			return nil, fmt.Errorf("Stream %s: Failed to get table extremes: %s", stream.ID(), err)
		}
	}

	var numericChunkBounds *NumericChunkBounds
	var stringChunkBounds *StringChunkBounds

	if len(pkColumns) == 1 {
		var dataType string
		query := jdbc.MySQLColumnStatsQuery()
		err = m.client.QueryRowContext(ctx, query, stream.Name(), pkColumns[0]).Scan(&dataType, &dataMaxLength, &columnCollationType)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch column datatype and max length for column %s: %s", pkColumns[0], err)
		}
		// 1. Try Numeric Strategy
		numericChunkBounds = isNumericAndEvenDistributed(minVal, maxVal, approxRowCount, chunkSize, dataType)

		// 2. If not numeric, check for supported String strategy
		if numericChunkBounds == nil {
			stringChunkBounds = isStringSupportedPK(minVal, maxVal, dataMaxLength, dataType)
		}
	}

	// Takes the user defined batch size as chunkSize
	// TODO: common-out the chunking logic for db2, mssql, mysql
	splitViaPrimaryKey := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
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
				columns := strings.Split(utils.ConvertToString(currentVal), ",")

				// Create args array with the correct number of arguments for the query
				args := make([]interface{}, 0)
				for columnIndex := 0; columnIndex < len(pkColumns); columnIndex++ {
					// For each column combination in the WHERE clause, we need to add the necessary parts
					for partIndex := 0; partIndex <= columnIndex && partIndex < len(columns); partIndex++ {
						args = append(args, columns[partIndex])
					}
				}
				var nextValRaw interface{}
				err := tx.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
				if err == sql.ErrNoRows || nextValRaw == nil {
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

			logger.Infof("Chunking completed using splitViaPrimaryKey Method for stream %s", stream.ID())
			return nil
		})
	}
	limitOffsetChunking := func(chunks *types.Set[types.Chunk]) error {
		return jdbc.WithIsolation(ctx, m.client, true, func(tx *sql.Tx) error {
			chunks.Insert(types.Chunk{
				Min: nil,
				Max: utils.ConvertToString(chunkSize),
			})
			lastChunk := chunkSize
			for lastChunk < approxRowCount {
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(lastChunk),
					Max: utils.ConvertToString(lastChunk + chunkSize),
				})
				lastChunk += chunkSize
			}
			chunks.Insert(types.Chunk{
				Min: utils.ConvertToString(lastChunk),
				Max: nil,
			})
			logger.Infof("Chunking completed using limit offset method for stream %s", stream.ID())
			return nil
		})
	}

	/*
		splitEvenlyForInt generates chunk boundaries for numeric values by dividing the range [minBoundary, maxBoundary] using an arithmetic progression (AP).

		Each boundary follows:
		next = prev + chunkStepSize

		Example:
		minBoundary = 0, maxBoundary = 100, chunkStepSize = 25

		AP sequence:
		0 → 25 → 50 → 75 → 100

		Chunks formed:
		(-∞, 0), [0,25), [25,50), [50,75), [75,100), [100, +∞)
	*/
	splitEvenlyForInt := func(chunks *types.Set[types.Chunk]) error {
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(numericChunkBounds.MinBoundary),
		})
		prev := numericChunkBounds.MinBoundary
		for next := numericChunkBounds.MinBoundary + numericChunkBounds.ChunkStep; next <= numericChunkBounds.MaxBoundary; next += numericChunkBounds.ChunkStep {
			// Guard against int64 overflow causing `next` to wrap and become ≤ prev
			if next <= prev {
				logger.Warnf("int64 arithmetic overflow, falling back to splitViaPrimaryKey for stream %s", stream.ID())
				chunks.Clear()
				return splitViaPrimaryKey(stream, chunks)
			}
			chunks.Insert(types.Chunk{
				Min: utils.ConvertToString(prev),
				Max: utils.ConvertToString(next),
			})
			prev = next
		}
		chunks.Insert(types.Chunk{
			Min: utils.ConvertToString(prev),
			Max: nil,
		})
		logger.Infof("Chunking completed using splitEvenlyForInt Method for stream %s", stream.ID())
		return nil
	}

	/*
		splitEvenlyForString generates chunk boundaries for string-based primary keys
		by converting string values into a numeric (big.Int) space and iteratively
		splitting that range.

		Workflow:
		1. Convert min and max string values into padded form and map them into big.Int using custom charset-based encoding.
		2. Estimate the expected number of chunks based on table size and target file size.
		3. Compute an initial chunk step size using ceil division on the numeric range based on expected chunk count.
		4. Iteratively (with exponentially increasing attempts):
			- Adjust the interval dynamically using a scaling factor (stepShrinkFactor).
			- Generate candidate boundaries in numeric space and map them back to strings.
			- Align boundaries with actual DB values using MySQLDistinctAlignedPKValuesWithCollationQuery to fetch distinct collation-aware values between min and max.
			- Keep the best boundary set seen so far (max coverage).
			- Stop early if sufficient boundaries are obtained or no better boundary set can be produced due to query/result constraints.

		Final Step:
		- If sufficient boundaries are not obtained, fallback to primary key-based chunking.
		- Reduce boundary count to match expectedChunks if needed by evenly subsampling while preserving order.

		Example:
		minVal = "aa", maxVal = "az", expectedChunks = 4

		Generated boundaries after refining boundaries using collation-aware DB queries:
		["aa", "ai", "ar", "az"]

		Chunks:
		(-∞, "aa"), ["aa","ai"), ["ai","ar"), ["ar","az"), ["az", +∞)
	*/
	splitEvenlyForString := func(chunks *types.Set[types.Chunk]) error {
		expectedChunks := int64(math.Ceil(float64(approxTableSize) / float64(constants.EffectiveParquetSize)))
		expectedChunks = utils.Ternary(expectedChunks <= 0, int64(1), expectedChunks).(int64)

		stringChunkStepSize := new(big.Int).Sub(stringChunkBounds.maxEncodedBigIntValue, stringChunkBounds.minEncodedBigIntValue)
		stringChunkStepSize.Add(stringChunkStepSize, new(big.Int).Sub(big.NewInt(expectedChunks), big.NewInt(1)))
		stringChunkStepSize.Div(stringChunkStepSize, big.NewInt(expectedChunks))

		var (
			chunkBoundaries  = []string{}   // final chunk boundary values from DB
			rangeSlice       = []string{}   // reusable slice for boundary values per iteration
			adjustedStepSize = new(big.Int) // step size adjusted each iteration for balanced chunking
			currentBoundary  = new(big.Int) // current position in keyspace while generating boundaries
		)

		// Try to generate balanced chunks by adaptively adjusting the step size across exponentially increasing attempts.
		for stepShrinkFactor := int64(1); stepShrinkFactor <= int64(1000000); stepShrinkFactor = stepShrinkFactor * 2 {
			rangeSlice = rangeSlice[:0]
			adjustedStepSize.Set(stringChunkStepSize)
			adjustedStepSize.Add(adjustedStepSize, big.NewInt(stepShrinkFactor))
			adjustedStepSize.Div(adjustedStepSize, big.NewInt(stepShrinkFactor+1))
			currentBoundary.Set(stringChunkBounds.minEncodedBigIntValue)

			for chunkIdx := int64(0); chunkIdx < expectedChunks*(stepShrinkFactor+1) && currentBoundary.Cmp(stringChunkBounds.maxEncodedBigIntValue) < 0; chunkIdx++ {
				rangeSlice = append(rangeSlice, decodeBigIntToCharsetString(currentBoundary))
				currentBoundary.Add(currentBoundary, adjustedStepSize)
			}

			rangeSlice = append(rangeSlice, stringChunkBounds.MaxPadded)

			// Align boundaries with actual DB values using MySQL collation ordering
			query, args := jdbc.MySQLDistinctAlignedPKValuesWithCollationQuery(stream, pkColumns[0], rangeSlice, columnCollationType, stringChunkBounds.MinPadded, stringChunkBounds.MaxPadded)
			rows, err := m.client.QueryContext(ctx, query, args...)
			if err != nil {
				logger.Debugf("distinct boundary query failed for stream %s: %s", stream.ID(), err)
				break
			}
			rangeSlice = rangeSlice[:0]
			for rows.Next() {
				var val string
				if err := rows.Scan(&val); err != nil {
					rows.Close()
					return fmt.Errorf("failed to scan row: %s", err)
				}
				rangeSlice = append(rangeSlice, val)
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("row iteration error during distinct boundaries iteration: %s", err)
			}
			if len(rangeSlice) > len(chunkBoundaries) {
				chunkBoundaries = slices.Clone(rangeSlice)
			}
			if len(rangeSlice) >= int(expectedChunks) {
				break
			}
		}

		if len(chunkBoundaries) >= int(math.Ceil(float64(constants.MysqlChunkAcceptanceRatio*float64(expectedChunks)))) {
			chunkBoundaries = condenseStrings(chunkBoundaries, expectedChunks)
		} else {
			logger.Warnf("failed to generate chunks for stream %s, falling back to splitViaPrimaryKey method", stream.ID())
			return splitViaPrimaryKey(stream, chunks)
		}
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: chunkBoundaries[0],
		})

		for idx := 1; idx < len(chunkBoundaries); idx++ {
			chunks.Insert(types.Chunk{
				Min: chunkBoundaries[idx-1],
				Max: chunkBoundaries[idx],
			})
		}

		chunks.Insert(types.Chunk{
			Min: chunkBoundaries[len(chunkBoundaries)-1],
			Max: nil,
		})

		logger.Infof("Chunking completed using splitEvenlyForString Method for stream %s", stream.ID())
		return nil
	}

	switch {
	case numericChunkBounds != nil:
		logger.Infof("Using splitEvenlyForInt Method for stream %s", stream.ID())
		err = splitEvenlyForInt(chunks)
	case stringChunkBounds != nil:
		logger.Infof("Using splitEvenlyForString Method for stream %s", stream.ID())
		err = splitEvenlyForString(chunks)
	case len(pkColumns) > 0:
		logger.Infof("Using splitViaPrimaryKey Method for stream %s", stream.ID())
		err = splitViaPrimaryKey(stream, chunks)
	default:
		logger.Infof("Falling back to limit offset method for stream %s", stream.ID())
		err = limitOffsetChunking(chunks)
	}

	return chunks, err
}

func (m *MySQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string) (min, max any, err error) {
	query := jdbc.MinMaxQueryMySQL(stream, pkColumns)
	err = m.client.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// isNumericAndEvenDistributed checks if the pk column is numeric and evenly distributed
func isNumericAndEvenDistributed(minVal any, maxVal any, approxRowCount int64, chunkSize int64, dataType string) *NumericChunkBounds {
	destinationDataType := mysqlTypeToDataTypes[strings.ToLower(dataType)]
	if destinationDataType != types.Int32 && destinationDataType != types.Int64 {
		logger.Debugf("Current pk is not a supported numeric column")
		return nil
	}

	minBoundary, err := typeutils.ReformatInt64(minVal)
	if err != nil {
		logger.Debugf("failed to parse minVal: %s", err)
		return nil
	}

	maxBoundary, err := typeutils.ReformatInt64(maxVal)
	if err != nil {
		logger.Debugf("failed to parse maxVal: %s", err)
		return nil
	}

	distributionFactor := (float64(maxBoundary) - float64(minBoundary) + 1) / float64(approxRowCount)

	if distributionFactor < constants.DistributionLower || distributionFactor > constants.DistributionUpper {
		logger.Debugf("distribution factor is not in the range of %f to %f", constants.DistributionLower, constants.DistributionUpper)
		return nil
	}

	chunkStepSize := int64(math.Ceil(math.Max(distributionFactor*float64(chunkSize), 1)))

	return &NumericChunkBounds{
		ChunkStep:   chunkStepSize,
		MinBoundary: minBoundary,
		MaxBoundary: maxBoundary,
	}
}

// isStringSupportedPK checks if the pk column is a supported string column
func isStringSupportedPK(minVal any, maxVal any, dataMaxLength sql.NullInt64, dataType string) *StringChunkBounds {
	if dataType != "char" && dataType != "varchar" {
		logger.Debugf("Current pk is not a supported string column")
		return nil
	}

	minValPadded := utils.ConvertToString(minVal)
	maxValPadded := utils.ConvertToString(maxVal)

	if !dataMaxLength.Valid {
		logger.Debugf("dataMaxLength is not valid")
		return nil
	}
	minValPadded = padRightWithZeroes(minValPadded, int(dataMaxLength.Int64))
	maxValPadded = padRightWithZeroes(maxValPadded, int(dataMaxLength.Int64))

	minEncodedBigIntValue, err := encodeCharsetStringToBigInt(minValPadded)
	if err != nil {
		logger.Debugf("failed to encode minVal: %s", err)
		return nil
	}

	maxEncodedBigIntValue, err := encodeCharsetStringToBigInt(maxValPadded)
	if err != nil {
		logger.Debugf("failed to encode maxVal: %s", err)
		return nil
	}

	if minEncodedBigIntValue.Cmp(maxEncodedBigIntValue) >= 0 {
		logger.Debugf("encoded PK range is non-increasing")
		return nil
	}

	return &StringChunkBounds{
		MinPadded:             minValPadded,
		MaxPadded:             maxValPadded,
		minEncodedBigIntValue: minEncodedBigIntValue,
		maxEncodedBigIntValue: maxEncodedBigIntValue,
	}
}

type (
	// NumericChunkBounds holds evenly-spread integer PK chunking (splitEvenlyForInt)
	NumericChunkBounds struct {
		ChunkStep   int64
		MinBoundary int64
		MaxBoundary int64
	}
	// StringChunkBounds holds string-based primary key chunking (splitEvenlyForString)
	StringChunkBounds struct {
		MinPadded             string
		MaxPadded             string
		minEncodedBigIntValue *big.Int
		maxEncodedBigIntValue *big.Int
	}
)

var (
	// 95-character set: digits + uppercase + lowercase + symbols
	charset = []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz[\\]^_`{|}~!\"#$%&'()*+,-./:;<=>?@ ")

	charToIndex, indexToChar = buildCharsetMaps()              // maps for character to index and index to character
	charsetBase              = big.NewInt(int64(len(charset))) // base for the charset
)

// buildCharsetMaps builds the character to index and index to character maps
func buildCharsetMaps() (map[rune]int64, map[int64]rune) {
	charToIdx := make(map[rune]int64, len(charset))
	idxToChar := make(map[int64]rune, len(charset))
	for i, ch := range charset {
		idx := int64(i + 1)
		charToIdx[ch] = idx
		idxToChar[idx] = ch
	}
	return charToIdx, idxToChar
}

// encodeCharsetStringToBigInt converts a string to a big.Int using a custom charset and 1-based index
func encodeCharsetStringToBigInt(s string) (*big.Int, error) {
	val := big.NewInt(0)

	for _, ch := range []rune(s) {
		idx, ok := charToIndex[ch]
		if !ok {
			return big.NewInt(0), fmt.Errorf("unsupported character: %s", string(ch))
		}
		val.Mul(val, charsetBase)
		val.Add(val, big.NewInt(idx))
	}
	return val, nil
}

// decodeBigIntToCharsetString converts a big.Int to a string using a custom charset and 1-based index
func decodeBigIntToCharsetString(n *big.Int) string {
	if n.Cmp(big.NewInt(0)) == 0 {
		return ""
	}

	x := new(big.Int).Set(n)
	var runes []rune

	for x.Cmp(big.NewInt(0)) > 0 {
		rem := new(big.Int).Mod(x, charsetBase)
		if rem.Cmp(big.NewInt(0)) == 0 {
			rem = charsetBase
			x.Sub(x, big.NewInt(1))
		}
		ch := indexToChar[rem.Int64()]
		runes = append(runes, ch)
		x.Div(x, charsetBase)
	}

	slices.Reverse(runes)
	return string(runes)
}

// padRightWithZeroes pads a string with zeroes to the right up to a maximum length
func padRightWithZeroes(s string, maxLength int) string {
	length := utf8.RuneCountInString(s)
	if length >= maxLength {
		return s
	}
	return s + strings.Repeat("0", maxLength-length)
}

/*
	condenseStrings picks expectedChunks elements evenly from candidateBoundaries.

	Each output index i (0..expectedChunks-1) is mapped to an input index (0..numCandidateBoundaries-1) using the formula:

		idx ≈ round(i*(numCandidateBoundaries-1)/(expectedChunks-1))

	- Always includes first (0) and last (numCandidateBoundaries-1)
	- Rounding keeps spacing balanced (no left/right bias)

	Example:
	numCandidateBoundaries = 15 (indices 0..14), expectedChunks = 8
	Range is split into 7 equal gaps (~2 apart), so we pick:
	[0,2,4,6,8,10,12,14]
*/
func condenseStrings(candidateBoundaries []string, expectedChunks int64) []string {
	numCandidateBoundaries := int64(len(candidateBoundaries))
	if expectedChunks >= numCandidateBoundaries {
		return candidateBoundaries
	}
	// If only one element needed
	if expectedChunks == 1 {
		return []string{candidateBoundaries[0]}
	}
	condensedBoundaries := make([]string, expectedChunks)
	for i := int64(0); i < expectedChunks; i++ {
		// evenly distributed index (rounded)
		idx := (i*(numCandidateBoundaries-1) + (expectedChunks-1)/2) / (expectedChunks - 1)
		condensedBoundaries[i] = candidateBoundaries[idx]
	}
	return condensedBoundaries
}
