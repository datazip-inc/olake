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
		var chunkArgs []any
		if chunkColumn != "" {
			stmt, chunkArgs = jdbc.MysqlChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt, chunkArgs = jdbc.MysqlChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			stmt = jdbc.MysqlLimitOffsetScanQuery(stream, chunk, filter)
		}
		args = append(chunkArgs, args...)
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
		approxRowCount		int64
		avgRowSize  	    any
		approxTableSize     int64
		columnCollationType string
		dataMaxLength    	sql.NullInt64
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
		chunkStepSize int64
		minVal        any // to define lower range of the chunk
		maxVal        any // to define upper range of the chunk
		minBoundary   int64
		maxBoundary   int64
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

	stringSupportedPk := false

	if len(pkColumns) == 1 {
		var dataType string
		query := jdbc.MySQLColumnStatsQuery()
		err = m.client.QueryRowContext(ctx, query, stream.Name(), pkColumns[0]).Scan(&dataType, &dataMaxLength, &columnCollationType)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch Column DataType and max length %s", err)
		}
		// 1. Try Numeric Strategy
		chunkStepSize, minBoundary, maxBoundary = isNumericAndEvenDistributed(minVal, maxVal, approxRowCount, chunkSize, dataType)

		// 2. If not numeric, check for supported String strategy
		if chunkStepSize == 0 {
			switch strings.ToLower(dataType) {
			case "char", "varchar":
				stringSupportedPk = true
			default:
				logger.Infof("%s is not a string type PK", pkColumns[0])
			}
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

			logger.Infof("Chunking completed using SplitViaPrimaryKey Method for stream %s", stream.ID())
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
	splitEvenlyForInt := func(chunks *types.Set[types.Chunk], chunkStepSize int64) error {
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(minBoundary),
		})
		prev := minBoundary
		for next := minBoundary + chunkStepSize; next <= maxBoundary; next += chunkStepSize {
			// condition to protect from infinite loop
			if next <= prev {
				logger.Warnf("int64 arithmetic overflow, falling back to SplitViaPrimaryKey for stream %s", stream.ID())
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
		1. Convert min and max string values into padded form and map them into big.Int using unicode-based encoding.
		2. Estimate the expected number of chunks based on table size and target file size.
		3. Compute an initial chunk interval using ceil division on the numeric range.
		4. Iteratively (up to 5 attempts):
		- Adjust the interval using an AP-based variation.
		- Generate candidate boundaries in numeric space and map them back to strings.
		- Query distinct values using collation-aware SQL (ordering handled in query).
		- Validate the number of effective chunks using a count query.
		- If at least the required threshold (~80%) of chunks is achieved, accept and stop.
		5. If all attempts fail, fallback to primary key–based chunking.

		Final Step:
		- Use the validated boundary values to construct non-overlapping chunks
		covering the full range [min, max], including open-ended boundaries.

		Example:
		minVal = "aa", maxVal = "az", expectedChunks = 3

		Generated boundaries after refining boundaries using collation-aware DB queries:
		["aa", "ai", "ar", "az"]

		Chunks:
		(-∞, "aa"), ["aa","ai"), ["ai","ar"), ["ar","az"), ["az", +∞)
	*/
	splitEvenlyForString := func(chunks *types.Set[types.Chunk]) error {
		var validChunksCount int

		maxValPadded := utils.ConvertToString(maxVal)
		minValPadded := utils.ConvertToString(minVal)

		if dataMaxLength.Valid {
			maxValPadded = padRightWithNulls(maxValPadded, int(dataMaxLength.Int64))
			minValPadded = padRightWithNulls(minValPadded, int(dataMaxLength.Int64))
		}

		maxEncodedBigIntValue := encodeUnicodeStringToBigInt(maxValPadded)
		minEncodedBigIntValue := encodeUnicodeStringToBigInt(minValPadded)

		expectedChunks := int64(math.Ceil(float64(approxTableSize) / float64(constants.EffectiveParquetSize)))
		expectedChunks = utils.Ternary(expectedChunks <= 0, int64(1), expectedChunks).(int64)

		stringChunkStepSize := new(big.Int).Sub(&maxEncodedBigIntValue, &minEncodedBigIntValue)
		stringChunkStepSize.Add(stringChunkStepSize, new(big.Int).Sub(big.NewInt(expectedChunks), big.NewInt(1)))
		stringChunkStepSize.Div(stringChunkStepSize, big.NewInt(expectedChunks)) //ceil division set up

		rangeSlice := []string{}
		// Try up to 5 times to generate balanced chunks by slightly adjusting the chunk size each iteration.
		for retryAttempt := int64(0); retryAttempt < int64(5); retryAttempt++ {
			adjustedStepSize := new(big.Int).Set(stringChunkStepSize)
			adjustedStepSize.Add(adjustedStepSize, big.NewInt(retryAttempt))
			adjustedStepSize.Div(adjustedStepSize, big.NewInt(retryAttempt+1))
			currentBoundary := new(big.Int).Set(&minEncodedBigIntValue)

			for chunkIdx := int64(0); chunkIdx < expectedChunks*(retryAttempt+1) && currentBoundary.Cmp(&maxEncodedBigIntValue) < 0; chunkIdx++ {
				rangeSlice = append(rangeSlice, decodeBigIntToUnicodeString(currentBoundary))
				currentBoundary.Add(currentBoundary, adjustedStepSize)
			}

			// Align boundaries with actual DB values using MySQL collation ordering
			rangeSlice = append(rangeSlice, decodeBigIntToUnicodeString(&maxEncodedBigIntValue))
			query, args := jdbc.MySQLDistinctValuesWithCollationQuery(rangeSlice, columnCollationType)
			rows, err := m.client.QueryContext(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("failed to run distinct query: %s", err)
			}
			rangeSlice = rangeSlice[:0]
			// Some chunks generated might be completely empty when boundaries greater
			// than the max value and smaller than the min value exists
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

			// Counting the number of valid chunks generated i.e., between min and max
			query, args = jdbc.MySQLCountGeneratedInRange(rangeSlice, columnCollationType, minValPadded, maxValPadded)
			err = m.client.QueryRowContext(ctx, query, args...).Scan(&validChunksCount)
			if err != nil {
				return fmt.Errorf("failed to run count query: %s", err)
			}

			// Accept boundaries if enough valid chunks are produced
			if float64(validChunksCount) >= float64(expectedChunks)*constants.MysqlChunkAcceptanceRatio {
				logger.Infof("Successfully Generated Chunks using splitEvenlyForString Method for stream %s", stream.ID())
				break
			}

			// If the number of valid chunks generated is less than the expected chunks * a constant factor even after 5 iterations, we fallback to splitViaPrimaryKey
			if float64(validChunksCount) < float64(expectedChunks)*constants.MysqlChunkAcceptanceRatio && retryAttempt == 4 {
				logger.Warnf("failed to generate chunks for stream %s, falling back to splitviaprimarykey method", stream.ID())
				err = splitViaPrimaryKey(stream, chunks)
				if err != nil {
					return fmt.Errorf("failed to generate chunks for stream %s: %s", stream.ID(), err)
				}
				return nil
			}
			rangeSlice = rangeSlice[:0]
		}

		if len(rangeSlice) == 0 {
			return nil
		}

		chunks.Insert(types.Chunk{
			Min: nil,
			Max: rangeSlice[0],
		})

		for idx := 1; idx < len(rangeSlice); idx++ {
			chunks.Insert(types.Chunk{
				Min: rangeSlice[idx-1],
				Max: rangeSlice[idx],
			})
		}

		chunks.Insert(types.Chunk{
			Min: rangeSlice[len(rangeSlice)-1],
			Max: nil,
		})

		logger.Infof("Chunking completed using splitEvenlyForString Method for stream %s", stream.ID())
		return nil
	}

	switch {
	case chunkStepSize > 0:
		logger.Infof("Using splitEvenlyForInt Method for stream %s", stream.ID())
		err = splitEvenlyForInt(chunks, chunkStepSize)
	case stringSupportedPk:
		logger.Infof("Using splitEvenlyForString Method for stream %s", stream.ID())
		err = splitEvenlyForString(chunks)
	case len(pkColumns) > 0:
		logger.Infof("Using SplitViaPrimaryKey Method for stream %s", stream.ID())
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

// checks if the pk column is numeric and evenly distributed
func isNumericAndEvenDistributed(minVal any, maxVal any, approxRowCount int64, chunkSize int64, dataType string) (int64, int64, int64) {
	destinationDataType := mysqlTypeToDataTypes[strings.ToLower(dataType)]
	if destinationDataType != types.Int32 && destinationDataType != types.Int64 {
		logger.Debugf("Current pk is not a supported numeric column")
		return 0, 0, 0
	}

	minBoundary, err := typeutils.ReformatInt64(minVal)
	if err != nil {
		logger.Debugf("failed to parse minVal: %s", err)
		return 0, 0, 0
	}

	maxBoundary, err := typeutils.ReformatInt64(maxVal)
	if err != nil {
		logger.Debugf("failed to parse maxVal: %s", err)
		return 0, 0, 0
	}

	distributionFactor := (float64(maxBoundary) - float64(minBoundary) + 1) / float64(approxRowCount)

	if distributionFactor < constants.DistributionLower || distributionFactor > constants.DistributionUpper {
		logger.Debugf("distribution factor is not in the range of %f to %f", constants.DistributionLower, constants.DistributionUpper)
		return 0, 0, 0
	}

	chunkStepSize := int64(math.Ceil(math.Max(distributionFactor*float64(chunkSize), 1)))
	return chunkStepSize, minBoundary, maxBoundary
}

/*
	encodeUnicodeStringToBigInt maps a string to a big.Int using base = 1114112(UnicodeSize), treating each rune as a digit in a positional system.

	Value = r₀*base^(n-1) + r₁*base^(n-2) + ... + rₙ

	Example:
	s = "aa"
	r₀ = 'a' = 97, r₁ = 'a' = 97, base = 1114112

	Value = r₀*base^(n-1) + r₁*base^(n-2)

		= 97*1114112 + 97
		= 108068961
*/
func encodeUnicodeStringToBigInt(s string) big.Int {
	base := big.NewInt(constants.UnicodeSize)
	val := big.NewInt(0)

	for _, ch := range []rune(s) {
		val.Mul(val, base)
		val.Add(val, big.NewInt(int64(ch)))
	}
	return *val
}

/*
	decodeBigIntToUnicodeString reconstructs the original string from its big.Int representation by extracting digits in base = 1114112 (UnicodeSize).

	It repeatedly takes modulus and division by base to recover each rune:
	rᵢ = n % base, then n = n / base

	Example:
	n = 108068961, base = 1114112

	Step 1:
	r₁ = n % base = 97 → 'a'
	n = n / base = 97

	Step 2:
	r₀ = n % base = 97 → 'a'
	n = 0

	Reconstructed (after reversing):
	"aa"
*/
func decodeBigIntToUnicodeString(n *big.Int) string {
	if n.Cmp(big.NewInt(0)) == 0 {
		return ""
	}
	base := big.NewInt(constants.UnicodeSize)
	x := new(big.Int).Set(n)
	var runes []rune

	for x.Cmp(big.NewInt(0)) > 0 {
		rem := new(big.Int).Mod(x, base)
		runes = append(runes, rune(rem.Int64()))
		x.Div(x, base)
	}

	slices.Reverse(runes)
	return string(runes)
}

/*
	Padding a string with null characters to a specified length.

	Example:
	padRightWithNulls("aa", 4) = "aa\x00\x00"
*/
func padRightWithNulls(s string, maxLength int) string {
	length := utf8.RuneCountInString(s)
	if length >= maxLength {
		return s
	}
	return s + strings.Repeat("\x00", maxLength-length)
}
