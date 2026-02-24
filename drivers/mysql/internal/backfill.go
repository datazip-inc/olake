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

func (m *MySQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	var avgRowSize any
	var avgSchemaSize int64
	var tableCollationType string
	var dataMaxLength sql.NullInt64
	approxRowCountQuery := jdbc.MySQLTableRowStatsQuery()
	err := m.client.QueryRowContext(ctx, approxRowCountQuery, stream.Name()).Scan(&approxRowCount, &avgRowSize, &avgSchemaSize, &tableCollationType)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch RowStats query for table=%s: %v", stream.Name(), err)
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
		isNumericAndEvenDistributed bool
		step                        int64
		minVal                      any //to define lower range of the chunk
		maxVal                      any //to define upper range of the chunk
		minFloat                    float64
		maxFloat                    float64
	)

	pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
	if chunkColumn != "" {
		pkColumns = []string{chunkColumn}
	}
	sort.Strings(pkColumns)

	if len(pkColumns) > 0 || chunkColumn != "" {
		minVal, maxVal, err = m.getTableExtremes(ctx, stream, pkColumns)
		if err != nil {
			logger.Debugf("Stream %s: Failed to get table extremes: %v", stream.ID(), err)
		}
	}
	// Supported MySQL string-like PK datatypes
	var stringTypes = map[string]struct{}{
		"char":    {},
		"varchar": {},
	}
	//defining boolean to check if string is supported or not
	stringSupportedPk := false

	if len(pkColumns) == 1 {
		isNumericAndEvenDistributed, step, minFloat, maxFloat, err = IsNumericAndEvenDistributed(minVal, maxVal, approxRowCount, chunkSize)
		if err != nil {
			isNumericAndEvenDistributed = false
			logger.Debugf("Stream %s: PK is not numeric or conversion failed, falling back to string splitting: %v", stream.ID(), err)
		}
		var dataType string
		query := jdbc.MySQLColumnTypeQuery()
		err = m.client.QueryRowContext(ctx, query, stream.Name(), pkColumns[0]).Scan(&dataType, &dataMaxLength)
		if err != nil {
			logger.Errorf("failed to fetch Column DataType and max length %s", err)
		} else {
			if _, ok := stringTypes[dataType]; ok {
				stringSupportedPk = true
				fmt.Println("This is a string type PK")
			}
			if dataMaxLength.Valid {
				fmt.Println("Data Max Length:", dataMaxLength.Int64)
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
			return nil
		})
	}

	//used mathematical calculation to split the chunks for cases where the distribution factor is within the range when pk is numeric
	splitEvenlyForInt := func(chunks *types.Set[types.Chunk], step float64) {
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: utils.ConvertToString(minFloat),
		})
		prev := minFloat
		for next := minFloat + step; next <= maxFloat; next += step {
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
	}

	//used mathematical calculation to split the chunks for cases where the pk columns size is 1 and pk data type is string
	splitEvenlyForString := func(chunks *types.Set[types.Chunk]) error {
		var maxValBaseN, minValBaseN big.Int
		var validChunksCount int
		maxValPadded := utils.ConvertToString(maxVal)
		minValPadded := utils.ConvertToString(minVal)
		if dataMaxLength.Valid {
			maxValPadded = padRightNull(maxValPadded, int(dataMaxLength.Int64))
			minValPadded = padRightNull(minValPadded, int(dataMaxLength.Int64))
		}
		if val, err := convertUnicodeStringToInt(maxValPadded); err != nil {
			return fmt.Errorf("failed to convert maxVal: %v", err)
		} else {
			maxValBaseN.Set(&val)
		}
		if val, err := convertUnicodeStringToInt(minValPadded); err != nil {
			return fmt.Errorf("failed to convert minVal: %v", err)
		} else {
			minValBaseN.Set(&val)
		}

		expectedChunks := int64(math.Ceil(float64(avgSchemaSize) / float64(constants.EffectiveParquetSize)))
		if expectedChunks <= 0 {
			expectedChunks = 1
		}
		chunkdiff := new(big.Int).Sub(&maxValBaseN, &minValBaseN)
		chunkdiff.Add(chunkdiff, new(big.Int).Sub(big.NewInt(expectedChunks), big.NewInt(1)))
		chunkdiff.Div(chunkdiff, big.NewInt(expectedChunks)) //ceil division set up
		rangeSlice := []string{}
		for i := int64(0); i < int64(5); i++ {
			temporarychunkdiff := new(big.Int).Set(chunkdiff)
			temporarychunkdiff.Add(temporarychunkdiff, big.NewInt(i))
			temporarychunkdiff.Div(temporarychunkdiff, big.NewInt(i+1))
			curr := new(big.Int).Set(&minValBaseN)
			for j := int64(0); j < expectedChunks && curr.Cmp(&maxValBaseN) < 0; j++ {
				rangeSlice = append(rangeSlice, convertIntUnicodeToString(curr))
				curr.Add(curr, temporarychunkdiff)
			}
			rangeSlice = append(rangeSlice, convertIntUnicodeToString(&maxValBaseN))
			query, args := jdbc.MySQLDistinctValuesWithCollationQuery(rangeSlice, tableCollationType)
			rows, err := m.client.QueryContext(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("failed to run distinct query: %v", err)
			}
			rangeSlice = rangeSlice[:0]
			for rows.Next() {
				var val string
				if err := rows.Scan(&val); err != nil {
					logger.Errorf("failed to scan row: %v", err)
				}
				rangeSlice = append(rangeSlice, val)
			}
			rows.Close()
			query, args = jdbc.MySQLCountGeneratedInRange(rangeSlice, tableCollationType, minValPadded, maxValPadded)
			err = m.client.QueryRowContext(ctx, query, args...).Scan(&validChunksCount)
			if err != nil {
				return fmt.Errorf("failed to run count query: %v", err)
			}
			if float64(validChunksCount) >= float64(expectedChunks)*constants.MysqlChunkSizeReductionFactor {
				logger.Debug("Successfully Generated Chunks")
				for i, val := range rangeSlice {
					logger.Debugf("Boundary[%d] = %q", i, val)
				}
				break
			}
			if float64(validChunksCount) < float64(expectedChunks)*constants.MysqlChunkSizeReductionFactor && i == 4 {
				logger.Warnf("failed to generate chunks for stream %s, falling back to primary key chunking", stream.ID())
				err = splitViaPrimaryKey(stream, chunks)
				if err != nil {
					return fmt.Errorf("failed to generate chunks for stream %s: %v", stream.ID(), err)
				}
				return nil
			}
			rangeSlice = rangeSlice[:0]
		}
		if len(rangeSlice) == 0 {
			return nil
		}
		prev := rangeSlice[0]
		chunks.Insert(types.Chunk{
			Min: nil,
			Max: prev,
		})
		for idx := range rangeSlice {
			if idx == 0 {
				continue
			}
			currVal := rangeSlice[idx]
			chunks.Insert(types.Chunk{
				Min: prev,
				Max: currVal,
			})
			prev = currVal
		}
		chunks.Insert(types.Chunk{
			Min: prev,
			Max: nil,
		})
		return nil
	}
	switch {
	case len(pkColumns) == 1 && isNumericAndEvenDistributed:
		logger.Debugf("Using splitEvenlyForInt Method for stream %s", stream.ID())
		splitEvenlyForInt(chunks, float64(step))
		logger.Debugf("Chunking completed using splitEvenlyForInt Method for stream %s", stream.ID())
	case len(pkColumns) == 1 && stringSupportedPk:
		logger.Debugf("Using splitEvenlyForString Method for stream %s", stream.ID())
		err = splitEvenlyForString(chunks)
		logger.Debugf("Chunking completed using splitEvenlyForString Method for stream %s", stream.ID())
	case len(pkColumns) > 1:
		logger.Debugf("Using SplitViaPrimaryKey Method for stream %s", stream.ID())
		err = splitViaPrimaryKey(stream, chunks)
		logger.Debugf("Chunking completed using SplitViaPrimaryKey Method for stream %s", stream.ID())
	default:
		logger.Debugf("Falling back to limit offset method for stream %s", stream.ID())
		err = limitOffsetChunking(chunks)
		logger.Debugf("Chunking completed using limit offset method for stream %s", stream.ID())
	}
	return chunks, err
}

func (m *MySQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string) (min, max any, err error) {
	query := jdbc.MinMaxQueryMySQL(stream, pkColumns)
	err = m.client.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// checks if the pk column is numeric and evenly distributed
func IsNumericAndEvenDistributed(minVal any, maxVal any, approxRowCount int64, chunkSize int64) (bool, int64, float64, float64, error) {
	if approxRowCount == 0 {
		return false, 0, 0, 0, nil
	}
	minFloat, err1 := typeutils.ReformatFloat64(minVal)
	maxFloat, err2 := typeutils.ReformatFloat64(maxVal)
	if err1 != nil || err2 != nil {
		if err1 != nil {
			return false, 0, 0, 0, err1
		}
		return false, 0, 0, 0, err2
	}
	distributionFactor := (maxFloat - minFloat + 1) / float64(approxRowCount)
	if distributionFactor < constants.DistributionLower || distributionFactor > constants.DistributionUpper {
		err := fmt.Errorf("distribution factor is not in the range of %f to %f", constants.DistributionLower, constants.DistributionUpper)
		return false, 0, 0, 0, err
	}
	step := int64(math.Max(distributionFactor*float64(chunkSize), 1))
	return true, step, minFloat, maxFloat, nil
}

// convert a string to a baseN number
func convertUnicodeStringToInt(s string) (big.Int, error) {
	base := big.NewInt(constants.UnicodeSize)
	val := big.NewInt(0)

	for _, ch := range []rune(s) {
		val.Mul(val, base)
		val.Add(val, big.NewInt(int64(ch)))
	}
	return *val, nil
}

// convert a baseN number to a string pointer
func convertIntUnicodeToString(n *big.Int) string {
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

func padRightNull(s string, maxLength int) string {
	length := utf8.RuneCountInString(s)
	if length >= maxLength {
		return s
	}
	return s + strings.Repeat("\x00", maxLength-length)
}
