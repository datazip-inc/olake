package driver

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

func (m *Mongo) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) (err error) {
	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	parsedFilter, err := m.getParsedFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	cursor, err := collection.Aggregate(ctx, generatePipeline(chunk.Min, chunk.Max, parsedFilter), opts)
	if err != nil {
		return fmt.Errorf("failed to create cursor: %s", err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.M
		if _, err = cursor.Current.LookupErr("_id"); err != nil {
			return fmt.Errorf("looking up idProperty: %s", err)
		} else if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("backfill decoding document: %s", err)
		}
		// Filter mongo object
		filterMongoObject(doc)
		if err := OnMessage(doc); err != nil {
			return fmt.Errorf("failed to send message to writer: %s", err)
		}
	}
	return cursor.Err()
}

func (m *Mongo) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	recordCount, err := m.totalCountInCollection(ctx, collection)
	if err != nil {
		return nil, err
	}
	if recordCount == 0 {
		logger.Infof("Collection is empty, nothing to backfill")
		return types.NewSet[types.Chunk](), nil
	}

	logger.Infof("Total expected count for stream %s: %d", stream.ID(), recordCount)
	pool.AddRecordsToSync(recordCount)

	// Parse the filter
	parsedFilter, err := m.getParsedFilter(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter during chunk splitting: %s", err)
	}

	// Generate and update chunks
	var retryErr error
	var chunksArray []types.Chunk
	err = abstract.RetryOnBackoff(m.config.RetryCount, 1*time.Minute, func() error {
		chunksArray, retryErr = m.splitChunks(ctx, collection, stream, parsedFilter)
		return retryErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed after retry backoff: %s", err)
	}
	return types.NewSet(chunksArray...), nil
}

func (m *Mongo) splitChunks(ctx context.Context, collection *mongo.Collection, stream types.StreamInterface, parsedFilter bson.D) ([]types.Chunk, error) {
	splitVectorStrategy := func() ([]types.Chunk, error) {
		getID := func(order int) (primitive.ObjectID, error) {
			var doc bson.M
			err := collection.FindOne(ctx, parsedFilter, options.FindOne().SetSort(bson.D{{Key: "_id", Value: order}})).Decode(&doc)
			if err == mongo.ErrNoDocuments {
				return primitive.NilObjectID, nil
			}
			return doc["_id"].(primitive.ObjectID), err
		}

		minID, err := getID(1)
		if err != nil || minID == primitive.NilObjectID {
			return nil, err
		}
		maxID, err := getID(-1)
		if err != nil {
			return nil, err
		}
		getChunkBoundaries := func() ([]*primitive.ObjectID, error) {
			var result bson.M
			cmd := bson.D{
				{Key: "splitVector", Value: fmt.Sprintf("%s.%s", collection.Database().Name(), collection.Name())},
				{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
				{Key: "maxChunkSize", Value: 1024},
			}
			if len(parsedFilter) > 0 {
				cmd = append(cmd, bson.E{Key: "filter", Value: parsedFilter})
			}
			if err := collection.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
				return nil, fmt.Errorf("failed to run splitVector command: %s", err)
			}

			boundaries := []*primitive.ObjectID{&minID}
			for _, key := range result["splitKeys"].(bson.A) {
				if id, ok := key.(bson.M)["_id"].(primitive.ObjectID); ok {
					boundaries = append(boundaries, &id)
				}
			}
			return append(boundaries, &maxID), nil
		}

		boundaries, err := getChunkBoundaries()
		if err != nil {
			return nil, fmt.Errorf("failed to get chunk boundaries: %s", err)
		}
		var chunks []types.Chunk
		for i := 0; i < len(boundaries)-1; i++ {
			chunks = append(chunks, types.Chunk{
				Min: boundaries[i].Hex(),
				Max: boundaries[i+1].Hex(),
			})
		}
		if len(boundaries) > 0 {
			chunks = append(chunks, types.Chunk{
				Min: boundaries[len(boundaries)-1].Hex(),
				Max: nil,
			})
		}
		return chunks, nil
	}
	bucketAutoStrategy := func() ([]types.Chunk, error) {
		logger.Info("using bucket auto strategy for stream: %s", stream.ID())
		// Use $bucketAuto for chunking
		pipeline := mongo.Pipeline{}
		if len(parsedFilter) > 0 {
			pipeline = append(pipeline, bson.D{{Key: "$match", Value: parsedFilter}})
		}
		pipeline = append(pipeline,
			bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
			bson.D{{Key: "$bucketAuto", Value: bson.D{
				{Key: "groupBy", Value: "$_id"},
				{Key: "buckets", Value: m.config.MaxThreads * 4},
			}}},
		)

		cursor, err := collection.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, fmt.Errorf("failed to execute bucketAuto aggregation: %s", err)
		}
		defer cursor.Close(ctx)

		var buckets []struct {
			ID struct {
				Min primitive.ObjectID `bson:"min"`
				Max primitive.ObjectID `bson:"max"`
			} `bson:"_id"`
			Count int `bson:"count"`
		}

		if err := cursor.All(ctx, &buckets); err != nil {
			return nil, fmt.Errorf("failed to decode bucketAuto results: %s", err)
		}

		var chunks []types.Chunk
		for _, bucket := range buckets {
			chunks = append(chunks, types.Chunk{
				Min: bucket.ID.Min.Hex(),
				Max: bucket.ID.Max.Hex(),
			})
		}
		if len(buckets) > 0 {
			chunks = append(chunks, types.Chunk{
				Min: buckets[len(buckets)-1].ID.Max.Hex(),
				Max: nil,
			})
		}

		return chunks, nil
	}

	timestampStrategy := func() ([]types.Chunk, error) {
		// Time-based strategy implementation
		first, last, err := m.fetchExtremes(ctx, collection, parsedFilter)
		if err != nil {
			return nil, err
		}

		logger.Infof("Extremes of Stream %s are start: %s \t end:%s", stream.ID(), first, last)
		timeDiff := last.Sub(first).Hours() / 6
		if timeDiff < 1 {
			timeDiff = 1
		}
		// for every 6hr difference ideal density is 10 Seconds
		density := time.Duration(timeDiff) * (10 * time.Second)
		start := first
		var chunks []types.Chunk
		for start.Before(last) {
			end := start.Add(density)
			minObjectID := generateMinObjectID(start)
			maxObjectID := generateMinObjectID(end)
			if end.After(last) {
				maxObjectID = generateMinObjectID(last.Add(time.Second))
			}
			start = end
			chunks = append(chunks, types.Chunk{
				Min: minObjectID,
				Max: maxObjectID,
			})
		}
		chunks = append(chunks, types.Chunk{
			Min: generateMinObjectID(last),
			Max: nil,
		})

		return chunks, nil
	}

	switch m.config.ChunkingStrategy {
	case "timestamp":
		return timestampStrategy()
	default:
		chunks, err := splitVectorStrategy()
		// check if authorization error occurs
		if err != nil && (strings.Contains(err.Error(), "not authorized") ||
			strings.Contains(err.Error(), "CMD_NOT_ALLOWED")) {
			logger.Warnf("failed to get chunks via split vector strategy: %s", err)
			return bucketAutoStrategy()
		}
		return chunks, err
	}
}

func (m *Mongo) totalCountInCollection(ctx context.Context, collection *mongo.Collection) (int64, error) {
	var countResult bson.M
	command := bson.D{{
		Key:   "collStats",
		Value: collection.Name(),
	}}
	err := collection.Database().RunCommand(ctx, command).Decode(&countResult)
	if err != nil {
		return 0, fmt.Errorf("failed to get total count: %s", err)
	}
	return int64(countResult["count"].(int32)), nil
}

func (m *Mongo) fetchExtremes(ctx context.Context, collection *mongo.Collection, parsedFilter bson.D) (time.Time, time.Time, error) {
	extreme := func(sortby int) (time.Time, error) {
		var result bson.M
		err := collection.FindOne(ctx, parsedFilter, options.FindOne().SetSort(bson.D{{Key: "_id", Value: sortby}})).Decode(&result)
		if err != nil {
			return time.Time{}, err
		}
		objectID, ok := result["_id"].(primitive.ObjectID)
		if !ok {
			return time.Time{}, fmt.Errorf("failed to cast _id[%v] to ObjectID", objectID)
		}
		return objectID.Timestamp(), nil
	}

	start, err := extreme(1)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find start: %s", err)
	}

	end, err := extreme(-1)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find end: %s", err)
	}

	start = start.Add(-time.Minute * 10)
	end = end.Add(time.Minute * 10)
	return start, end, nil
}

func generatePipeline(start, end any, parsedFilter bson.D) mongo.Pipeline {
	start, _ = primitive.ObjectIDFromHex(start.(string))
	if end != nil {
		end, _ = primitive.ObjectIDFromHex(end.(string))
	}
	andOperation := bson.A{
		bson.D{
			{
				Key: "$and",
				Value: bson.A{
					bson.D{{
						Key: "_id", Value: bson.D{{
							Key:   "$type",
							Value: 7,
						}},
					}},
					bson.D{{
						Key: "_id",
						Value: bson.D{{
							Key:   "$gte",
							Value: start,
						}},
					}},
				}},
		},
	}

	if end != nil {
		andOperation = append(andOperation, bson.D{{
			Key: "_id",
			Value: bson.D{{
				Key:   "$lt",
				Value: end,
			}},
		}})
	}

	if len(parsedFilter) > 0 {
		andOperation = append(andOperation, parsedFilter)
	}

	return mongo.Pipeline{
		{
			{
				Key: "$match",
				Value: bson.D{
					{
						Key:   "$and",
						Value: andOperation,
					},
				}},
		},
		bson.D{
			{Key: "$sort",
				Value: bson.D{{Key: "_id", Value: 1}}},
		},
	}
}

func generateMinObjectID(t time.Time) string {
	objectID := primitive.NewObjectIDFromTimestamp(t)
	for i := 4; i < 12; i++ {
		objectID[i] = 0x00
	}
	return objectID.Hex()
}

// getParsedFilter converts the stream's filter metadata into a BSON document
func (m *Mongo) getParsedFilter(stream types.StreamInterface) (bson.D, error) {
	filter := strings.TrimSpace(stream.Self().StreamMetadata.Filter)
	if filter == "" {
		return bson.D{}, nil
	}

	// If the filter is raw BSON
	if strings.HasPrefix(filter, "{") {
		var parsedFilter bson.D
		err := bson.UnmarshalExtJSON([]byte(filter), false, &parsedFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter: %v", err)
		}
		return parsedFilter, nil
	}
	// Otherwise, parse domain-specific filter DSL
	parsedFilter, err := parseDSLFilter(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to parse domain-specific filter: %s", err)
	}
	return parsedFilter, nil
}

// parseDSLFilter converts a simple DSL expression (e.g., "age>=30 and name=\"John\"") into BSON
func parseDSLFilter(input string) (bson.D, error) {
	// Determine top-level operator ($and or $or) and split clauses
	mongoOperator, parts := splitTopLevel(input)
	var bsonClauses []interface{}
	clausePattern := regexp.MustCompile(`^\s*([a-zA-Z0-9_]+)\s*(>=|<=|!=|=|>|<)\s*(?:"([^"]*)"|(\S+))\s*$`)

	// Map comparison operators to Mongo symbols
	opMapping := map[string]string{
		">":  "$gt",
		">=": "$gte",
		"<":  "$lt",
		"<=": "$lte",
		"=":  "$eq",
		"!=": "$ne",
	}

	for _, partExpr := range parts {
		partExpr = strings.TrimSpace(partExpr)
		// Handle nested expressions wrapped in parentheses
		if strings.HasPrefix(partExpr, "(") && strings.HasSuffix(partExpr, ")") {
			inner := strings.TrimSpace(partExpr[1 : len(partExpr)-1])
			subFilter, err := parseDSLFilter(inner)
			if err != nil {
				return nil, err
			}
			bsonClauses = append(bsonClauses, subFilter)
			continue
		}

		matches := clausePattern.FindStringSubmatch(partExpr)
		if matches == nil {
			return nil, fmt.Errorf("invalid filter clause %q", partExpr)
		}
		field, opSymbol, stringValue, unquotedValue := matches[1], matches[2], matches[3], matches[4]
		rawValue := unquotedValue
		if stringValue != "" {
			rawValue = stringValue
		}

		// Attempt type conversions: ObjectID, boolean, time, int
		var parsedValue interface{} = rawValue
		if field == "_id" && len(rawValue) == 24 {
			if objectID, err := primitive.ObjectIDFromHex(rawValue); err == nil {
				parsedValue = objectID
			}
		} else if strings.ToLower(rawValue) == "true" {
			parsedValue = true
		} else if strings.ToLower(rawValue) == "false" {
			parsedValue = false
		} else if parsedTimeVal, err := time.Parse(time.RFC3339, rawValue); err == nil {
			parsedValue = parsedTimeVal
		} else if parsedIntVal, err := strconv.ParseInt(rawValue, 10, 64); err == nil {
			parsedValue = parsedIntVal
		}

		bsonClauses = append(bsonClauses, bson.D{{Key: field, Value: bson.D{{Key: opMapping[opSymbol], Value: parsedValue}}}})
	}

	if len(bsonClauses) == 1 {
		return bsonClauses[0].(bson.D), nil
	}
	return bson.D{{Key: mongoOperator, Value: bsonClauses}}, nil
}

// splitTopLevelClauses separates the DSL filter into top-level clauses and returns the Mongo logical operator
func splitTopLevel(filterExp string) (string, []string) {
	inQuotes := false
	// Track parentheses depth to handle nested expressions
	depth := 0
	var buffer strings.Builder
	var parts []string
	mongoOperator := ""

	for index := 0; index < len(filterExp); {
		currChar := filterExp[index]
		if currChar == '"' {
			// Toggle inside-quote state
			inQuotes = !inQuotes
			buffer.WriteByte(currChar)
			index++
			continue
		}
		if !inQuotes {
			if currChar == '(' {
				depth++
				buffer.WriteByte(currChar)
				index++
				continue
			}
			if currChar == ')' {
				if depth > 0 {
					depth--
				}
				buffer.WriteByte(currChar)
				index++
				continue
			}
			// Split at top-level (depth = 0)
			if depth == 0 {
				// Detect " or " operators at top level
				if strings.HasPrefix(filterExp[index:], " or ") {
					if mongoOperator == "" || mongoOperator == "$or" {
						mongoOperator = "$or"
						parts = append(parts, strings.TrimSpace(buffer.String()))
						buffer.Reset()
						index += len(" or ")
						continue
					}
				}
				if strings.HasPrefix(filterExp[index:], " and ") {
					if mongoOperator == "" || mongoOperator == "$and" {
						mongoOperator = "$and"
						parts = append(parts, strings.TrimSpace(buffer.String()))
						buffer.Reset()
						index += len(" and ")
						continue
					}
				}
			}
		}
		buffer.WriteByte(currChar)
		index++
	}
	parts = append(parts, strings.TrimSpace(buffer.String()))
	// Default to $and if no operator found
	if mongoOperator == "" {
		mongoOperator = "$and"
	}
	return mongoOperator, parts
}
