package driver

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

func (m *Mongo) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	filter, err := m.buildFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %w", err)
	}

	logger.Debugf("Starting backfill from %v to %v with filter: %s", chunk.Min, chunk.Max, filter)

	// hex string bounds are ObjectIDs only when the collection holds ObjectIDs
	objectIDPresent, err := isObjectID(ctx, collection)
	if err != nil {
		return fmt.Errorf("failed to check if _id is ObjectID: %w", err)
	}

	chunkFilter, err := buildChunkFilter(decodeChunkBound(chunk.Min, objectIDPresent), decodeChunkBound(chunk.Max, objectIDPresent))
	if err != nil {
		return fmt.Errorf("failed to build filter for chunk min[%v] max[%v]: %w", chunk.Min, chunk.Max, err)
	}

	cursor, err := collection.Aggregate(ctx, generatePipeline(chunkFilter, filter), opts)
	if err != nil {
		return fmt.Errorf("failed to create cursor: %w", err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.M
		if _, err = cursor.Current.LookupErr("_id"); err != nil {
			return fmt.Errorf("looking up idProperty: %w", err)
		} else if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("backfill decoding document: %w", err)
		}
		// BSON wire-format size of this document, read before the cursor advances.
		docBytes := int64(len(cursor.Current))
		// filter mongo object
		filterMongoObject(doc)
		if err := OnMessage(ctx, doc, docBytes); err != nil {
			return fmt.Errorf("failed to send message to writer: %w", err)
		}
	}
	return cursor.Err()
}

func (m *Mongo) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	recordCount, storageSize, err := m.totalCountAndStorageSizeInCollection(ctx, collection)
	if err != nil {
		return nil, err
	}
	if recordCount == 0 {
		logger.Infof("Collection is empty, nothing to backfill")
		return types.NewSet[types.Chunk](), nil
	}

	logger.Infof("Total expected count for stream %s: %d", stream.ID(), recordCount)
	pool.AddRecordsToSyncStats(recordCount)

	// Generate and update chunks
	var retryErr error
	var chunksArray []types.Chunk
	err = utils.RetryOnBackoff(ctx, m.MaxRetries(), constants.DefaultRetryTimeout, func(ctx context.Context) error {
		chunksArray, retryErr = m.splitChunks(ctx, collection, stream, storageSize)
		return retryErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed after retry backoff: %w", err)
	}

	return types.NewSet(chunksArray...), nil
}

func (m *Mongo) splitChunks(ctx context.Context, collection *mongo.Collection, stream types.StreamInterface, storageSize float64) ([]types.Chunk, error) {
	splitVectorStrategy := func() ([]types.Chunk, error) {
		// NOTE: split-vector runs only when every _id is an ObjectID (see isObjectIDOnly)
		// splitVector is designed to compute chunk boundaries based on the internal format of BSON ObjectIDs
		// (embeds a timestamp and provide monotonically increasing values, useful in sharded clusters).
		// Other _id types (e.g., strings, integers) do not guarantee this ordering or timestamp metadata,
		// leading to uneven splits, overlaps, or gaps.
		logger.Debugf("using split vector strategy for stream: %s", stream.ID())
		getID := func(order int) (primitive.ObjectID, error) {
			var doc bson.M
			objectIDBson := bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: 7}}}}
			err := collection.FindOne(ctx, objectIDBson, options.FindOne().SetSort(bson.D{{Key: "_id", Value: order}})).Decode(&doc)
			if err == mongo.ErrNoDocuments {
				return primitive.NilObjectID, nil
			}
			if err != nil {
				return primitive.NilObjectID, err
			}
			return doc["_id"].(primitive.ObjectID), nil
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

			if err := collection.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
				return nil, fmt.Errorf("failed to run splitVector command: %w", err)
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
			return nil, fmt.Errorf("failed to get chunk boundaries: %w", err)
		}
		// Group every 8 splitVector chunks (~1GB each) into a single larger chunk (~8GB) for consistency with other chunking strategies
		var chunks []types.Chunk
		boundaryLength := len(boundaries)
		for idx := 0; idx < boundaryLength-1; idx += 8 {
			var maxBoundary any
			if idx < (boundaryLength - 9) {
				maxBoundary = boundaries[idx+8].Hex()
			}
			chunks = append(chunks, types.Chunk{
				Min: boundaries[idx].Hex(),
				Max: maxBoundary,
			})
		}
		return chunks, nil
	}
	bucketAutoStrategy := func(storageSize float64) ([]types.Chunk, error) {
		logger.Debugf("using bucket auto strategy for stream: %s", stream.ID())
		// Use $bucketAuto for chunking
		numberOfBuckets := int(math.Ceil(storageSize / float64(constants.EffectiveParquetSize)))
		// NOTE: chunk min and max can be of different _id types, and this is intended.
		// Buckets follow BSON sort order across all _id types, so a bucket can start in one
		// type and end in another, e.g. {min: 1357415547, max: "62a1c5577146a407536dbd74"},
		// and such chunks are saved to state as they are. buildChunkFilter reads them correctly.
		// A nil max on the last chunk means "to the end of every _id type", not just the min's type.
		// Chunking each _id type on its own would keep min and max the same type, but it needs
		// typed bounds in state: without them, a nil max can not tell "end of this type" from
		// "end of every type", and states written before this change rely on the second meaning.
		pipeline := mongo.Pipeline{
			{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
			{{Key: "$bucketAuto", Value: bson.D{
				{Key: "groupBy", Value: "$_id"},
				{Key: "buckets", Value: numberOfBuckets},
			}}},
		}

		opts := options.Aggregate().SetAllowDiskUse(true)
		cursor, err := collection.Aggregate(ctx, pipeline, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to execute bucketAuto aggregation: %w", err)
		}
		defer cursor.Close(ctx)

		var buckets []struct {
			ID struct {
				Min interface{} `bson:"min"`
				Max interface{} `bson:"max"`
			} `bson:"_id"`
			Count int `bson:"count"`
		}

		if err := cursor.All(ctx, &buckets); err != nil {
			return nil, fmt.Errorf("failed to decode bucketAuto results: %w", err)
		}

		var chunks []types.Chunk
		for idx, bucket := range buckets {
			// converts value according to _id string repr.
			minVal, err := reformatID(bucket.ID.Min)
			if err != nil {
				return nil, fmt.Errorf("failed to convert bucket min value to required type: %w", err)
			}
			var maxVal interface{}
			// for last bucket, max will be nil
			if idx != len(buckets)-1 {
				maxVal, err = reformatID(bucket.ID.Max)
				if err != nil {
					return nil, fmt.Errorf("failed to convert bucket max value to required type: %w", err)
				}
			}
			chunks = append(chunks, types.Chunk{
				Min: minVal,
				Max: maxVal,
			})
		}
		return chunks, nil
	}
	timestampStrategy := func() ([]types.Chunk, error) {
		// Time-based strategy implementation
		first, last, err := m.fetchExtremes(ctx, collection)
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

	// splitVector and timestamp chunks cover only ObjectID _id values
	objectIDOnly, err := isObjectIDOnly(ctx, collection)
	if err != nil {
		return nil, fmt.Errorf("failed to check _id types: %w", err)
	}
	if !objectIDOnly {
		logger.Infof("stream %s has _id values that are not ObjectID, using bucket-auto strategy", stream.ID())
		return bucketAutoStrategy(storageSize)
	}

	//  compare case-insensitively
	switch strings.ToLower(strings.TrimSpace(m.config.ChunkingStrategy)) {
	case "timestamp":
		return timestampStrategy()
	default:
		chunks, err := splitVectorStrategy()
		// fallback to bucket-auto strategy if authorization error occurs,
		// or on sharded collections where mongos rejects splitVector with IllegalOperation
		if err != nil && (strings.Contains(err.Error(), "not authorized") ||
			strings.Contains(err.Error(), "CMD_NOT_ALLOWED") ||
			strings.Contains(err.Error(), "IllegalOperation")) {
			logger.Warnf("failed to get chunks via split vector strategy: %s", err)
			return bucketAutoStrategy(storageSize)
		}
		return chunks, err
	}
}

func (m *Mongo) totalCountAndStorageSizeInCollection(ctx context.Context, collection *mongo.Collection) (int64, float64, error) {
	var statsResult bson.M
	command := bson.D{{
		Key:   "collStats",
		Value: collection.Name(),
	}}
	// Select the database
	err := collection.Database().RunCommand(ctx, command).Decode(&statsResult)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch collection stats: %w", err)
	}
	count, err := typeutils.ReformatInt64(statsResult["count"])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to reformat total count from %T to int64: %w", statsResult["count"], err)
	}
	storageSize, err := typeutils.ReformatFloat64(statsResult["storageSize"])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to reformat storage size from %T to float64: %w", statsResult["storageSize"], err)
	}
	return count, storageSize, nil
}

func (m *Mongo) fetchExtremes(ctx context.Context, collection *mongo.Collection) (time.Time, time.Time, error) {
	extreme := func(sortby int) (time.Time, error) {
		// Find the first document
		var result bson.M
		// Sort by _id ascending to get the first document
		err := collection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: sortby}})).Decode(&result)
		if err != nil {
			return time.Time{}, err
		}

		// Extract the _id from the result
		objectID, ok := result["_id"].(primitive.ObjectID)
		if !ok {
			return time.Time{}, fmt.Errorf("failed to cast _id[%v] to ObjectID", objectID)
		}
		return objectID.Timestamp(), nil
	}

	start, err := extreme(1)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find start: %w", err)
	}

	end, err := extreme(-1)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find end: %w", err)
	}

	// provide gap of 10 minutes
	start = start.Add(-time.Minute * 10)
	end = end.Add(time.Minute * 10)
	return start, end, nil
}

// generatePipeline matches the chunk filter and the user filter, sorted by _id.
func generatePipeline(chunkFilter, filter bson.D) mongo.Pipeline {
	andOperation := []bson.D{chunkFilter}
	if len(filter) > 0 {
		andOperation = append(andOperation, filter)
	}

	return mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "$and", Value: andOperation}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}
}

// idTypeBrackets lists the BSON type brackets an _id value can sort into, in BSON sort order.
// Values in one bracket compare by value. A bracket always sorts before the brackets after it.
// Each entry is a $type alias. "number" covers int, long, double and decimal.
// See https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
var idTypeBrackets = []any{"null", "number", bson.A{"string", "symbol"}, "object", "binData", "objectId", "bool", "date", "timestamp"}

// idTypeBracket returns the index in idTypeBrackets of the bracket that v sorts into.
func idTypeBracket(v any) (int, error) {
	switch v.(type) {
	case nil, primitive.Null:
		return 0, nil
	case int, int32, int64, float32, float64, primitive.Decimal128:
		return 1, nil
	case string, primitive.Symbol:
		return 2, nil
	case bson.M, bson.D, map[string]any:
		return 3, nil
	case primitive.Binary:
		return 4, nil
	case primitive.ObjectID:
		return 5, nil
	case bool:
		return 6, nil
	case primitive.DateTime, time.Time:
		return 7, nil
	case primitive.Timestamp:
		return 8, nil
	default:
		return -1, fmt.Errorf("unsupported _id type %T", v)
	}
}

// decodeChunkBound turns a chunk bound read from state back into the value to query with.
// ObjectIDs are stored as hex strings, so a valid hex string becomes an ObjectID when the
// collection holds ObjectIDs. Every other value is used as is.
// TODO: check if we can adopt typed chunk values in state.
func decodeChunkBound(v any, objectIDPresent bool) any {
	if s, ok := v.(string); ok && objectIDPresent {
		if oid, err := primitive.ObjectIDFromHex(s); err == nil {
			return oid
		}
	}
	return v
}

// buildChunkFilter matches every _id from min (inclusive) to max (exclusive) in BSON sort order.
// A nil max means no upper bound.
// MongoDB comparison operators only match values in the bracket of the operand, so a range whose
// bounds sit in different brackets is split into one clause per bracket, each served by the _id index:
// the rest of the min bracket, every bracket in between, and the part of the max bracket below max.
func buildChunkFilter(minVal, maxVal any) (bson.D, error) {
	minBracket, err := idTypeBracket(minVal)
	if err != nil {
		return nil, fmt.Errorf("chunk min: %w", err)
	}

	maxBracket := len(idTypeBrackets)
	if maxVal != nil {
		if maxBracket, err = idTypeBracket(maxVal); err != nil {
			return nil, fmt.Errorf("chunk max: %w", err)
		}
		if maxBracket < minBracket {
			return nil, fmt.Errorf("chunk max %v sorts before min %v", maxVal, minVal)
		}
		// same bracket
		if maxBracket == minBracket {
			return bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: minVal}, {Key: "$lt", Value: maxVal}}}}, nil
		}
	}

	// values of the min type that are greater than or equal to min
	clauses := bson.A{bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: minVal}}}}}
	// every value of each type that sorts between the min type and the max type
	for _, bracket := range idTypeBrackets[minBracket+1 : maxBracket] {
		clauses = append(clauses, bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: bracket}}}})
	}
	// values of the max type that are less than max (skipped for the last chunk, which has no max)
	if maxVal != nil {
		clauses = append(clauses, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: maxVal}}}})
	}
	// a document is in the chunk if it matches any one of the clauses
	return bson.D{{Key: "$or", Value: clauses}}, nil
}

// function to generate ObjectID with the minimum value for a given time
func generateMinObjectID(t time.Time) string {
	// Create the ObjectID with the first 4 bytes as the timestamp and the rest 8 bytes as 0x00
	objectID := primitive.NewObjectIDFromTimestamp(t)
	for i := 4; i < 12; i++ {
		objectID[i] = 0x00
	}
	return objectID.Hex()
}

func buildMongoCondition(isLegacy bool, cond interface{}) bson.D {
	opMap := map[string]string{
		">":  "$gt",
		">=": "$gte",
		"<":  "$lt",
		"<=": "$lte",
		"=":  "$eq",
		"!=": "$ne",
	}
	c := cond.(types.FilterCondition)
	// legacy filter condition
	if isLegacy {
		value := func(field, val string) interface{} {
			if val == "null" {
				return nil
			}

			if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
				val = val[1 : len(val)-1]
			}

			if field == "_id" && len(val) == 24 {
				if oid, err := primitive.ObjectIDFromHex(val); err == nil {
					return oid
				}
			}

			if strings.EqualFold(val, "true") || strings.EqualFold(val, "false") {
				return strings.EqualFold(val, "true")
			}

			if timeVal, err := typeutils.ReformatDate(val, false); err == nil {
				return timeVal
			}

			if intVal, err := typeutils.ReformatInt64(val); err == nil {
				return intVal
			}

			if floatVal, err := typeutils.ReformatFloat64(val); err == nil {
				return floatVal
			}

			return val
		}(c.Column, c.Value.(string))

		return bson.D{
			{Key: c.Column, Value: bson.D{
				{Key: opMap[c.Operator], Value: value},
			}},
		}
	}

	var value interface{}
	if v, ok := c.Value.(string); ok {
		//For string values, attempt type conversion based on field characteristics
		//This handles cases like timestamp strings, ObjectIDs etc.
		if c.Column == "_id" && len(v) == 24 {
			if oid, err := primitive.ObjectIDFromHex(v); err == nil {
				value = oid
			} else {
				value = v
			}
		} else if timeVal, err := typeutils.ReformatDate(v, false); err == nil {
			value = timeVal
		} else {
			value = c.Value
		}
	} else {
		// already typed (nil, bool, int, float, etc.)
		value = c.Value
	}

	return bson.D{
		{Key: c.Column, Value: bson.D{
			{Key: opMap[c.Operator], Value: value},
		}},
	}
}

// buildFilter generates a BSON document for MongoDB by combining threshold conditions with user-defined filter conditions
func (m *Mongo) buildFilter(stream types.StreamInterface) (bson.D, error) {
	thresholdConditions, err := m.ThresholdFilter(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create threshold filter: %w", err)
	}

	filter, isLegacy, err := stream.GetFilter()
	if err != nil {
		return nil, fmt.Errorf("failed to parse stream filter: %w", err)
	}

	var allConditions bson.A
	allConditions = append(allConditions, thresholdConditions...)

	logger.Debugf("all conditions len: %d, filter conditions len: %d", len(allConditions), len(filter.Conditions))

	switch {
	case len(filter.Conditions) == 0:
		return utils.Ternary(len(allConditions) == 0, bson.D{}, bson.D{{Key: "$and", Value: allConditions}}).(bson.D), nil
	case len(filter.Conditions) == 1:
		allConditions = append(allConditions, buildMongoCondition(isLegacy, filter.Conditions[0]))
	case len(filter.Conditions) == 2:
		allConditions = append(allConditions, bson.D{{Key: "$" + filter.LogicalOperator, Value: bson.A{buildMongoCondition(isLegacy, filter.Conditions[0]), buildMongoCondition(isLegacy, filter.Conditions[1])}}})
	}

	return bson.D{{Key: "$and", Value: allConditions}}, nil
}

func reformatID(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case primitive.ObjectID:
		return t.Hex(), nil
	case int32, int64, float64:
		return t, nil
	default:
		// fallback
		return utils.ConvertToString(v), nil
	}
}

func isObjectID(ctx context.Context, collection *mongo.Collection) (bool, error) {
	var doc bson.M
	err := collection.FindOne(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: 7}}}}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// No data
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// isObjectIDOnly reports whether every _id in the collection is an ObjectID.
// ObjectID is one bracket in BSON sort order, so this holds when the smallest and the
// largest _id are both ObjectIDs. Both lookups use the _id index.
func isObjectIDOnly(ctx context.Context, collection *mongo.Collection) (bool, error) {
	for _, order := range []int{1, -1} {
		var doc bson.M
		opts := options.FindOne().SetSort(bson.D{{Key: "_id", Value: order}}).SetProjection(bson.D{{Key: "_id", Value: 1}})
		if err := collection.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
			if err == mongo.ErrNoDocuments {
				return false, nil
			}
			return false, err
		}
		if _, ok := doc["_id"].(primitive.ObjectID); !ok {
			return false, nil
		}
	}
	return true, nil
}
