package driver

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

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

func (m *Mongo) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) (err error) {
	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	filter, err := buildFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	// check for _id type
	isObjID, err := isObjectID(ctx, collection)
	if err != nil {
		return fmt.Errorf("failed to check if _id is ObjectID: %s", err)
	}

	cursor, err := collection.Aggregate(ctx, generatePipeline(chunk.Min, chunk.Max, filter, isObjID), opts)
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
		// filter mongo object
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

	// build filter
	filter, err := buildFilter(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter during chunk splitting: %s", err)
	}

	// check for _id type
	isObjID, err := isObjectID(ctx, collection)
	if err != nil {
		return nil, fmt.Errorf("failed to check if _id is ObjectID: %s", err)
	}
	logger.Infof("_id is %s ObjectID", utils.Ternary(isObjID, "", "not").(string))

	// Generate and update chunks
	var retryErr error
	var chunksArray []types.Chunk
	err = abstract.RetryOnBackoff(m.config.RetryCount, 1*time.Minute, func() error {
		chunksArray, retryErr = m.splitChunks(ctx, collection, stream, filter, isObjID)
		return retryErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed after retry backoff: %s", err)
	}
	return types.NewSet(chunksArray...), nil
}

func (m *Mongo) splitChunks(ctx context.Context, collection *mongo.Collection, stream types.StreamInterface, filter bson.D, isObjID bool) ([]types.Chunk, error) {
	splitVectorStrategy := func() ([]types.Chunk, error) {
		getID := func(order int) (primitive.ObjectID, error) {
			var doc bson.M
			err := collection.FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "_id", Value: order}})).Decode(&doc)
			if err == mongo.ErrNoDocuments {
				return primitive.NilObjectID, nil
			}
			if err != nil {
				return primitive.NilObjectID, err
			}
			id, ok := doc["_id"].(primitive.ObjectID)
			if !ok {
				return primitive.NilObjectID, fmt.Errorf("multiple type _id exist")
			}
			return id, nil
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

			if len(filter) > 0 {
				cmd = append(cmd, bson.E{Key: "filter", Value: filter})
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
		logger.Infof("using bucket auto strategy for stream: %s", stream.ID())
		// Use $bucketAuto for chunking
		pipeline := mongo.Pipeline{}
		if len(filter) > 0 {
			pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
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
				Min interface{} `bson:"min"`
				Max interface{} `bson:"max"`
			} `bson:"_id"`
			Count int `bson:"count"`
		}

		if err := cursor.All(ctx, &buckets); err != nil {
			return nil, fmt.Errorf("failed to decode bucketAuto results: %s", err)
		}

		var chunks []types.Chunk
		for _, bucket := range buckets {
			// converts value according to _id string repr.
			min, err := reformatID(bucket.ID.Min)
			if err != nil {
				return nil, fmt.Errorf("failed to convert bucket min value to required type: %s", err)
			}
			max, err := reformatID(bucket.ID.Max)
			if err != nil {
				return nil, fmt.Errorf("failed to convert bucket max value to required type: %s", err)
			}
			chunks = append(chunks, types.Chunk{
				Min: min,
				Max: max,
			})
		}
		if len(buckets) > 0 {
			max, err := reformatID(buckets[len(buckets)-1].ID.Max)
			if err != nil {
				return nil, fmt.Errorf("failed to convert last bucket max value to required type: %s", err)
			}
			chunks = append(chunks, types.Chunk{
				Min: max,
				Max: nil,
			})
		}

		return chunks, nil
	}

	timestampStrategy := func() ([]types.Chunk, error) {
		// Time-based strategy implementation
		first, last, err := m.fetchExtremes(ctx, collection, filter)
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
		if !isObjID {
			return bucketAutoStrategy()
		}
		// Not using splitVector strategy when _id is not an ObjectID:
		// splitVector is designed to compute chunk boundaries based on the internal format of BSON ObjectIDs
		// (embeds a timestamp and provide monotonically increasing values, useful in sharded clusters).
		// Other _id types (e.g., strings, integers) do not guarantee this ordering or timestamp metadata,
		// leading to uneven splits, overlaps, or gaps.
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
	// Select the database
	err := collection.Database().RunCommand(ctx, command).Decode(&countResult)
	if err != nil {
		return 0, fmt.Errorf("failed to get total count: %s", err)
	}
	return typeutils.ReformatInt64(countResult["count"])
}

func (m *Mongo) fetchExtremes(ctx context.Context, collection *mongo.Collection, filter bson.D) (time.Time, time.Time, error) {
	extreme := func(sortby int) (time.Time, error) {
		// Find the first document
		var result bson.M
		// Sort by _id ascending to get the first document
		err := collection.FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "_id", Value: sortby}})).Decode(&result)
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
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find start: %s", err)
	}

	end, err := extreme(-1)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find end: %s", err)
	}

	// provide gap of 10 minutes
	start = start.Add(-time.Minute * 10)
	end = end.Add(time.Minute * 10)
	return start, end, nil
}

func generatePipeline(start, end any, filter bson.D, isObjID bool) mongo.Pipeline {
	var andOperation []bson.D

	if isObjID {
		// convert to primitive.ObjectID
		start, _ = primitive.ObjectIDFromHex(start.(string))
		if end != nil {
			end, _ = primitive.ObjectIDFromHex(end.(string))
		}

		andOperation = append(andOperation,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: 7}}}},
		)
	}

	andOperation = append(andOperation,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: start}}}},
	)
	if end != nil {
		// Changed from $lte to $lt to remove collision between boundaries
		andOperation = append(andOperation,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: end}}}},
		)
	}

	if len(filter) > 0 {
		andOperation = append(andOperation, filter)
	}

	// Define the aggregation pipeline
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

// function to generate ObjectID with the minimum value for a given time
func generateMinObjectID(t time.Time) string {
	// Create the ObjectID with the first 4 bytes as the timestamp and the rest 8 bytes as 0x00
	objectID := primitive.NewObjectIDFromTimestamp(t)
	for i := 4; i < 12; i++ {
		objectID[i] = 0x00
	}
	return objectID.Hex()
}

func buildMongoCondition(cond types.Condition) bson.D {
	opMap := map[string]string{
		">":  "$gt",
		">=": "$gte",
		"<":  "$lt",
		"<=": "$lte",
		"=":  "$eq",
		"!=": "$ne",
	}
	//TODO: take val as any type
	value := func(field, val string) interface{} {
		// Handle unquoted null
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
		if strings.ToLower(val) == "true" || strings.ToLower(val) == "false" {
			return strings.ToLower(val) == "true"
		}
		if timeVal, err := typeutils.ReformatDate(val); err == nil {
			return timeVal
		}
		if intVal, err := typeutils.ReformatInt64(val); err == nil {
			return intVal
		}
		if floatVal, err := typeutils.ReformatFloat64(val); err == nil {
			return floatVal
		}
		return val
	}(cond.Column, cond.Value)
	return bson.D{{Key: cond.Column, Value: bson.D{{Key: opMap[cond.Operator], Value: value}}}}
}

// buildFilter generates a BSON document for MongoDB
func buildFilter(stream types.StreamInterface) (bson.D, error) {
	filter, err := stream.GetFilter()
	if err != nil {
		return nil, fmt.Errorf("failed to parse stream filter: %s", err)
	}

	if len(filter.Conditions) == 0 {
		return bson.D{}, nil
	}

	switch {
	case len(filter.Conditions) == 0:
		return bson.D{}, nil
	case len(filter.Conditions) == 1:
		return buildMongoCondition(filter.Conditions[0]), nil
	default:
		return bson.D{{Key: "$" + filter.LogicalOperator, Value: bson.A{buildMongoCondition(filter.Conditions[0]), buildMongoCondition(filter.Conditions[1])}}}, nil
	}
}

func reformatID(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case primitive.ObjectID:
		return t.Hex(), nil
	case int32, int64:
		return t, nil
	default:
		// fallback
		return utils.ConvertToString(v), nil
	}
}

func isObjectID(ctx context.Context, collection *mongo.Collection) (bool, error) {
	var doc bson.M
	err := collection.FindOne(ctx, bson.D{}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// No data
			return false, nil
		}
		return false, err
	}
	idVal, ok := doc["_id"]
	if !ok {
		return false, fmt.Errorf("no _id field found")
	}
	_, isObjID := idVal.(primitive.ObjectID)
	return isObjID, nil
}
