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
	cursor, err := collection.Aggregate(ctx, generatePipeline(chunk.Min, chunk.Max), opts)
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

	// Generate and update chunks
	var retryErr error
	var chunksArray []types.Chunk
	err = abstract.RetryOnBackoff(m.config.RetryCount, 1*time.Minute, func() error {
		chunksArray, retryErr = m.splitChunks(ctx, collection, stream)
		return retryErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed after retry backoff: %s", err)
	}
	return types.NewSet(chunksArray...), nil
}

func (m *Mongo) splitChunks(ctx context.Context, collection *mongo.Collection, stream types.StreamInterface) ([]types.Chunk, error) {
	splitVectorStrategy := func() ([]types.Chunk, error) {
		getID := func(order int) (primitive.ObjectID, error) {
			var doc bson.M
			err := collection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: order}})).Decode(&doc)
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
				{Key: "maxChunkSize", Value: 1},
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
		pipeline := mongo.Pipeline{
			{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
			{{Key: "$bucketAuto", Value: bson.D{
				{Key: "groupBy", Value: "$_id"},
				{Key: "buckets", Value: m.config.MaxThreads * 4},
			}}},
		}

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

	switch m.config.PartitionStrategy {
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
	// Select the database
	err := collection.Database().RunCommand(ctx, command).Decode(&countResult)
	if err != nil {
		return 0, fmt.Errorf("failed to get total count: %s", err)
	}

	return int64(countResult["count"].(int32)), nil
}
func (m *Mongo) fetchExtremes(ctx context.Context, collection *mongo.Collection) (time.Time, time.Time, error) {
	extreme := func(sortby int) (time.Time, error) {
		// Find the first document
		var result bson.M
		// Sort by _id ascending to get the first document
		err := collection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{
			Key: "_id", Value: sortby}})).Decode(&result)
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
		return time.Time{}, time.Time{}, fmt.Errorf("failed to find start: %s", err)
	}

	// provide gap of 10 minutes
	start = start.Add(-time.Minute * 10)
	end = end.Add(time.Minute * 10)
	return start, end, nil
}

func generatePipeline(start, end any) mongo.Pipeline {
	// convert to primitive.ObjectID
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
		// Changed from $lt to $lte to include boundary documents
		andOperation = append(andOperation, bson.D{{
			Key: "_id",
			Value: bson.D{{
				Key:   "$lt",
				Value: end,
			}},
		}})
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
