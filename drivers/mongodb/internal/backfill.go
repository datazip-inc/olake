package driver

import (
	"context"
	"fmt"
	"math"
	"strings"

	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

func (m *Mongo) backfill(stream protocol.Stream, pool *protocol.WriterPool) error {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	var chunks []types.Chunk
	failedChunks := stream.GetChunksFromStreamState()
	if len(failedChunks) > 0 {
		//process failed chunks
		logger.Infof("Processing %d failed chunks for stream [%s]", len(failedChunks), stream.ID())
		chunks = append(chunks, failedChunks...)
	}
	if len(chunks) == 0 {
		logger.Infof("starting full load for stream [%s]", stream.ID())

		totalCount, err := m.totalCountInCollection(collection)
		if err != nil {
			return err
		}

		first, last, err := m.fetchExtremes(collection)
		if err != nil {
			return err
		}

		logger.Infof("Extremes of Stream %s are start: %s \t end:%s", stream.ID(), first, last)
		logger.Infof("Total expected count for stream %s are %d", stream.ID(), totalCount)

		timeDiff := last.Sub(first).Hours() / 6
		if timeDiff < 1 {
			timeDiff = 1
		}
		// for every 6hr difference ideal density is 10 Seconds
		density := time.Duration(timeDiff) * (10 * time.Second)
		start := first
		for start.Before(last) {
			end := start.Add(density)
			var boundry types.Chunk
			if end.After(last) {
				boundry = types.Chunk{
					Min: *generateMinObjectID(start),
					Max: nil,
				}
				logger.Info("scheduling last full load chunk query!")
				logger.Info("total chunks are :", len(chunks))
			} else {
				boundry = types.Chunk{
					Min: *generateMinObjectID(start),
					Max: generateMinObjectID(end),
				}
			}
			chunks = append(chunks, boundry)
			chunk := types.Chunk{
				Status: "scheduled",
				Min:    boundry.Min,
				Max:    boundry.Max,
			}
			stream.AppendChunksToStreamState(chunk)
			start = end
		}

	}

	return utils.ConcurrentC(context.TODO(), utils.Yield(func(prev *types.Chunk) (bool, *types.Chunk, error) {
		if len(chunks) == 0 {
			return true, nil, nil
		}
		nextChunk := chunks[0]
		chunks = chunks[1:]
		return false, &nextChunk, nil
	}), m.config.MaxThreads, func(ctx context.Context, one *types.Chunk, number int64) error {
		return m.processChunk(ctx, pool, stream, collection, one.Min, one.Max)
	})

}

func (m *Mongo) processChunk(ctx context.Context, pool *protocol.WriterPool, stream protocol.Stream, collection *mongo.Collection, start primitive.ObjectID, end *primitive.ObjectID) error {
	threadContext, cancelThread := context.WithCancel(ctx)
	defer cancelThread()

	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
	cursor, err := collection.Aggregate(ctx, generatepipeline(start, end), opts)
	if err != nil {
		return fmt.Errorf("collection.Find: %s", err)
	}
	defer cursor.Close(ctx)

	waitChannel := make(chan struct{})
	defer func() {
		if stream.GetSyncMode() == types.CDC {
			// only wait in cdc mode
			// make sure it get called after insert.Close()
			<-waitChannel
		}
	}()

	insert, err := pool.NewThread(threadContext, stream, protocol.WithWaitChannel(waitChannel))
	if err != nil {
		return err
	}
	defer insert.Close()

	for cursor.Next(ctx) {
		var doc bson.M
		if _, err = cursor.Current.LookupErr("_id"); err != nil {
			return fmt.Errorf("looking up idProperty: %s", err)
		} else if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("backfill decoding document: %s", err)
		}

			handleObjectID(doc)
			exit, err := insert.Insert(types.Record(doc))
			if err != nil {
				return fmt.Errorf("failed to finish backfill chunk %d: %s", number, err)
			}
			if exit {
				return nil
			}
		}
		return cursor.Err()
	})

}

func (m *Mongo) totalCountInCollection(collection *mongo.Collection) (int64, error) {
	var countResult bson.M
	command := bson.D{{
		Key:   "collStats",
		Value: collection.Name(),
	}}
	// Select the database
	err := collection.Database().RunCommand(context.TODO(), command).Decode(&countResult)
	if err != nil {
		return 0, fmt.Errorf("failed to get total count: %s", err)
	}

	return int64(countResult["count"].(int32)), nil
}

func (m *Mongo) fetchExtremes(collection *mongo.Collection) (time.Time, time.Time, error) {
	extreme := func(sortby int) (time.Time, error) {
		// Find the first document
		var result bson.M
		// Sort by _id ascending to get the first document
		err := collection.FindOne(context.Background(), bson.D{}, options.FindOne().SetSort(bson.D{{
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

func generatepipeline(start primitive.ObjectID, end *primitive.ObjectID) mongo.Pipeline {
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
				Value: *end,
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
func generateMinObjectID(t time.Time) *primitive.ObjectID {
	// Create the ObjectID with the first 4 bytes as the timestamp and the rest 8 bytes as 0x00
	objectID := primitive.NewObjectIDFromTimestamp(t)
	for i := 4; i < 12; i++ {
		objectID[i] = 0x00
	}

	return &objectID
}

func handleObjectID(doc bson.M) {
	objectID := doc[constants.MongoPrimaryID].(primitive.ObjectID).String()
	doc[constants.MongoPrimaryID] = strings.TrimRight(strings.TrimLeft(objectID, constants.MongoPrimaryIDPrefix), constants.MongoPrimaryIDSuffix)
}
