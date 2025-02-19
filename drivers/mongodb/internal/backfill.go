package driver

import (
	"context"
	"fmt"
	"math"
	"strings"

	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/base"
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
	chunks := stream.GetStateChunks()
	backfillCtx := context.TODO()
	if chunks == nil || chunks.Len() == 0 {
		chunks = types.NewSet[types.Chunk]()
		// chunks state not present means full load
		logger.Infof("starting full load for stream [%s]", stream.ID())

		totalCount, err := m.totalCountInCollection(backfillCtx, collection)
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
			minObjectID := generateMinObjectID(start)
			maxObjecID := generateMinObjectID(end)
			if end.After(last) {
				maxObjecID = generateMinObjectID(last.Add(time.Second))
			}
			start = end
			chunks.Insert(types.Chunk{
				Min: minObjectID,
				Max: maxObjecID,
			})
		}
		// save the chunks state
		stream.SetStateChunks(chunks)

	}
	logger.Infof("Running backfill for %d chunks", chunks.Len())
	// notice: err is declared in return, reason: defer call can access it
	processChunk := func(ctx context.Context, pool *protocol.WriterPool, stream protocol.Stream, collection *mongo.Collection, minStr string, maxStr *string) (err error) {
		threadContext, cancelThread := context.WithCancel(ctx)
		defer cancelThread()
		start, err := primitive.ObjectIDFromHex(minStr)
		if err != nil {
			return fmt.Errorf("invalid min ObjectID: %s", err)
		}

		var end *primitive.ObjectID
		if maxStr != nil {
			max, err := primitive.ObjectIDFromHex(*maxStr)
			if err != nil {
				return fmt.Errorf("invalid max ObjectID: %s", err)
			}
			end = &max
		}

		waitChannel := make(chan error, 1)
		insert, err := pool.NewThread(threadContext, stream, protocol.WithWaitChannel(waitChannel))
		if err != nil {
			return err
		}
		defer func() {
			insert.Close()
			// wait for chunk completion
			err = <-waitChannel
		}()

		opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
		cursorIterationFunc := func() error {
			cursor, err := collection.Aggregate(ctx, generatepipeline(start, end), opts)
			if err != nil {
				return fmt.Errorf("collection.Find: %s", err)
			}
			defer cursor.Close(ctx)

			for cursor.Next(ctx) {
				var doc bson.M
				if _, err = cursor.Current.LookupErr("_id"); err != nil {
					return fmt.Errorf("looking up idProperty: %s", err)
				} else if err = cursor.Decode(&doc); err != nil {
					return fmt.Errorf("backfill decoding document: %s", err)
				}

				handleObjectID(doc)
				exit, err := insert.Insert(types.CreateRawRecord(utils.GetKeysHash(doc, constants.MongoPrimaryID), doc, 0))
				if err != nil {
					return fmt.Errorf("failed to finish backfill chunk: %s", err)
				}
				if exit {
					return nil
				}
			}
			return cursor.Err()
		}
		return base.RetryOnBackoff(m.config.RetryCount, 1*time.Minute, cursorIterationFunc)
	}

	return utils.Concurrent(backfillCtx, chunks.Array(), m.config.MaxThreads, func(ctx context.Context, one types.Chunk, number int) error {
		err := processChunk(backfillCtx, pool, stream, collection, one.Min, &one.Max)
		if err != nil {
			return err
		}
		// remove success chunk from state
		stream.RemoveStateChunk(one)
		return nil
	})
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
func generateMinObjectID(t time.Time) string {
	// Create the ObjectID with the first 4 bytes as the timestamp and the rest 8 bytes as 0x00
	objectID := primitive.NewObjectIDFromTimestamp(t)
	for i := 4; i < 12; i++ {
		objectID[i] = 0x00
	}

	return objectID.Hex()
}

func handleObjectID(doc bson.M) {
	objectID := doc[constants.MongoPrimaryID].(primitive.ObjectID).String()
	doc[constants.MongoPrimaryID] = strings.TrimRight(strings.TrimLeft(objectID, constants.MongoPrimaryIDPrefix), constants.MongoPrimaryIDSuffix)
}
