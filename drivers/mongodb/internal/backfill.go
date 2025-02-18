package driver

import (
	"context"
	"fmt"
	"math"
	"strings"

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
	chunks := stream.GetStateChunks()
	if chunks == nil || chunks.Len() == 0 {
		chunks = types.NewSet[types.Chunk]()
		// chunks state not present means full load
		logger.Infof("starting full load for stream [%s]", stream.ID())

		boundaries, err := m.getChunkBoundaries(collection)
		if err != nil {
			return fmt.Errorf("failed to get chunk boundaries: %s", err)
		}
		if len(boundaries) == 0 {
			logger.Infof("collection is empty, nothing to backfill")
			return nil
		}

		for i := 0; i < len(boundaries)-1; i++ {
			chunks.Insert(types.Chunk{
				Min: boundaries[i],
				Max: boundaries[i+1],
			})
		}
		// save the chunks state
		stream.SetStateChunks(chunks)
	}
	logger.Infof("Running backfill for %d chunks", chunks.Len())
	// notice: err is declared in return, reason: defer call can access it
	processChunk := func(ctx context.Context, pool *protocol.WriterPool, stream protocol.Stream, collection *mongo.Collection, minStr string, maxStr *string) error {
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

		opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
		cursor, err := collection.Aggregate(ctx, generatepipeline(start, end), opts)
		if err != nil {
			return fmt.Errorf("collection.Aggregate: %s", err)
		}
		defer cursor.Close(ctx)

		waitChannel := make(chan error, 1)
		insert, err := pool.NewThread(threadContext, stream, protocol.WithWaitChannel(waitChannel))
		if err != nil {
			return err
		}
		defer func() {
			insert.Close()
			err = <-waitChannel
		}()

		for cursor.Next(ctx) {
			var doc bson.M
			if err = cursor.Decode(&doc); err != nil {
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

	return utils.Concurrent(context.TODO(), chunks.Array(), chunks.Len(), func(ctx context.Context, one types.Chunk, number int) error {
		err := processChunk(ctx, pool, stream, collection, one.Min, &one.Max)
		if err != nil {
			return err
		}
		// remove success chunk from state
		stream.RemoveStateChunk(one)
		return nil
	})
}

func (m *Mongo) getChunkBoundaries(collection *mongo.Collection) ([]string, error) {
	// First get min and max _id to ensure we have complete coverage
	minDoc := bson.M{}
	err := collection.FindOne(
		context.TODO(),
		bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}}),
	).Decode(&minDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil // Empty collection
		}
		return nil, fmt.Errorf("failed to get minimum _id: %s", err)
	}

	maxDoc := bson.M{}
	err = collection.FindOne(
		context.TODO(),
		bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&maxDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to get maximum _id: %s", err)
	}

	minID := minDoc["_id"].(primitive.ObjectID)
	maxID := maxDoc["_id"].(primitive.ObjectID)

	// Execute splitVector with a force parameter to get desired number of chunks
	command := bson.D{
		{Key: "splitVector", Value: fmt.Sprintf("%s.%s", collection.Database().Name(), collection.Name())},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "force", Value: true},
		{Key: "maxSplits", Value: m.config.MaxThreads - 1}, // -1 because n splits create n+1 chunks
		{Key: "bounds", Value: bson.A{minID, maxID}},
	}

	var result bson.M
	err = collection.Database().RunCommand(context.TODO(), command).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("failed to execute splitVector: %s", err)
	}

	boundaries := []string{minID.Hex()}

	if splitKeys, ok := result["splitKeys"].(bson.A); ok {
		for _, key := range splitKeys {
			if objID, ok := key.(bson.M); ok {
				if id, ok := objID["_id"].(primitive.ObjectID); ok {
					boundaries = append(boundaries, id.Hex())
				}
			}
		}
	}

	boundaries = append(boundaries, maxID.Hex())

	// Ensure we have at least one chunk (min to max)
	if len(boundaries) < 2 {
		boundaries = []string{minID.Hex(), maxID.Hex()}
	}

	return boundaries, nil
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

func handleObjectID(doc bson.M) {
	objectID := doc[constants.MongoPrimaryID].(primitive.ObjectID).String()
	doc[constants.MongoPrimaryID] = strings.TrimRight(strings.TrimLeft(objectID, constants.MongoPrimaryIDPrefix), constants.MongoPrimaryIDSuffix)
}
