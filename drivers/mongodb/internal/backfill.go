package driver

import (
	"context"
	"fmt"
	"math"
	"sort"
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
	var chunksArray []types.Chunk
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
		totalCount, err := m.totalCountInCollection(backfillCtx, collection)
		if err != nil {
			return err
		}

		logger.Infof("Total expected count for stream %s are %d", stream.ID(), totalCount)
		logger.Infof("Total documents to sync: %d", totalCount)
		pool.AddRecordsToSync(totalCount)

		for i := 0; i < len(boundaries)-1; i++ {
			chunks.Insert(types.Chunk{
				Min: boundaries[i],
				Max: boundaries[i+1],
			})
		}
		stream.SetStateChunks(chunks)
		chunksArray = chunks.Array()
	} else {
		// TODO: to get estimated time need to update pool.AddRecordsToSync(totalCount) (Can be done via storing some vars in state)
		rawChunkArray := chunks.Array()
		// convert to premitive.ObjectID
		for _, chunk := range rawChunkArray {
			premitiveMinID, _ := primitive.ObjectIDFromHex(chunk.Min.(string))
			premitiveMaxID, _ := primitive.ObjectIDFromHex(chunk.Max.(string))
			chunksArray = append(chunksArray, types.Chunk{
				Min: &premitiveMinID,
				Max: &premitiveMaxID,
			})
		}
	}
	logger.Infof("Running backfill for %d chunks", chunks.Len())
	// notice: err is declared in return, reason: defer call can access it
	processChunk := func(ctx context.Context, chunk types.Chunk, _ int) (err error) {
		threadContext, cancelThread := context.WithCancel(ctx)
		defer cancelThread()

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
			cursor, err := collection.Aggregate(ctx, generatepipeline(chunk.Min, chunk.Max), opts)
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
	// note: there are performance issues with mongodb if chunks are not sorted
	sort.Slice(chunksArray, func(i, j int) bool {
		return chunksArray[i].Min.(string) < chunksArray[j].Min.(string)
	})

	return utils.Concurrent(backfillCtx, chunksArray, m.config.MaxThreads, func(ctx context.Context, one types.Chunk, number int) error {
		batchStartTime := time.Now()
		err := processChunk(backfillCtx, one, number)
		if err != nil {
			return err
		}
		// remove success chunk from state
		stream.RemoveStateChunk(one)
		logger.Debugf("finished %d chunk[%s-%s] in %0.2f seconds", number, one.Min, one.Max, time.Since(batchStartTime).Seconds())
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

func (m *Mongo) getChunkBoundaries(collection *mongo.Collection) ([]primitive.ObjectID, error) {
	minDoc := bson.M{}
	err := collection.FindOne(
		context.TODO(),
		bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}}),
	).Decode(&minDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
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

	command := bson.D{
		{Key: "splitVector", Value: fmt.Sprintf("%s.%s", collection.Database().Name(), collection.Name())},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "maxChunkSize", Value: 1024},
	}

	var result bson.M
	err = collection.Database().RunCommand(context.TODO(), command).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("failed to execute splitVector: %s", err)
	}

	boundaries := []primitive.ObjectID{minID}

	if splitKeys, ok := result["splitKeys"].(bson.A); ok {
		for _, key := range splitKeys {
			if objID, ok := key.(bson.M); ok {
				if id, ok := objID["_id"].(primitive.ObjectID); ok {
					boundaries = append(boundaries, id)
				}
			}
		}
	}

	boundaries = append(boundaries, maxID)
	return boundaries, nil
}

func generatepipeline(start, end any) mongo.Pipeline {
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
