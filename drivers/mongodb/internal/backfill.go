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
		// Initialize chunks set
		chunks = types.NewSet[types.Chunk]()
		// chunks state not present means full load
		logger.Infof("starting full load for stream [%s]", stream.ID())
		recordCount, err := m.totalCountInCollection(backfillCtx, collection)
		if err != nil {
			return err
		}
		// Check if collection is empty first to avoid unnecessary work
		if recordCount == 0 {
			logger.Infof("collection is empty, nothing to backfill")
			return nil
		}
		// chunks state not present means full load
		logger.Infof("starting full load for stream [%s]", stream.ID())
		logger.Infof("Total expected count for stream %s are %d", stream.ID(), recordCount)
		pool.AddRecordsToSync(recordCount)
		// Generate chunks and directly insert into the set
		err = m.splitChunks(collection, stream, *chunks)
		if err != nil {
			return err
		}

		// Update stream state
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

	logger.Infof("Running backfill for %d chunks", len(chunksArray))
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
			cursor, err := collection.Aggregate(ctx, generatePipeline(chunk.Min, chunk.Max), opts)
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
		return chunksArray[i].Min.(*primitive.ObjectID).Hex() < chunksArray[j].Min.(*primitive.ObjectID).Hex()
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

func (m *Mongo) splitChunks(collection *mongo.Collection, stream protocol.Stream, chunks types.Set[types.Chunk]) error {
	splitVectorStrategy := func() error {
		// Split-vector strategy implementation
		// get chunk boundaries
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
				Min: &boundaries[i],
				Max: &boundaries[i+1],
			})
		}
		return nil
	}

	timestampStrategy := func() error {
		// Time-based strategy implementation
		first, last, err := m.fetchExtremes(collection)
		if err != nil {
			return err
		}

		logger.Infof("Extremes of Stream %s are start: %s \t end:%s", stream.ID(), first, last)
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
			maxObjectID := generateMinObjectID(end)
			if end.After(last) {
				maxObjectID = generateMinObjectID(last.Add(time.Second))
			}
			start = end
			chunks.Insert(types.Chunk{
				Min: minObjectID,
				Max: maxObjectID,
			})
		}
		return nil
	}

	switch m.config.PartitionStrategy {
	case "timestamp":
		return timestampStrategy()
	default:
		return splitVectorStrategy()
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

func (m *Mongo) getChunkBoundaries(collection *mongo.Collection) ([]*primitive.ObjectID, error) {
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

	boundaries := []*primitive.ObjectID{&minID}

	if splitKeys, ok := result["splitKeys"].(bson.A); ok {
		for _, key := range splitKeys {
			if objID, ok := key.(bson.M); ok {
				if id, ok := objID["_id"].(primitive.ObjectID); ok {
					boundaries = append(boundaries, &id)
				}
			}
		}
	}

	boundaries = append(boundaries, &maxID)
	return boundaries, nil
}

func generatePipeline(start, end any) mongo.Pipeline {
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
				Key:   "$lte",
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
