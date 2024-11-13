package driver

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/safego"
	"github.com/datazip-inc/olake/types"
	"github.com/piyushsingariya/relec"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

type Boundry struct {
	StartID primitive.ObjectID  `json:"start"`
	EndID   *primitive.ObjectID `json:"end"`
	end     time.Time
}

func (m *Mongo) backfill(stream protocol.Stream, pool *protocol.WriterPool) error {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Snapshot())).Collection(stream.Name())
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

	// for every 6hr difference ideal density is 10 Seconds
	density := (last.Sub(first) / 6 * time.Hour) * (10 * time.Second)
	concurrency := 50 // default; TODO: decide from MongoDB server resources

	return relec.ConcurrentC(context.TODO(), relec.Yield(func(prev *Boundry) (bool, *Boundry, error) {
		start := first
		if prev != nil {
			start = prev.end
		}
		boundry := &Boundry{
			StartID: *generateMinObjectID(start),
		}

		end := start.Add(density)
		exit := true
		if !end.After(last) {
			exit = false
			boundry.EndID = generateMinObjectID(end)
			boundry.end = end
		}

		return exit, boundry, nil
	}), concurrency, func(ctx context.Context, one *Boundry, number int64) error {
		thread, err := pool.NewThread(ctx, stream)
		if err != nil {
			return err
		}
		defer safego.Close(thread)

		opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(math.Pow10(6)))
		cursor, err := collection.Aggregate(ctx, generatepipeline(one.StartID, one.EndID), opts)
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

			if !safego.Insert(thread, types.Record(doc)) {
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
		Value: collection,
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
