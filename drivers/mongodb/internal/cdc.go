package driver

import (
    "context"
    "fmt"
    "time"

    "github.com/datazip-inc/olake/constants"
    "github.com/datazip-inc/olake/logger"
    "github.com/datazip-inc/olake/protocol"
    "github.com/datazip-inc/olake/types"
    "github.com/datazip-inc/olake/utils"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readconcern"
)

type CDCDocument struct {
    OperationType string         `json:"operationType"`
    FullDocument  map[string]any `json:"fullDocument"`
    DocumentKey   map[string]any `json:"documentKey"`
    NS            struct {
        DB         string `json:"db"`
        Collection string `json:"coll"`
    } `json:"ns"`
    ClusterTime *bson.Raw `json:"clusterTime,omitempty"`
}

// Rest of the unchanged functions remain the same until changeStreamSync

func (m *Mongo) changeStreamSync(stream protocol.Stream, pool *protocol.WriterPool) error {
    cdcCtx := context.TODO()
    collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
    changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
    pipeline := mongo.Pipeline{
        {{Key: "$match", Value: bson.D{
            {Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
        }}},
    }

    prevResumeToken := stream.GetStateKey(cdcCursorField)
    if prevResumeToken == nil {
        resumeToken, err := m.getCurrentResumeToken(cdcCtx, collection, pipeline)
        if err != nil {
            return err
        }
        if resumeToken != nil {
            prevResumeToken = (*resumeToken).Lookup(cdcCursorField).StringValue()
        }
        if err := m.backfill(stream, pool); err != nil {
            return err
        }
        logger.Infof("backfill done for stream[%s]", stream.ID())
    }

    changeStreamOpts = changeStreamOpts.SetResumeAfter(map[string]any{cdcCursorField: prevResumeToken})
    logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), prevResumeToken)

    cursor, err := collection.Watch(cdcCtx, pipeline, changeStreamOpts)
    if err != nil {
        return fmt.Errorf("failed to open change stream: %s", err)
    }
    defer cursor.Close(cdcCtx)

    insert, err := pool.NewThread(cdcCtx, stream)
    if err != nil {
        return err
    }
    defer insert.Close()

    for cursor.TryNext(cdcCtx) {
        var record CDCDocument
        if err := cursor.Decode(&record); err != nil {
            return fmt.Errorf("error while decoding: %s", err)
        }

        var documentToInsert map[string]any

        switch record.OperationType {
        case "delete":
            // For delete operations, create a new document with metadata
            documentToInsert = map[string]any{
                "_id":           record.DocumentKey["_id"],
                "cdc_type":      record.OperationType,
                "deleted_at":    time.Now().UTC().Format(time.RFC3339),
                "document_key":  record.DocumentKey,
                "namespace":     record.NS,
                "cluster_time": record.ClusterTime,
            }
        default: // insert or update
            if record.FullDocument != nil {
                documentToInsert = record.FullDocument
                documentToInsert["cdc_type"] = record.OperationType
            }
        }

        if documentToInsert != nil {
            handleObjectID(documentToInsert)
            rawRecord := types.CreateRawRecord(utils.GetKeysHash(documentToInsert, constants.MongoPrimaryID), documentToInsert, 0)
            exit, err := insert.Insert(rawRecord)
            if err != nil {
                return err
            }
            if exit {
                return nil
            }
        }

        prevResumeToken = cursor.ResumeToken().Lookup(cdcCursorField).StringValue()
    }

    if err := cursor.Err(); err != nil {
        return fmt.Errorf("failed to iterate change streams cursor: %s", err)
    }

    stream.SetStateKey(cdcCursorField, prevResumeToken)
    return nil
}
