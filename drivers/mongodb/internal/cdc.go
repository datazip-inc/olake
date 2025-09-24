package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

// PBRT checkpoint throttle interval (avoid writing state too often)
const (
	pbrtCheckpointInterval = 1 * time.Minute
	maxAwait               = 2 * time.Second
)

var ErrIdleTermination = errors.New("change stream terminated due to idle timeout")

type CDCDocument struct {
	OperationType string              `json:"operationType"`
	FullDocument  map[string]any      `json:"fullDocument"`
	ClusterTime   primitive.Timestamp `json:"clusterTime"`
	WallTime      primitive.DateTime  `json:"wallTime"`
	DocumentKey   map[string]any      `json:"documentKey"`
}

func (m *Mongo) PreCDC(cdcCtx context.Context, streams []types.StreamInterface) error {
	for _, stream := range streams {
		collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
		pipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
			}}},
		}

		prevResumeToken := m.state.GetCursor(stream.Self(), cdcCursorField)
		if prevResumeToken == nil {
			resumeToken, err := m.getCurrentResumeToken(cdcCtx, collection, pipeline)
			if err != nil {
				return err
			}
			if resumeToken != nil {
				prevResumeToken = (*resumeToken).Lookup(cdcCursorField).StringValue()
				m.state.SetCursor(stream.Self(), cdcCursorField, prevResumeToken)
			}
		}
		m.cdcCursor.Store(stream.ID(), prevResumeToken)
	}

	LastOplogTime, err := m.getClusterOpTime(cdcCtx)
	if err != nil {
		logger.Warnf("Failed to get cluster op time: %s", err)
		return err
	}
	m.LastOplogTime = LastOplogTime

	return nil
}

func (m *Mongo) StreamChanges(ctx context.Context, stream types.StreamInterface, OnMessage abstract.CDCMsgFn) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetMaxAwaitTime(maxAwait)
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	// Seed resume token from previously saved cursor (if any)
	resumeToken, ok := m.cdcCursor.Load(stream.ID())
	if !ok {
		return fmt.Errorf("resume token not found for stream: %s", stream.ID())
	}
	changeStreamOpts = changeStreamOpts.SetResumeAfter(map[string]any{cdcCursorField: resumeToken})
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), resumeToken)

	cursor, err := collection.Watch(ctx, pipeline, changeStreamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(ctx)

	lastPbrtCheckpoint := time.Time{}
	var lastDocToken bson.Raw

	for {
		if !cursor.TryNext(ctx) {
			if err := cursor.Err(); err != nil {
				logger.Errorf("Change stream for stream %s terminated with error: %s", stream.ID(), err)
				return fmt.Errorf("change stream error: %s", err)
			}

			// PBRT checkpoint and termination check
			if err := m.handleIdleCheckpoint(ctx, cursor, stream, lastDocToken, &lastPbrtCheckpoint); err != nil {
				if errors.Is(err, ErrIdleTermination) {
					// graceful termination requested by helper
					return nil
				}
				return err
			}
			// Wait before for a brief pause before the next iteration of the loop
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := m.handleChangeDoc(ctx, cursor, stream, OnMessage, &lastDocToken); err != nil {
			return err
		}
	}
}

func (m *Mongo) handleIdleCheckpoint(_ context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, lastDocToken bson.Raw, lastPbrtCheckpoint *time.Time) error {
	finalToken := cursor.ResumeToken()
	if finalToken == nil {
		logger.Warnf("No resume token available for stream %s after TryNext", stream.ID())
		return nil
	}

	tokenVal, err := finalToken.LookupErr(cdcCursorField)
	if err != nil {
		return fmt.Errorf("%s field not found in resume token: %s", cdcCursorField, err)
	}
	tokenValStr := tokenVal.StringValue()

	// Persist PBRT if it differs from last document token and interval elapsed
	if (lastDocToken == nil || !bytes.Equal(finalToken, lastDocToken)) &&
		time.Since(*lastPbrtCheckpoint) > pbrtCheckpointInterval {
		m.cdcCursor.Store(stream.ID(), tokenValStr)
		m.state.SetCursor(stream.Self(), cdcCursorField, tokenValStr)
		*lastPbrtCheckpoint = time.Now()
		logger.Debugf("Persisted PBRT checkpoint for stream %s with token %s", stream.ID(), tokenValStr)
	}
	streamOpTime, err := decodeResumeTokenOpTime(tokenValStr)
	if err != nil {
		logger.Warnf("Failed to decode resume token for stream %s: %s", stream.ID(), err)
		return nil
	}

	// If stream is caught up -> request graceful termination
	if !m.LastOplogTime.After(streamOpTime) {
		logger.Infof("Change stream %s caught up to cluster opTime; terminating gracefully", stream.ID())
		return ErrIdleTermination
	}

	return nil
}

func (m *Mongo) handleChangeDoc(ctx context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, OnMessage abstract.CDCMsgFn, lastDocToken *bson.Raw) error {
	var record CDCDocument
	if err := cursor.Decode(&record); err != nil {
		return fmt.Errorf("error while decoding: %s", err)
	}

	if record.OperationType == "delete" {
		// replace full document(null) with documentKey
		record.FullDocument = record.DocumentKey
	}

	filterMongoObject(record.FullDocument)

	ts := utils.Ternary(record.WallTime != 0,
		record.WallTime.Time(), // millisecond precision
		time.UnixMilli(int64(record.ClusterTime.T)*1000+int64(record.ClusterTime.I)), // seconds only
	).(time.Time)

	change := abstract.CDCChange{
		Stream:    stream,
		Timestamp: ts,
		Data:      record.FullDocument,
		Kind:      record.OperationType,
	}

	if rt := cursor.ResumeToken(); rt != nil {
		m.cdcCursor.Store(stream.ID(), rt.Lookup(cdcCursorField).StringValue())
		*lastDocToken = rt
	}
	if err := OnMessage(ctx, change); err != nil {
		return fmt.Errorf("failed to process message: %s", err)
	}
	return nil
}

func (m *Mongo) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool) error {
	if noErr {
		val, ok := m.cdcCursor.Load(stream.ID())
		if ok {
			m.state.SetCursor(stream.Self(), cdcCursorField, val)
		} else {
			logger.Warnf("no resume token found for stream: %s", stream.ID())
		}
	}
	return nil
}

func (m *Mongo) getCurrentResumeToken(cdcCtx context.Context, collection *mongo.Collection, pipeline []bson.D) (*bson.Raw, error) {
	cursor, err := collection.Watch(cdcCtx, pipeline, options.ChangeStream().SetMaxAwaitTime(maxAwait))
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(cdcCtx)

	resumeToken := cursor.ResumeToken()
	return &resumeToken, nil
}

// getClusterOpTime retrieves the latest cluster operation time from MongoDB.
// It first tries the modern 'hello' command and falls back to 'isMaster' for older servers.
func (m *Mongo) getClusterOpTime(ctx context.Context) (primitive.Timestamp, error) {
	var opTime primitive.Timestamp

	// Helper to run a command and return raw result
	runCmd := func(cmd bson.M) (bson.Raw, error) {
		return m.client.Database("admin").RunCommand(ctx, cmd).Raw()
	}

	// Try 'hello' command first
	raw, err := runCmd(bson.M{"hello": 1})
	if err != nil {
		logger.Debug("running 'hello' command failed, falling back to 'isMaster' command")
		raw, err = runCmd(bson.M{"isMaster": 1})
		if err != nil {
			return opTime, fmt.Errorf("fetching 'operationTime' failed: both 'hello' and 'isMaster' failed: %s", err)
		}
	}

	// Extract 'operationTime' field
	opRaw, err := raw.LookupErr("operationTime")
	if err != nil {
		return opTime, fmt.Errorf("operationTime field missing in server response: %s", err)
	}
	if err := opRaw.Unmarshal(&opTime); err != nil {
		return opTime, fmt.Errorf("failed to unmarshal operationTime: %s", err)
	}

	// Sanity check: zero timestamp
	if opTime.T == 0 && opTime.I == 0 {
		return primitive.Timestamp{}, fmt.Errorf("operationTime returned zero timestamp")
	}

	return opTime, nil
}

// decodeResumeTokenOpTime extracts a stable, sortable MongoDB resume token timestamp (4-byte timestamp + 4-byte increment).
func decodeResumeTokenOpTime(dataStr string) (primitive.Timestamp, error) {
	dataBytes, err := hex.DecodeString(dataStr)
	if err != nil || len(dataBytes) < 9 {
		return primitive.Timestamp{}, fmt.Errorf("invalid resume token")
	}
	return primitive.Timestamp{
		T: binary.BigEndian.Uint32(dataBytes[1:5]),
		I: binary.BigEndian.Uint32(dataBytes[5:9]),
	}, nil
}
