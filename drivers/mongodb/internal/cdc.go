package driver

import (
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

const (
	maxAwait               = 2 * time.Second
	changeStreamRetryDelay = 500 * time.Millisecond
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

	// TODO:move it to stream change function
	lastOplogTime, err := m.getClusterOpTime(cdcCtx, m.config.Database)
	if err != nil {
		logger.Warnf("Failed to get cluster op time: %s", err)
		return err
	}
	m.LastOplogTime = lastOplogTime
	m.streams = streams
	return nil
}

func (m *Mongo) StreamChanges(ctx context.Context, streamIndex int, OnMessage abstract.CDCMsgFn) error {
	stream := m.streams[streamIndex]
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

	for {
		if !cursor.TryNext(ctx) {
			if err := cursor.Err(); err != nil {
				return fmt.Errorf("change stream error: %s", err)
			}

			// PBRT checkpoint and termination check
			if err := m.handleIdleCheckpoint(ctx, cursor, stream); err != nil {
				if errors.Is(err, ErrIdleTermination) {
					// graceful termination requested by helper
					logger.Infof("change stream %s caught up to cluster opTime; terminating gracefully", stream.ID())
					return nil
				}
				return err
			}
			// Wait before for a brief pause before the next iteration of the loop
			time.Sleep(changeStreamRetryDelay)
			continue
		}

		if err := m.handleChangeDoc(ctx, cursor, stream, OnMessage); err != nil {
			return err
		}
	}
}

func (m *Mongo) handleIdleCheckpoint(_ context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface) error {
	finalToken := cursor.ResumeToken()
	if finalToken == nil {
		return fmt.Errorf("no resume token available for stream %s after TryNext", stream.ID())
	}

	rawToken, err := finalToken.LookupErr(cdcCursorField)
	if err != nil {
		return fmt.Errorf("%s field not found in resume token: %s", cdcCursorField, err)
	}
	token := rawToken.StringValue()

	// check pointing post batch resume token
	m.cdcCursor.Store(stream.ID(), token)

	streamOpTime, err := decodeResumeTokenOpTime(token)
	if err != nil {
		return fmt.Errorf("failed to decode resume token for stream %s: %s", stream.ID(), err)
	}

	// If stream is caught up -> request graceful termination
	if !m.LastOplogTime.After(streamOpTime) {
		return ErrIdleTermination
	}

	return nil
}

func (m *Mongo) handleChangeDoc(ctx context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, OnMessage abstract.CDCMsgFn) error {
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

	if resumeToken := cursor.ResumeToken(); resumeToken != nil {
		m.cdcCursor.Store(stream.ID(), resumeToken.Lookup(cdcCursorField).StringValue())
	}
	return OnMessage(ctx, change)
}

func (m *Mongo) PostCDC(ctx context.Context, streamIndex int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		val, ok := m.cdcCursor.Load(m.streams[streamIndex].ID())
		if ok {
			m.state.SetCursor(m.streams[streamIndex].Self(), cdcCursorField, val)
		} else {
			logger.Warnf("no resume token found for stream: %s", m.streams[streamIndex].ID())
		}
		return nil
	}
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
func (m *Mongo) getClusterOpTime(ctx context.Context, dbName string) (primitive.Timestamp, error) {
	var opTime primitive.Timestamp

	// Helper to run a command and return raw result
	runCmd := func(cmd bson.M) (bson.Raw, error) {
		return m.client.Database(dbName).RunCommand(ctx, cmd).Raw()
	}

	// Try 'hello' command first
	raw, err := runCmd(bson.M{"hello": 1})
	if err != nil {
		logger.Debug("failed to run 'hello' command, falling back to 'isMaster' command")
		raw, err = runCmd(bson.M{"isMaster": 1})
		if err != nil {
			return opTime, fmt.Errorf("failed to fetch 'operationTime' from 'isMaster' command: both 'hello' and 'isMaster' failed: %s", err)
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
