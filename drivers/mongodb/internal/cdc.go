package driver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/destination"
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

func (m *Mongo) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, true // concurrent change streams supported, stream can start after finishing full load
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
	}
	m.streams = streams
	return nil
}

func (m *Mongo) StreamChanges(ctx context.Context, streamIndex int, OnMessage abstract.CDCMsgFn) error {
	stream := m.streams[streamIndex]
	// lastOplogTime is the latest timestamp of any operation applied in the MongoDB cluster
	lastOplogTime, err := m.getClusterOpTime(ctx, m.config.Database)
	if err != nil {
		logger.Warnf("Failed to get cluster op time: %s", err)
		return err
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetMaxAwaitTime(maxAwait)
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	prevResumeToken := m.state.GetCursor(stream.Self(), cdcCursorField)
	if prevResumeToken == nil {
		return fmt.Errorf("resume token not found for stream: %s", stream.ID())
	}

	changeStreamOpts = changeStreamOpts.SetResumeAfter(map[string]any{cdcCursorField: prevResumeToken})
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), prevResumeToken)

	cursor, err := collection.Watch(ctx, pipeline, changeStreamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(ctx)

	for {
		hasNext := cursor.TryNext(ctx)
		if err := cursor.Err(); err != nil {
			return fmt.Errorf("change stream error: %s", err)
		}

		if hasNext {
			if err := m.handleChangeDoc(ctx, cursor, stream, prevResumeToken.(string), OnMessage); err != nil {
				return err
			}
		}

		// Check boundary AFTER emitting
		if err := m.handleStreamCatchup(ctx, cursor, stream, lastOplogTime); err != nil {
			if errors.Is(err, ErrIdleTermination) {
				// graceful termination requested by helper
				logger.Infof("change stream %s caught up to cluster opTime; terminating gracefully", stream.ID())
				return nil
			}
			return err
		}

		if !hasNext {
			// Wait before for a brief pause before the next iteration of the loop
			// TryNext() method behaves differently when connecting through mongos vs. direct replica set connections:TryNext() is non-blocking and may return false immediately even when events exist
			time.Sleep(changeStreamRetryDelay)
		}
	}
}

func (m *Mongo) handleStreamCatchup(_ context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, lastOplogTime primitive.Timestamp) error {
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
	if !lastOplogTime.After(streamOpTime) {
		return ErrIdleTermination
	}

	return nil
}

func (m *Mongo) handleChangeDoc(ctx context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, startingResumeToken string, OnMessage abstract.CDCMsgFn) error {
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
		Position:  startingResumeToken,
	}

	if err := OnMessage(ctx, change); err != nil {
		return err
	}

	if resumeToken := cursor.ResumeToken(); resumeToken != nil {
		m.cdcCursor.Store(stream.ID(), resumeToken.Lookup(cdcCursorField).StringValue())
	}
	return nil
}

func (m *Mongo) PostCDC(ctx context.Context, streamIndex int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		finalToken := m.streams[streamIndex].ID()
		val, ok := m.cdcCursor.Load(finalToken)
		if ok {
			m.state.SetCursor(m.streams[streamIndex].Self(), cdcCursorField, val)
		} else {
			logger.Warnf("no resume token found for stream: %s", finalToken)
		}
		return nil
	}
}

func (m *Mongo) GetCDCPosition() string {
	// MongoDB has per-stream resume tokens, return empty for global
	// Use GetCDCPositionForStream for per-stream position
	return ""
}

// GetCDCStartPosition returns empty for MongoDB (per-stream tokens are used)
func (m *Mongo) GetCDCStartPosition() string {
	return ""
}

func (m *Mongo) GetCDCPositionForStream(streamID string) string {
	val, ok := m.cdcCursor.Load(streamID)
	if ok {
		return val.(string)
	}
	return ""
}

func (m *Mongo) SetNextCDCPosition(position string) {
	// MongoDB has per-stream positions, this is a no-op for global
}

func (m *Mongo) GetNextCDCPosition() string {
	// MongoDB has per-stream positions, no global next position
	return ""
}

func (m *Mongo) SetCurrentCDCPosition(position string) {
	// MongoDB has per-stream positions, this is a no-op for global
}

func (m *Mongo) SetProcessingStreams(streamIDs []string) {
	// MongoDB has per-stream positions, no global processing array needed
}

func (m *Mongo) RemoveProcessingStream(streamID string) {
	// MongoDB has per-stream positions, no global processing array needed
}

func (m *Mongo) GetProcessingStreams() []string {
	// MongoDB has per-stream positions, no global processing array
	return nil
}

func (m *Mongo) SetTargetCDCPosition(position string) {
	// MongoDB has per-stream positions, no global target position
}

func (m *Mongo) GetTargetCDCPosition() string {
	// MongoDB has per-stream positions
	return ""
}

// SaveNextCDCPositionForStream saves current position as next_data before commit (for 2PC)
func (m *Mongo) SaveNextCDCPositionForStream(streamID string) {
	val, ok := m.cdcCursor.Load(streamID)
	if ok {
		// Save current position as next_data in state
		for _, stream := range m.streams {
			if stream.ID() == streamID {
				m.state.SetCursor(stream.Self(), "next_data", val)
				logger.Debugf("Saved next_data for stream %s: %s", streamID, val)
				break
			}
		}
	}
}

// CommitCDCPositionForStream moves next_data to _data after successful commit
func (m *Mongo) CommitCDCPositionForStream(streamID string) {
	for _, stream := range m.streams {
		if stream.ID() == streamID {
			nextData := m.state.GetCursor(stream.Self(), "next_data")
			if nextData != nil {
				// Move next_data to _data
				m.state.SetCursor(stream.Self(), cdcCursorField, nextData)
				// Clear next_data
				m.state.SetCursor(stream.Self(), "next_data", nil)
				logger.Debugf("Committed _data for stream %s: %s", streamID, nextData)
			}
			break
		}
	}
}

// CheckPerStreamRecovery checks if next_data exists for this stream, verifies commit status, and updates or rollbacks.
// This implements 2PC recovery for MongoDB per-stream positions.
func (m *Mongo) CheckPerStreamRecovery(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error {
	nextData := m.state.GetCursor(stream.Self(), "next_data")
	if nextData == nil {
		return nil // No recovery needed for this stream
	}

	currentData := m.state.GetCursor(stream.Self(), cdcCursorField)

	// Generate threadID using _data (the position at which we started the sync)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(currentData.(string))))
	threadID := fmt.Sprintf("%s_%s", stream.ID(), hash)

	// Create temporary writer to check commit status
	writer, err := pool.NewWriter(ctx, stream, destination.WithThreadID(threadID))
	if err != nil {
		return fmt.Errorf("failed to create writer for recovery check: %s", err)
	}

	committed, err := writer.IsThreadCommitted(ctx, threadID)
	if err != nil {
		return fmt.Errorf("failed to check commit status for stream %s: %s", stream.ID(), err)
	}
	if closeErr := writer.Close(ctx); closeErr != nil {
		logger.Warnf("Failed to close recovery check writer for stream %s: %s", stream.ID(), closeErr)
	}

	if committed {
		// Thread committed successfully - move next_data to _data
		logger.Infof("Recovery: Stream %s (thread %s) committed, updating _data to %s", stream.ID(), threadID, nextData)
		m.state.SetCursor(stream.Self(), cdcCursorField, nextData)
		m.state.SetCursor(stream.Self(), "next_data", nil)
	} else {
		// Thread not committed - rollback by removing next_data (keep old _data)
		logger.Infof("Recovery: Stream %s (thread %s) not committed, rolling back (keeping _data=%s)", stream.ID(), threadID, currentData)
		m.state.SetCursor(stream.Self(), "next_data", nil)
	}

	return nil
}

// AcknowledgeCDCPosition - no-op for MongoDB
func (m *Mongo) AcknowledgeCDCPosition(ctx context.Context, position string) error {
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
