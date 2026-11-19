package driver

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

const (
	maxAwait               = 2 * time.Second
	changeStreamRetryDelay = 500 * time.Millisecond
	CDCResumeToken         = "_cdc_resume_token" //nolint:gosec // false positive: DB field name MongoDB resume token
)

var ErrIdleTermination = errors.New("change stream terminated due to idle timeout")

type CDCDocument struct {
	OperationType            string              `json:"operationType"`
	FullDocument             map[string]any      `json:"fullDocument"`
	FullDocumentBeforeChange map[string]any      `json:"fullDocumentBeforeChange"`
	ClusterTime              primitive.Timestamp `json:"clusterTime"`
	WallTime                 primitive.DateTime  `json:"wallTime"`
	DocumentKey              map[string]any      `json:"documentKey"`
}

func (m *Mongo) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, true // concurrent change streams supported, stream can start after finishing full load
}

func (m *Mongo) PreCDC(cdcCtx context.Context, streams []types.StreamInterface) error {
	for _, stream := range streams {
		collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
		pipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace", "delete"}}}},
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
				prevTypeBits := resumeTokenTypeBits(*resumeToken)
				m.state.SetCursor(stream.Self(), cdcTypeBitsField, prevTypeBits)
				m.state.SetCursor(stream.Self(), cdcCursorField, prevResumeToken)
			}
		}
	}
	m.streams = streams
	return nil
}

func (m *Mongo) StreamChanges(ctx context.Context, streamIndex int, metadataStates map[string]any, OnMessage abstract.CDCMsgFn) (any, error) {
	stream := m.streams[streamIndex]
	mtState := metadataStates[stream.ID()]
	prevResumeToken := m.state.GetCursor(stream.Self(), cdcCursorField)
	if prevResumeToken == nil {
		return nil, errs.Precondition(errs.StateInvalid, codeResumeTokenMissing,
			fmt.Errorf("resume token not found for stream: %s", stream.ID()))
	}
	//   metadata > state  →  metadata is further ahead (crash-recovery path: metadata
	//                         was committed to the destination but state write failed).
	//                         Use the metadata token so we don't re-read already-written events.
	//   state >= metadata →  state is current or ahead; read forward normally.
	if mtState != nil {
		// metadata is ahead: skip reading this sync and only advance state to the metadata resume token.
		if typeutils.Compare(prevResumeToken, mtState) < 0 {
			logger.Infof("Stream[%s] metadata ahead of state, using metadata resume token for recovery", stream.ID())
			m.cdcCursor.Store(stream.ID(), mtState)
			// the destination keeps only _data, so the type bits of this token come from the server
			m.cdcCursor.Store(typeBitsKey(stream.ID()), m.lookupTypeBits(ctx, stream, mtState))
			return mtState, nil
		}
	}

	// lastOplogTime is the latest timestamp of any operation applied in the MongoDB cluster
	lastOplogTime, err := m.getClusterOpTime(ctx, m.config.Database)
	if err != nil {
		logger.Warnf("Failed to get cluster op time: %s", err)
		return nil, err
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace", "delete"}}}},
		}}},
	}
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetFullDocumentBeforeChange(options.WhenAvailable).SetMaxAwaitTime(maxAwait)
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	changeStreamOpts = changeStreamOpts.SetResumeAfter(resumeAfterDocument(prevResumeToken, m.state.GetCursor(stream.Self(), cdcTypeBitsField)))
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), prevResumeToken)

	// State written before type bits were kept can hold a token the server cannot match. Such a
	// stream restarts once from the token's own cluster time, and skips what it already read.
	restarted := false
	restartAtTokenTime := func(cause error) (*mongo.ChangeStream, error) {
		data, _ := prevResumeToken.(string)
		opTime, decodeErr := decodeResumeTokenOpTime(data)
		if restarted || decodeErr != nil || !isResumeTokenNotFound(cause) {
			return nil, cause
		}
		restarted = true
		logger.Warnf("Stream[%s] resume token was not found by the server, restarting from its cluster time[%d,%d]", stream.ID(), opTime.T, opTime.I)
		return collection.Watch(ctx, pipeline, changeStreamOpts.SetResumeAfter(nil).SetStartAtOperationTime(&opTime))
	}

	cursor, err := collection.Watch(ctx, pipeline, changeStreamOpts)
	if err != nil {
		if cursor, err = restartAtTokenTime(err); err != nil {
			return nil, fmt.Errorf("failed to open change stream: %w", err)
		}
	}
	defer func() { _ = cursor.Close(ctx) }()

	for {
		hasNext := cursor.TryNext(ctx)
		if err := cursor.Err(); err != nil {
			// through mongos the token is checked on the first read, not when the stream opens
			restartedCursor, err := restartAtTokenTime(err)
			if err != nil {
				return nil, fmt.Errorf("change stream error: %w", err)
			}
			_ = cursor.Close(ctx)
			cursor = restartedCursor
			continue
		}

		// after a restart the stream begins at the saved token's cluster time: events up to
		// that token were read by the sync that saved it (_data sorts in oplog order)
		if hasNext && restarted && typeutils.Compare(cursor.Current.Lookup("_id", cdcCursorField).StringValue(), prevResumeToken) <= 0 {
			continue
		}

		if hasNext {
			if err := m.handleChangeDoc(ctx, cursor, stream, OnMessage); err != nil {
				return nil, err
			}
		}

		// Check boundary AFTER emitting
		if latestResumeToken, err := m.handleStreamCatchup(ctx, cursor, stream, lastOplogTime); err != nil {
			if errors.Is(err, ErrIdleTermination) {
				// graceful termination requested by helper
				logger.Infof("change stream %s caught up to cluster opTime; terminating gracefully", stream.ID())
				return latestResumeToken, nil
			}
			return nil, err
		}

		if !hasNext {
			// Wait before for a brief pause before the next iteration of the loop
			// TryNext() method behaves differently when connecting through mongos vs. direct replica set connections:TryNext() is non-blocking and may return false immediately even when events exist
			time.Sleep(changeStreamRetryDelay)
		}
	}
}

func (m *Mongo) handleStreamCatchup(_ context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, lastOplogTime primitive.Timestamp) (string, error) {
	token, err := GetResumeToken(cursor, stream.ID())
	if err != nil {
		return "", err
	}

	// check pointing post batch resume token
	m.cdcCursor.Store(stream.ID(), token)
	m.cdcCursor.Store(typeBitsKey(stream.ID()), resumeTokenTypeBits(cursor.ResumeToken()))

	streamOpTime, err := decodeResumeTokenOpTime(token)
	if err != nil {
		return token, fmt.Errorf("failed to decode resume token for stream %s: %w", stream.ID(), err)
	}

	// If stream is caught up -> request graceful termination
	if !lastOplogTime.After(streamOpTime) {
		return token, ErrIdleTermination
	}

	return token, nil
}

func (m *Mongo) handleChangeDoc(ctx context.Context, cursor *mongo.ChangeStream, stream types.StreamInterface, OnMessage abstract.CDCMsgFn) error {
	var record CDCDocument
	if err := cursor.Decode(&record); err != nil {
		return fmt.Errorf("error while decoding: %w", err)
	}

	// Count the BSON bytes of the actual document payload, not the full
	// change-event envelope (which adds operationType, clusterTime, ns,
	// documentKey, updateDescription, resumeToken on top of the doc).
	docBytes := cdcDocBytes(cursor.Current, record.OperationType)

	record.OperationType = normalizeOperationType(record.OperationType)

	switch record.OperationType {
	case "delete":
		if record.FullDocumentBeforeChange != nil {
			record.FullDocument = record.FullDocumentBeforeChange
		} else {
			record.FullDocument = record.DocumentKey
		}
	case "update":
		if record.FullDocument == nil && record.FullDocumentBeforeChange != nil {
			record.FullDocument = record.FullDocumentBeforeChange
		}
	}

	filterMongoObject(record.FullDocument)

	ts := utils.Ternary(record.WallTime != 0,
		record.WallTime.Time(), // millisecond precision
		time.UnixMilli(int64(record.ClusterTime.T)*1000+int64(record.ClusterTime.I)), // seconds only
	).(time.Time)
	token, err := GetResumeToken(cursor, stream.ID())
	if err != nil {
		return err
	}
	change := abstract.NewCDCChange(stream, ts, record.OperationType, record.FullDocument,
		map[string]any{CDCResumeToken: token}, docBytes, nil)

	if err := OnMessage(ctx, change); err != nil {
		return err
	}

	if resumeToken := cursor.ResumeToken(); resumeToken != nil {
		m.cdcCursor.Store(stream.ID(), resumeToken.Lookup(cdcCursorField).StringValue())
		m.cdcCursor.Store(typeBitsKey(stream.ID()), resumeTokenTypeBits(resumeToken))
	}
	return nil
}

func (m *Mongo) PostCDC(ctx context.Context, streamIndex int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		val, ok := m.cdcCursor.Load(m.streams[streamIndex].ID())
		if ok {
			typeBits, _ := m.cdcCursor.Load(typeBitsKey(m.streams[streamIndex].ID()))
			m.state.SetCursor(m.streams[streamIndex].Self(), cdcTypeBitsField, typeBits)
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
		return nil, fmt.Errorf("failed to open change stream: %w", err)
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
			return opTime, fmt.Errorf("failed to fetch 'operationTime' from 'isMaster' command: both 'hello' and 'isMaster' failed: %w", err)
		}
	}

	// Extract 'operationTime' field
	opRaw, err := raw.LookupErr("operationTime")
	if err != nil {
		return opTime, fmt.Errorf("operationTime field missing in server response: %w", err)
	}
	if err := opRaw.Unmarshal(&opTime); err != nil {
		return opTime, fmt.Errorf("failed to unmarshal operationTime: %w", err)
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
		return primitive.Timestamp{}, errs.Precondition(errs.StateInvalid, codeResumeTokenInvalid,
			fmt.Errorf("invalid resume token"))
	}
	return primitive.Timestamp{
		T: binary.BigEndian.Uint32(dataBytes[1:5]),
		I: binary.BigEndian.Uint32(dataBytes[5:9]),
	}, nil
}

// GetResumeToken extracts and validates the resume token string from a change stream cursor
func GetResumeToken(cursor *mongo.ChangeStream, streamID string) (string, error) {
	finalToken := cursor.ResumeToken()
	if finalToken == nil {
		return "", errs.Precondition(errs.StateInvalid, codeResumeTokenMissing,
			fmt.Errorf("no resume token available for stream %s", streamID))
	}

	rawToken, err := finalToken.LookupErr(cdcCursorField)
	if err != nil {
		return "", fmt.Errorf("%s field not found in resume token for stream %s: %w",
			cdcCursorField, streamID, err)
	}

	token := rawToken.StringValue()
	if token == "" {
		return "", errs.Precondition(errs.StateInvalid, codeResumeTokenInvalid,
			fmt.Errorf("empty resume token value for stream %s", streamID))
	}

	return token, nil
}

// normalizeOperationType maps MongoDB-specific operation types to the standard
// set understood by the abstract CDC layer (insert, update, delete).
// "replace" swaps the full document but keeps _id unchanged, so it is treated as an update.
func normalizeOperationType(opType string) string {
	switch opType {
	case "replace":
		return "update"
	default:
		return opType
	}
}

// cdcDocBytes returns the BSON byte size of the document payload inside a MongoDB change event
func cdcDocBytes(event bson.Raw, operationType string) int64 {
	switch operationType {
	case "delete":
		if v, err := event.LookupErr("fullDocumentBeforeChange"); err == nil && v.Type != bson.TypeNull {
			return int64(len(v.Value))
		}
		// No pre-image: MongoDB only sent the key, not the deleted document.
		if v, err := event.LookupErr("documentKey"); err == nil {
			return int64(len(v.Value))
		}
		return 0
	default:
		if v, err := event.LookupErr("fullDocument"); err == nil && v.Type != bson.TypeNull {
			return int64(len(v.Value))
		}
		return 0
	}
}

// typeBitsKey is where a stream's pending type bits sit in Mongo.cdcCursor until PostCDC.
func typeBitsKey(streamID string) string {
	return streamID + "." + cdcTypeBitsField
}

// resumeTokenTypeBits returns the base64 of the token's _typeBits, or "" when it has none.
func resumeTokenTypeBits(token bson.Raw) string {
	_, typeBits, ok := token.Lookup(cdcTypeBitsField).BinaryOK()
	if !ok {
		return ""
	}
	return base64.StdEncoding.EncodeToString(typeBits)
}

// resumeAfterDocument rebuilds the token the server issued from the two halves kept in state.
// State written before type bits were kept has none, and resumes from _data alone.
func resumeAfterDocument(data, typeBits any) bson.D {
	token := bson.D{{Key: cdcCursorField, Value: data}}
	encoded, _ := typeBits.(string)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) == 0 {
		return token
	}
	// the server issues _typeBits with the generic binary subtype, so state does not record one
	return append(token, bson.E{Key: cdcTypeBitsField, Value: primitive.Binary{Subtype: bson.TypeBinaryGeneric, Data: decoded}})
}

// lookupTypeBits returns the type bits of a resume token known only by its _data, or "" when
// it has none. It reads the oplog from the token's cluster time until the server hands the
// same token back, this time complete.
func (m *Mongo) lookupTypeBits(ctx context.Context, stream types.StreamInterface, data any) string {
	want, _ := data.(string)
	opTime, err := decodeResumeTokenOpTime(want)
	if err != nil {
		return ""
	}
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	cursor, err := collection.Watch(ctx, mongo.Pipeline{}, options.ChangeStream().SetStartAtOperationTime(&opTime).SetMaxAwaitTime(maxAwait))
	if err != nil {
		logger.Warnf("Stream[%s] could not read type bits of the metadata resume token: %s", stream.ID(), err)
		return ""
	}
	defer cursor.Close(ctx)

	// TryNext can come back empty through mongos while events exist, so one empty read is not the end
	for emptyReads := 0; emptyReads < 2; {
		if !cursor.TryNext(ctx) {
			emptyReads++
			continue
		}
		// the event's own _id, not cursor.ResumeToken(): that one is the post batch token, a
		// later position, once the event is the last of its batch
		token := cursor.Current.Lookup("_id").Document()
		// _data is built to sort in oplog order: past the wanted token means it is not an event token
		if got := token.Lookup(cdcCursorField).StringValue(); got == want {
			return resumeTokenTypeBits(token)
		} else if got > want {
			break
		}
	}
	return ""
}

// isResumeTokenNotFound reports whether the server rejected a resume because no oplog entry
// matches the token. A token older than the oplog fails with ChangeStreamHistoryLost instead.
func isResumeTokenNotFound(err error) bool {
	var cmdErr mongo.CommandError
	return errors.As(err, &cmdErr) && cmdErr.Name == "ChangeStreamFatalError" &&
		strings.Contains(cmdErr.Message, "resume token was not found")
}
