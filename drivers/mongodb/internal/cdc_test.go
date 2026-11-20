package driver

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

const (
	// an insert event token whose _id is a long above the int32 range, and its type bits
	longIDTokenData    = "826AFECC2C00000B832B042C0100296E5A1004E70BAF1BFE604A458CA395D58242B1BC463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002F0411677236000004" //nolint:gosec // false positive: a change stream resume token, not a credential
	changeStreamTestNS = "app.orders"
)

var longIDTypeBitsRaw = []byte{0x82, 0x00, 0x01}

// savedToken is a resume token as state keeps it under _resume_token: extended JSON, with the
// type bits of longIDTypeBitsRaw (base64 "ggAB") when asked for.
func savedToken(data string, typeBits bool) map[string]any {
	token := map[string]any{"_data": data}
	if typeBits {
		token["_typeBits"] = map[string]any{"$binary": map[string]any{"base64": "ggAB", "subType": "00"}}
	}
	return token
}

// eventToken is a resume token as the server issues it: _data, plus _typeBits when given.
func eventToken(data string, typeBits []byte) bson.D {
	token := bson.D{{Key: "_data", Value: data}}
	if typeBits != nil {
		token = append(token, bson.E{Key: "_typeBits", Value: primitive.Binary{Data: typeBits}})
	}
	return token
}

// changeEvent is the part of a change event lookupResumeToken reads: its _id, the resume token.
func changeEvent(data string, typeBits []byte) bson.D {
	return bson.D{{Key: "_id", Value: eventToken(data, typeBits)}}
}

// emptyBatchResponse is a cursor reply with no events and the given post batch resume token,
// which tells how far the stream has read.
func emptyBatchResponse(batch mtest.BatchIdentifier, postBatchData string) bson.D {
	return bson.D{
		{Key: "ok", Value: 1},
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(1)},
			{Key: "ns", Value: changeStreamTestNS},
			{Key: string(batch), Value: bson.A{}},
			{Key: "postBatchResumeToken", Value: bson.D{{Key: "_data", Value: postBatchData}}},
		}},
	}
}

// laterToken returns a token of the same shape one counter step after data.
func laterToken(data string) string {
	return data[:17] + "4" + data[18:]
}

func TestSavedResumeToken(t *testing.T) {
	marshal := func(doc bson.D) bson.Raw {
		raw, err := bson.Marshal(doc)
		require.NoError(t, err)
		return raw
	}

	assert.Equal(t, savedToken(longIDTokenData, true), savedResumeToken(marshal(eventToken(longIDTokenData, longIDTypeBitsRaw))), "token with type bits")
	assert.Equal(t, savedToken(longIDTokenData, false), savedResumeToken(marshal(eventToken(longIDTokenData, nil))), "token without type bits")
	// whatever the server puts next to _data is kept as it is
	assert.Equal(t, map[string]any{"_data": longIDTokenData, "_typeBits": "ggAB"},
		savedResumeToken(marshal(bson.D{{Key: "_data", Value: longIDTokenData}, {Key: "_typeBits", Value: "ggAB"}})), "field that is not binary")
	assert.Empty(t, savedResumeToken(marshal(bson.D{})), "empty token")
	assert.Empty(t, savedResumeToken(nil), "nil token")
}

func TestResumeAfterDocument(t *testing.T) {
	dataOnly := bson.D{{Key: "_data", Value: longIDTokenData}}
	whole := bson.D{
		{Key: "_data", Value: longIDTokenData},
		{Key: "_typeBits", Value: primitive.Binary{Subtype: bson.TypeBinaryGeneric, Data: longIDTypeBitsRaw}},
	}

	t.Run("whole token of the same _data is sent back, type bits as generic binary", func(t *testing.T) {
		assert.Equal(t, whole, resumeAfterDocument(longIDTokenData, savedToken(longIDTokenData, true)))
	})

	t.Run("whole token as read back from the state file", func(t *testing.T) {
		encoded, err := json.Marshal(savedToken(longIDTokenData, true))
		require.NoError(t, err)
		var saved map[string]any
		require.NoError(t, json.Unmarshal(encoded, &saved))
		assert.Equal(t, whole, resumeAfterDocument(longIDTokenData, saved))
	})

	// no whole token (state written by a version that keeps _data alone, or the server gave no
	// answer), a token that is whole with its _data, or one that cannot be decoded: all resume
	// from _data alone. A whole token of another _data is the caller's check: TestIsResumeTokenOf.
	for name, saved := range map[string]map[string]any{
		"no whole token":       nil,
		"empty document":       {},
		"token of _data alone": savedToken(longIDTokenData, false),
		"not extended JSON":    {"_data": longIDTokenData, "_typeBits": map[string]any{"$binary": "not base64!"}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, dataOnly, resumeAfterDocument(longIDTokenData, saved))
		})
	}

	t.Run("round trip gives back the token the server issued", func(t *testing.T) {
		issued, err := bson.Marshal(eventToken(longIDTokenData, longIDTypeBitsRaw))
		require.NoError(t, err)

		rebuilt, err := bson.Marshal(resumeAfterDocument(longIDTokenData, savedResumeToken(issued)))
		require.NoError(t, err)
		assert.Equal(t, bson.Raw(issued), bson.Raw(rebuilt))
	})
}

func TestIsResumeTokenOf(t *testing.T) {
	assert.True(t, isResumeTokenOf(savedToken(longIDTokenData, true), longIDTokenData))
	assert.True(t, isResumeTokenOf(savedToken(longIDTokenData, false), longIDTokenData))
	assert.False(t, isResumeTokenOf(savedToken(laterToken(longIDTokenData), true), longIDTokenData), "left behind by a version that moved _data")
	assert.False(t, isResumeTokenOf(nil, longIDTokenData), "state has none")
	assert.False(t, isResumeTokenOf(map[string]any(nil), longIDTokenData))
	assert.False(t, isResumeTokenOf(longIDTokenData, longIDTokenData), "not a document")
}

func TestLookupResumeToken(t *testing.T) {
	stream := types.NewStream("orders", "app", nil).Wrap(0)
	ctx := context.Background()

	mockMongo(t, "event with the same _data", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(longIDTokenData, longIDTypeBitsRaw)),
		)
		assert.Equal(mt, savedToken(longIDTokenData, true), m.lookupResumeToken(ctx, stream, longIDTokenData))

		// the stream starts at the token's own cluster time, not at the token
		started := mt.GetStartedEvent()
		require.NotNil(mt, started)
		assert.Equal(mt, "aggregate", started.CommandName)
		stage := started.Command.Lookup("pipeline", "0", "$changeStream").Document()
		seconds, increment := stage.Lookup("startAtOperationTime").Timestamp()
		assert.Equal(mt, uint32(0x6AFECC2C), seconds)
		assert.Equal(mt, uint32(0x00000B83), increment)
		_, err := stage.LookupErr("resumeAfter")
		assert.Error(mt, err, "the incomplete token must not be sent as resumeAfter")
	})

	mockMongo(t, "same _data, token has no type bits", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(longIDTokenData, nil)),
		)
		assert.Equal(mt, savedToken(longIDTokenData, false), m.lookupResumeToken(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "earlier events are passed over", func(mt *mtest.T, m *Mongo) {
		wanted := laterToken(longIDTokenData)
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch,
				changeEvent(longIDTokenData, nil),
				changeEvent(wanted, longIDTypeBitsRaw)),
		)
		assert.Equal(mt, savedToken(wanted, true), m.lookupResumeToken(ctx, stream, wanted))
	})

	mockMongo(t, "stream is already past the token", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(laterToken(longIDTokenData), longIDTypeBitsRaw)),
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, longIDTokenData), "the server did not issue it, so no token is made up from _data")
	})

	// withLookupTimeout runs the lookups of one test with the given timeout
	withLookupTimeout := func(mt *mtest.T, timeout time.Duration) {
		previous := resumeTokenLookupTimeout
		resumeTokenLookupTimeout = timeout
		mt.Cleanup(func() { resumeTokenLookupTimeout = previous })
	}

	mockMongo(t, "no events and no sign the token's position was reached, until the timeout", func(mt *mtest.T, m *Mongo) {
		withLookupTimeout(mt, 0)
		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.FirstBatch),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, longIDTokenData), "no answer, so a later sync asks again")
	})

	mockMongo(t, "no events, stream has read past the token", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, laterToken(longIDTokenData)),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "no events, stream has read up to the token itself", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, longIDTokenData),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "no events, stream is still before the token at the timeout", func(mt *mtest.T, m *Mongo) {
		withLookupTimeout(mt, 0)
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, longIDTokenData),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, laterToken(longIDTokenData)))
	})

	mockMongo(t, "empty reads before the timeout are read again", func(mt *mtest.T, m *Mongo) {
		wanted := laterToken(longIDTokenData)
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, longIDTokenData),
			emptyBatchResponse(mtest.NextBatch, longIDTokenData),
			emptyBatchResponse(mtest.NextBatch, longIDTokenData),
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.NextBatch, changeEvent(wanted, longIDTypeBitsRaw)),
		)
		assert.Equal(mt, savedToken(wanted, true), m.lookupResumeToken(ctx, stream, wanted), "a third empty read is not the end")
	})

	mockMongo(t, "canceled sync stops waiting", func(mt *mtest.T, m *Mongo) {
		canceled, cancel := context.WithCancel(ctx)
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, longIDTokenData),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		time.AfterFunc(50*time.Millisecond, cancel)
		started := time.Now()
		assert.Nil(mt, m.lookupResumeToken(canceled, stream, laterToken(longIDTokenData)))
		assert.Less(mt, time.Since(started), changeStreamRetryDelay)
	})

	mockMongo(t, "read fails after the stream opened", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.FirstBatch),
			// not a resumable error, so the driver hands it back instead of reopening the stream
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "resume point may no longer be in the oplog"}),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "change stream cannot be opened", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "resume point may no longer be in the oplog"}),
		)
		assert.Nil(mt, m.lookupResumeToken(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "token that cannot be decoded asks the server nothing", func(mt *mtest.T, m *Mongo) {
		for _, data := range []any{nil, "", "not hex", "82", 7} {
			assert.Nil(mt, m.lookupResumeToken(ctx, stream, data))
		}
		assert.Nil(mt, mt.GetStartedEvent(), "no command expected")
	})
}

// cdcTestMongo is a mocked driver holding one stream whose state, read the way a sync reads its
// state file, has the given cursor values.
func cdcTestMongo(mt *mtest.T, m *Mongo, cursors map[string]any) *types.ConfiguredStream {
	stream := types.NewStream("orders", "app", nil).Wrap(0)
	encoded, err := json.Marshal(map[string]any{
		"type":    types.StreamType,
		"streams": []map[string]any{{"stream": stream.Name(), "namespace": stream.Namespace(), "state": cursors}},
	})
	require.NoError(mt, err)
	m.state = &types.State{RWMutex: &sync.RWMutex{}}
	require.NoError(mt, json.Unmarshal(encoded, m.state))
	m.streams = []types.StreamInterface{stream}
	return stream
}

// pendingCursors returns what PostCDC will write to state for the stream: its _data and its whole token.
func pendingCursors(m *Mongo, stream *types.ConfiguredStream) (data, resumeToken any) {
	val, ok := m.cdcCursor.Load(stream.ID())
	if !ok {
		return nil, nil
	}
	position := val.(cdcPosition)
	return position.data, position.resumeToken
}

func TestStreamChangesMetadataResumeToken(t *testing.T) {
	ctx := context.Background()
	metadataToken := laterToken(longIDTokenData)

	// what the destination keeps: the same two values as the state file
	wholeMetadataDoc, err := json.Marshal(map[string]any{"_data": metadataToken, "_resume_token": savedToken(metadataToken, true)})
	require.NoError(t, err)
	wholeMetadata := string(wholeMetadataDoc)

	mockMongo(t, "metadata of _data alone: the whole token is read back and stored with its _data", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, false)})
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(metadataToken, longIDTypeBitsRaw)),
		)
		returned, err := m.StreamChanges(ctx, 0, map[string]any{stream.ID(): metadataToken}, nil)
		require.NoError(mt, err)
		data, resumeToken := pendingCursors(m, stream)
		assert.Equal(mt, metadataToken, data)
		assert.Equal(mt, savedToken(metadataToken, true), resumeToken)
		assert.JSONEq(mt, wholeMetadata, returned.(string), "the destination gets what the state file gets")
	})

	mockMongo(t, "metadata of _data alone the server does not hand back: no whole token is stored, so the next sync asks again", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, false)})
		// the stream is already past the metadata token: it is not an event token
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(metadataToken[:17]+"5"+metadataToken[18:], longIDTypeBitsRaw)),
		)
		returned, err := m.StreamChanges(ctx, 0, map[string]any{stream.ID(): metadataToken}, nil)
		require.NoError(mt, err)
		data, resumeToken := pendingCursors(m, stream)
		assert.Equal(mt, metadataToken, data)
		assert.Nil(mt, resumeToken, "a token made up from _data must not reach the state")
		assert.JSONEq(mt, `{"_data":"`+metadataToken+`","_resume_token":null}`, returned.(string))
	})

	mockMongo(t, "metadata that holds the whole token asks the server nothing", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, false)})
		// the destination hands metadata back as the JSON text it stored
		stored, err := types.SetMetadataState(wholeMetadata, "")
		require.NoError(mt, err)
		returned, err := m.StreamChanges(ctx, 0, map[string]any{stream.ID(): stored.State}, nil)
		require.NoError(mt, err)
		data, resumeToken := pendingCursors(m, stream)
		assert.Equal(mt, metadataToken, data)
		assert.Equal(mt, savedToken(metadataToken, true), resumeToken)
		assert.JSONEq(mt, wholeMetadata, returned.(string))
		assert.Nil(mt, mt.GetStartedEvent(), "no command expected")
	})

	mockMongo(t, "metadata behind the state is not used", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: laterToken(metadataToken), cdcResumeTokenField: savedToken(laterToken(metadataToken), false)})
		stored, err := types.SetMetadataState(wholeMetadata, "")
		require.NoError(mt, err)
		// no mock responses: the stream goes on to read from the server, and fails there
		_, err = m.StreamChanges(ctx, 0, map[string]any{stream.ID(): stored.State}, nil)
		require.Error(mt, err)
		data, _ := pendingCursors(m, stream)
		assert.Nil(mt, data, "the metadata token must not become the pending _data")
	})

	mockMongo(t, "no answer from the server leaves the whole token unset, so the next sync asks again", func(mt *mtest.T, m *Mongo) {
		// the state holds the whole token of its own _data: it must not survive next to another _data
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, true)})
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 89, Name: "NetworkTimeout", Message: "connection timed out"}),
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{stream.ID(): metadataToken}, nil)
		require.NoError(mt, err)
		data, resumeToken := pendingCursors(m, stream)
		assert.Equal(mt, metadataToken, data)
		assert.Nil(mt, resumeToken)
	})

	mockMongo(t, "unset whole token reads back from a state file as unset", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: metadataToken, cdcResumeTokenField: map[string]any(nil)})
		assert.Nil(mt, m.state.GetCursor(stream, cdcResumeTokenField), "nil is what makes the next sync look the whole token up")
		assert.Equal(mt, metadataToken, m.state.GetCursor(stream, cdcCursorField))
	})
}

func TestStreamChangesReadsBackResumeTokenOfOldState(t *testing.T) {
	ctx := context.Background()
	// what the stream reports once it has read past every event
	caughtUp := emptyBatchResponse(mtest.FirstBatch, laterToken(longIDTokenData))
	clusterTime := bson.D{{Key: "ok", Value: 1}, {Key: "operationTime", Value: primitive.Timestamp{T: 0x6AFECC2C, I: 1}}}
	resumeAfterOf := func(mt *mtest.T, aggregate int) bson.Raw {
		seen := 0
		for _, started := range mt.GetAllStartedEvents() {
			if started.CommandName != "aggregate" {
				continue
			}
			if seen++; seen == aggregate {
				return started.Command.Lookup("pipeline", "0", "$changeStream", "resumeAfter").Document()
			}
		}
		require.Fail(mt, "aggregate command not found")
		return nil
	}

	mockMongo(t, "state without the whole token", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData})
		require.Nil(mt, m.state.GetCursor(stream, cdcResumeTokenField))
		mt.AddMockResponses(
			clusterTime,
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(longIDTokenData, longIDTypeBitsRaw)), // lookup
			caughtUp, // the stream itself
			emptyBatchResponse(mtest.NextBatch, laterToken(longIDTokenData)),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{}, nil)
		require.NoError(mt, err)

		want, err := bson.Marshal(eventToken(longIDTokenData, longIDTypeBitsRaw))
		require.NoError(mt, err)
		assert.Equal(mt, bson.Raw(want), resumeAfterOf(mt, 2), "the stream must resume from the whole token")

		_, resumeToken := pendingCursors(m, stream)
		assert.NotNil(mt, resumeToken, "PostCDC writes the whole token to state, so the next sync does not look it up")
	})

	mockMongo(t, "whole token left behind by a version that moved _data", func(mt *mtest.T, m *Mongo) {
		// upgrade, rollback, upgrade: the older version wrote a new _data and kept the old whole token
		cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken("8200000001", true)})
		mt.AddMockResponses(
			clusterTime,
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(longIDTokenData, longIDTypeBitsRaw)), // lookup
			caughtUp, // the stream itself
			emptyBatchResponse(mtest.NextBatch, laterToken(longIDTokenData)),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{}, nil)
		require.NoError(mt, err)

		want, err := bson.Marshal(eventToken(longIDTokenData, longIDTypeBitsRaw))
		require.NoError(mt, err)
		assert.Equal(mt, bson.Raw(want), resumeAfterOf(mt, 2), "the stream must resume from the whole token of its own _data")
	})

	mockMongo(t, "state that already has the whole token asks the server nothing extra", func(mt *mtest.T, m *Mongo) {
		cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, true)})
		mt.AddMockResponses(
			clusterTime,
			caughtUp,
			emptyBatchResponse(mtest.NextBatch, laterToken(longIDTokenData)),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{}, nil)
		require.NoError(mt, err)

		want, err := bson.Marshal(eventToken(longIDTokenData, longIDTypeBitsRaw))
		require.NoError(mt, err)
		assert.Equal(mt, bson.Raw(want), resumeAfterOf(mt, 1))
	})

	mockMongo(t, "whole token of _data alone is not looked up again", func(mt *mtest.T, m *Mongo) {
		cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, false)})
		mt.AddMockResponses(
			clusterTime,
			caughtUp,
			emptyBatchResponse(mtest.NextBatch, laterToken(longIDTokenData)),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{}, nil)
		require.NoError(mt, err)

		want, err := bson.Marshal(eventToken(longIDTokenData, nil))
		require.NoError(mt, err)
		assert.Equal(mt, bson.Raw(want), resumeAfterOf(mt, 1))
	})
}

// Two streams read in either order: each must keep its own _data and its own whole token.
func TestResumeTokenOfOneStreamDoesNotReachAnother(t *testing.T) {
	ctx := context.Background()
	ordersToken := laterToken(longIDTokenData) // metadata token of "orders", has type bits
	otherToken := laterToken(ordersToken)      // metadata token of "orders._typeBits", has none

	for _, first := range []string{"orders", "orders._typeBits"} {
		mockMongo(t, "stream read first: "+first, func(mt *mtest.T, m *Mongo) {
			orders := types.NewStream("orders", "app", nil).Wrap(0)
			other := types.NewStream("orders._typeBits", "app", nil).Wrap(0)

			encoded, err := json.Marshal(map[string]any{
				"type": types.StreamType,
				"streams": []map[string]any{
					{"stream": orders.Name(), "namespace": "app", "state": map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, false)}},
					{"stream": other.Name(), "namespace": "app", "state": map[string]any{cdcCursorField: longIDTokenData, cdcResumeTokenField: savedToken(longIDTokenData, false)}},
				},
			})
			require.NoError(mt, err)
			m.state = &types.State{RWMutex: &sync.RWMutex{}}
			require.NoError(mt, json.Unmarshal(encoded, m.state))
			m.streams = []types.StreamInterface{orders, other}
			metadata := map[string]any{orders.ID(): ordersToken, other.ID(): otherToken}

			readOrders := func() {
				mt.AddMockResponses(mtest.CreateCursorResponse(0, "app.orders", mtest.FirstBatch, changeEvent(ordersToken, longIDTypeBitsRaw)))
				_, err := m.StreamChanges(ctx, 0, metadata, nil)
				require.NoError(mt, err)
			}
			readOther := func() {
				mt.AddMockResponses(mtest.CreateCursorResponse(0, "app.orders._typeBits", mtest.FirstBatch, changeEvent(otherToken, nil)))
				_, err := m.StreamChanges(ctx, 1, metadata, nil)
				require.NoError(mt, err)
			}
			if first == "orders" {
				readOrders()
				readOther()
			} else {
				readOther()
				readOrders()
			}

			data, resumeToken := pendingCursors(m, orders)
			assert.Equal(mt, ordersToken, data, "_data of orders")
			assert.Equal(mt, savedToken(ordersToken, true), resumeToken, "whole token of orders")
			data, resumeToken = pendingCursors(m, other)
			assert.Equal(mt, otherToken, data, "_data of orders._typeBits")
			assert.Equal(mt, savedToken(otherToken, false), resumeToken, "whole token of orders._typeBits")
		})
	}
}
