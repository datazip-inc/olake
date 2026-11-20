package driver

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

const (
	// an insert event token whose _id is a long above the int32 range, and its type bits
	longIDTokenData = "826AFECC2C00000B832B042C0100296E5A1004E70BAF1BFE604A458CA395D58242B1BC463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002F0411677236000004" //nolint:gosec // false positive: a change stream resume token, not a credential
	// base64 of 0x82 0x00 0x01
	longIDTypeBits     = "ggAB"
	changeStreamTestNS = "app.orders"
)

var longIDTypeBitsRaw = []byte{0x82, 0x00, 0x01}

// eventToken is a resume token as the server issues it: _data, plus _typeBits when given.
func eventToken(data string, typeBits []byte) bson.D {
	token := bson.D{{Key: "_data", Value: data}}
	if typeBits != nil {
		token = append(token, bson.E{Key: "_typeBits", Value: primitive.Binary{Data: typeBits}})
	}
	return token
}

// changeEvent is the part of a change event lookupTypeBits reads: its _id, the resume token.
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

func TestTypeBitsKey(t *testing.T) {
	assert.Equal(t, typeBitsCursorKey("app.orders._typeBits"), typeBitsKey("app.orders"))
	assert.NotEqual(t, typeBitsKey("app.orders"), typeBitsKey("app.customers"))

	// a collection may be named "orders._typeBits": its stream ID is the same text as the type
	// bits key of "app.orders", and the two entries must still be kept apart
	var cursors sync.Map
	cursors.Store("app.orders._typeBits", "resume token of the other stream")
	cursors.Store(typeBitsKey("app.orders"), longIDTypeBits)
	data, _ := cursors.Load("app.orders._typeBits")
	typeBits, _ := cursors.Load(typeBitsKey("app.orders"))
	assert.Equal(t, "resume token of the other stream", data)
	assert.Equal(t, longIDTypeBits, typeBits)
}

func TestResumeTokenTypeBits(t *testing.T) {
	marshal := func(doc bson.D) bson.Raw {
		raw, err := bson.Marshal(doc)
		require.NoError(t, err)
		return raw
	}

	tests := []struct {
		name  string
		token bson.Raw
		want  string
	}{
		{name: "token with type bits", token: marshal(eventToken(longIDTokenData, longIDTypeBitsRaw)), want: longIDTypeBits},
		{name: "token without type bits", token: marshal(eventToken(longIDTokenData, nil)), want: ""},
		{name: "type bits that are not binary", token: marshal(bson.D{{Key: "_data", Value: longIDTokenData}, {Key: "_typeBits", Value: "ggAB"}}), want: ""},
		{name: "empty token", token: marshal(bson.D{}), want: ""},
		{name: "nil token", token: nil, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resumeTokenTypeBits(tc.token))
		})
	}
}

func TestResumeAfterDocument(t *testing.T) {
	dataOnly := bson.D{{Key: "_data", Value: longIDTokenData}}

	t.Run("type bits are sent back as generic binary", func(t *testing.T) {
		want := bson.D{
			{Key: "_data", Value: longIDTokenData},
			{Key: "_typeBits", Value: primitive.Binary{Subtype: bson.TypeBinaryGeneric, Data: longIDTypeBitsRaw}},
		}
		assert.Equal(t, want, resumeAfterDocument(longIDTokenData, longIDTypeBits))
	})

	// state written before type bits were kept, or a token that has none, resumes from _data alone
	for name, typeBits := range map[string]any{
		"key absent from state": nil,
		"empty type bits":       "",
		"not base64":            "not base64!",
		"not a string":          7,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, dataOnly, resumeAfterDocument(longIDTokenData, typeBits))
		})
	}

	t.Run("round trip gives back the token the server issued", func(t *testing.T) {
		issued, err := bson.Marshal(eventToken(longIDTokenData, longIDTypeBitsRaw))
		require.NoError(t, err)

		rebuilt, err := bson.Marshal(resumeAfterDocument(longIDTokenData, resumeTokenTypeBits(issued)))
		require.NoError(t, err)
		assert.Equal(t, bson.Raw(issued), bson.Raw(rebuilt))
	})
}

func TestLookupTypeBits(t *testing.T) {
	stream := types.NewStream("orders", "app", nil).Wrap(0)
	ctx := context.Background()

	mockMongo(t, "event with the same _data", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(longIDTokenData, longIDTypeBitsRaw)),
		)
		assert.Equal(mt, longIDTypeBits, m.lookupTypeBits(ctx, stream, longIDTokenData))

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
		assert.Equal(mt, "", m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "earlier events are passed over", func(mt *mtest.T, m *Mongo) {
		wanted := laterToken(longIDTokenData)
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch,
				changeEvent(longIDTokenData, nil),
				changeEvent(wanted, longIDTypeBitsRaw)),
		)
		assert.Equal(mt, longIDTypeBits, m.lookupTypeBits(ctx, stream, wanted))
	})

	mockMongo(t, "stream is already past the token", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(laterToken(longIDTokenData), longIDTypeBitsRaw)),
		)
		assert.Equal(mt, "", m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "no events and no sign the token's position was reached", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.FirstBatch),
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.NextBatch),
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.NextBatch),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupTypeBits(ctx, stream, longIDTokenData), "no answer, so a later sync asks again")
	})

	mockMongo(t, "no events, stream has read past the token", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, laterToken(longIDTokenData)),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Equal(mt, "", m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "no events, stream has read up to the token itself", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, longIDTokenData),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Equal(mt, "", m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "no events, stream is still before the token", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			emptyBatchResponse(mtest.FirstBatch, longIDTokenData),
			emptyBatchResponse(mtest.NextBatch, longIDTokenData),
			emptyBatchResponse(mtest.NextBatch, longIDTokenData),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupTypeBits(ctx, stream, laterToken(longIDTokenData)))
	})

	mockMongo(t, "read fails after the stream opened", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.FirstBatch),
			// not a resumable error, so the driver hands it back instead of reopening the stream
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "resume point may no longer be in the oplog"}),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Nil(mt, m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "change stream cannot be opened", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "resume point may no longer be in the oplog"}),
		)
		assert.Nil(mt, m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "token that cannot be decoded asks the server nothing", func(mt *mtest.T, m *Mongo) {
		for _, data := range []any{nil, "", "not hex", "82", 7} {
			assert.Nil(mt, m.lookupTypeBits(ctx, stream, data))
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

// pendingCursors returns what PostCDC will write to state for the stream: its _data and type bits.
func pendingCursors(m *Mongo, stream *types.ConfiguredStream) (data, typeBits any) {
	data, _ = m.cdcCursor.Load(stream.ID())
	typeBits, _ = m.cdcCursor.Load(typeBitsKey(stream.ID()))
	return data, typeBits
}

func TestStreamChangesMetadataTokenTypeBits(t *testing.T) {
	ctx := context.Background()
	metadataToken := laterToken(longIDTokenData)

	mockMongo(t, "type bits of the metadata token are stored with it", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcTypeBitsField: ""})
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, changeStreamTestNS, mtest.FirstBatch, changeEvent(metadataToken, longIDTypeBitsRaw)),
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{stream.ID(): metadataToken}, nil)
		require.NoError(mt, err)
		data, typeBits := pendingCursors(m, stream)
		assert.Equal(mt, metadataToken, data)
		assert.Equal(mt, longIDTypeBits, typeBits)
	})

	mockMongo(t, "no answer from the server leaves the type bits unset, so the next sync asks again", func(mt *mtest.T, m *Mongo) {
		// the state holds type bits of its own token: they must not survive next to another token
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcTypeBitsField: longIDTypeBits})
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 89, Name: "NetworkTimeout", Message: "connection timed out"}),
		)
		_, err := m.StreamChanges(ctx, 0, map[string]any{stream.ID(): metadataToken}, nil)
		require.NoError(mt, err)
		data, typeBits := pendingCursors(m, stream)
		assert.Equal(mt, metadataToken, data)
		assert.Nil(mt, typeBits)
	})

	mockMongo(t, "unset type bits read back from a state file as unset", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: metadataToken, cdcTypeBitsField: nil})
		assert.Nil(mt, m.state.GetCursor(stream, cdcTypeBitsField), "nil is what makes the next sync look the type bits up")
		assert.Equal(mt, metadataToken, m.state.GetCursor(stream, cdcCursorField))
	})
}

func TestStreamChangesReadsBackTypeBitsOfOldState(t *testing.T) {
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

	mockMongo(t, "state without type bits", func(mt *mtest.T, m *Mongo) {
		stream := cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData})
		require.Nil(mt, m.state.GetCursor(stream, cdcTypeBitsField))
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

		_, typeBits := pendingCursors(m, stream)
		assert.NotNil(mt, typeBits, "PostCDC writes type bits to state, so the next sync does not look them up")
	})

	mockMongo(t, "state that already has type bits asks the server nothing extra", func(mt *mtest.T, m *Mongo) {
		cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcTypeBitsField: longIDTypeBits})
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

	mockMongo(t, "state with empty type bits is not looked up again", func(mt *mtest.T, m *Mongo) {
		cdcTestMongo(mt, m, map[string]any{cdcCursorField: longIDTokenData, cdcTypeBitsField: ""})
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

// A collection may be named after another one plus "._typeBits". The two streams then share a
// text: the stream ID of one is the type bits key of the other. Each must keep its own values.
func TestTypeBitsOfOneStreamDoNotReachAnother(t *testing.T) {
	ctx := context.Background()
	ordersToken := laterToken(longIDTokenData) // metadata token of "orders", has type bits
	otherToken := laterToken(ordersToken)      // metadata token of "orders._typeBits", has none

	for _, first := range []string{"orders", "orders._typeBits"} {
		mockMongo(t, "stream read first: "+first, func(mt *mtest.T, m *Mongo) {
			orders := types.NewStream("orders", "app", nil).Wrap(0)
			other := types.NewStream("orders._typeBits", "app", nil).Wrap(0)
			require.Equal(mt, other.ID(), string(typeBitsKey(orders.ID())), "the case under test: same text for both keys")

			encoded, err := json.Marshal(map[string]any{
				"type": types.StreamType,
				"streams": []map[string]any{
					{"stream": orders.Name(), "namespace": "app", "state": map[string]any{cdcCursorField: longIDTokenData, cdcTypeBitsField: ""}},
					{"stream": other.Name(), "namespace": "app", "state": map[string]any{cdcCursorField: longIDTokenData, cdcTypeBitsField: ""}},
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

			data, typeBits := pendingCursors(m, orders)
			assert.Equal(mt, ordersToken, data, "_data of orders")
			assert.Equal(mt, longIDTypeBits, typeBits, "type bits of orders")
			data, typeBits = pendingCursors(m, other)
			assert.Equal(mt, otherToken, data, "_data of orders._typeBits")
			assert.Equal(mt, "", typeBits, "type bits of orders._typeBits")
		})
	}
}
