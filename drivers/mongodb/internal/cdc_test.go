package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

const (
	// an insert event token whose _id is a long above the int32 range, and its type bits
	longIDTokenData    = "826AFECC2C00000B832B042C0100296E5A1004E70BAF1BFE604A458CA395D58242B1BC463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002F0411677236000004"
	longIDTypeBits     = "ggAB" // base64 of 0x82 0x00 0x01
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

// laterToken returns a token of the same shape one counter step after data.
func laterToken(data string) string {
	return data[:17] + "4" + data[18:]
}

func TestTypeBitsKey(t *testing.T) {
	assert.Equal(t, "app.orders._typeBits", typeBitsKey("app.orders"))
	// the pending type bits must not land on the key that holds the stream's _data
	assert.NotEqual(t, "app.orders", typeBitsKey("app.orders"))
	assert.NotEqual(t, typeBitsKey("app.orders"), typeBitsKey("app.customers"))
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

func TestIsResumeTokenNotFound(t *testing.T) {
	notFound := mongo.CommandError{
		Code:    280,
		Name:    "ChangeStreamFatalError",
		Message: `PlanExecutor error during aggregation :: caused by :: cannot resume stream; the resume token was not found. {_data: "82", _typeBits: BinData(0, "820001")}`,
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "token not found", err: notFound, want: true},
		{name: "token not found, wrapped", err: fmt.Errorf("change stream error: %w", notFound), want: true},
		{name: "nil", err: nil, want: false},
		{name: "same text outside a command error", err: errors.New("cannot resume stream; the resume token was not found"), want: false},
		{name: "another fatal change stream error", err: mongo.CommandError{Code: 280, Name: "ChangeStreamFatalError", Message: "something else"}, want: false},
		{name: "token older than the oplog", err: mongo.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "Resume of change stream was not possible, as the resume point may no longer be in the oplog."}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isResumeTokenNotFound(tc.err))
		})
	}
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
		assert.Empty(mt, m.lookupTypeBits(ctx, stream, longIDTokenData))
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
		assert.Empty(mt, m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "no events", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.FirstBatch),
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.NextBatch),
			mtest.CreateCursorResponse(1, changeStreamTestNS, mtest.NextBatch),
			mtest.CreateSuccessResponse(), // killCursors on close
		)
		assert.Empty(mt, m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "change stream cannot be opened", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "resume point may no longer be in the oplog"}),
		)
		assert.Empty(mt, m.lookupTypeBits(ctx, stream, longIDTokenData))
	})

	mockMongo(t, "token that cannot be decoded asks the server nothing", func(mt *mtest.T, m *Mongo) {
		for _, data := range []any{nil, "", "not hex", "82", 7} {
			assert.Empty(mt, m.lookupTypeBits(ctx, stream, data))
		}
		assert.Nil(mt, mt.GetStartedEvent(), "no command expected")
	})
}
