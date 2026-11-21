package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func idClause(op string, v any) bson.D {
	return bson.D{{Key: "_id", Value: bson.D{{Key: op, Value: v}}}}
}

func TestBuildChunkFilter(t *testing.T) {
	oid, err := primitive.ObjectIDFromHex("62a1c5577146a407536dbd74")
	require.NoError(t, err)
	stringBracket := bson.A{"string", "symbol"}

	testCases := []struct {
		name     string
		min, max any
		expected bson.D
	}{
		{
			name:     "same bracket uses a plain range",
			min:      float64(48),
			max:      int64(1200525751),
			expected: bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: float64(48)}, {Key: "$lt", Value: int64(1200525751)}}}},
		},
		{
			name: "number to ObjectID covers every bracket in between",
			min:  int32(1357415547),
			max:  oid,
			expected: bson.D{{Key: "$or", Value: bson.A{
				idClause("$gte", int32(1357415547)),
				idClause("$type", stringBracket),
				idClause("$type", "object"),
				idClause("$type", "binData"),
				idClause("$lt", oid),
			}}},
		},
		{
			name: "open number chunk covers every later bracket",
			min:  float64(1357415547),
			max:  nil,
			expected: bson.D{{Key: "$or", Value: bson.A{
				idClause("$gte", float64(1357415547)),
				idClause("$type", stringBracket),
				idClause("$type", "object"),
				idClause("$type", "binData"),
				idClause("$type", "objectId"),
				idClause("$type", "bool"),
				idClause("$type", "date"),
				idClause("$type", "timestamp"),
			}}},
		},
		{
			name: "open ObjectID chunk covers the brackets after ObjectID",
			min:  oid,
			max:  nil,
			expected: bson.D{{Key: "$or", Value: bson.A{
				idClause("$gte", oid),
				idClause("$type", "bool"),
				idClause("$type", "date"),
				idClause("$type", "timestamp"),
			}}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := buildChunkFilter(tc.min, tc.max)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, filter)
		})
	}

	t.Run("max before min fails", func(t *testing.T) {
		_, err := buildChunkFilter(oid, float64(5))
		assert.ErrorContains(t, err, "sorts before min")
	})

	t.Run("unsupported type fails", func(t *testing.T) {
		_, err := buildChunkFilter(primitive.Regex{Pattern: "a"}, nil)
		assert.ErrorContains(t, err, "unsupported _id type")
	})
}

func TestDecodeChunkBound(t *testing.T) {
	hex := "62a1c5577146a407536dbd74"
	oid, err := primitive.ObjectIDFromHex(hex)
	require.NoError(t, err)

	assert.Equal(t, oid, decodeChunkBound(hex, true), "hex string becomes ObjectID when the collection holds ObjectIDs")
	assert.Equal(t, hex, decodeChunkBound(hex, false), "hex string stays a string when the collection holds no ObjectIDs")
	assert.Equal(t, "order-1", decodeChunkBound("order-1", true), "non hex string stays a string")
	assert.Equal(t, float64(48), decodeChunkBound(float64(48), true), "number from state stays a number")
	assert.Nil(t, decodeChunkBound(nil, true))
}
