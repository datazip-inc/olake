package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

const oplogNS = "local.oplog.rs"

// mockMongo runs fn against a mock deployment: each server round trip consumes the next
// response queued with mt.AddMockResponses, so no MongoDB server is needed.
func mockMongo(t *testing.T, name string, fn func(mt *mtest.T, m *Mongo)) {
	t.Helper()
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run(name, func(mt *mtest.T) {
		fn(mt, &Mongo{client: mt.Client, config: &Config{Database: "app"}})
	})
}

func oplogEntry(seconds uint32) bson.D {
	return bson.D{{Key: "ts", Value: primitive.Timestamp{T: seconds, I: 1}}}
}

func TestMongoPrerequisiteChecks(t *testing.T) {
	required := map[string]bool{}
	for _, c := range (&Mongo{}).prerequisiteChecks() {
		required[c.Name] = c.Required
		assert.NotEmpty(t, c.Description, c.Name)
		assert.NotEmpty(t, c.Recommended, c.Name)
		assert.NotNil(t, c.Check, c.Name)
	}
	// no CDC intent in the config yet, so nothing may block setup
	assert.Equal(t, map[string]bool{
		"change_streams":  false,
		"oplog_retention": false,
	}, required)
}

func TestMongoCheckChangeStreams(t *testing.T) {
	mockMongo(t, "available", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, "app.$cmd.aggregate", mtest.FirstBatch),
		)

		current, ok, err := m.checkChangeStreams(context.Background())
		require.NoError(mt, err)
		assert.Equal(mt, "available", current)
		assert.True(mt, ok)
	})

	mockMongo(t, "standalone server", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{
			Code: errCodeChangeStreamsDisabled, Name: "Location40573",
			Message: "The $changeStream stage is only supported on replica sets",
		}))

		current, ok, err := m.checkChangeStreams(context.Background())
		require.NoError(mt, err)
		assert.Equal(mt, "standalone server", current)
		assert.False(mt, ok)
	})

	mockMongo(t, "not authorized", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{
			Code: errCodeUnauthorized, Name: "Unauthorized",
			Message: "not authorized on app to execute command { aggregate: 1 }",
		}))

		current, ok, err := m.checkChangeStreams(context.Background())
		require.NoError(mt, err)
		assert.Equal(mt, "not authorized", current)
		assert.False(mt, ok)
	})

	mockMongo(t, "other error is returned", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{
			Code: 2, Name: "BadValue", Message: "bad value",
		}))

		_, ok, err := m.checkChangeStreams(context.Background())
		require.Error(mt, err)
		assert.False(mt, ok)
	})
}

func TestMongoCheckOplogRetention(t *testing.T) {
	const start = uint32(1_700_000_000)
	days := func(n float64) uint32 { return uint32(n * (24 * time.Hour).Seconds()) }

	tests := []struct {
		name        string
		window      uint32
		wantCurrent string
		wantOK      bool
	}{
		{name: "10 days", window: days(10), wantCurrent: "10 days", wantOK: true},
		{name: "exactly 7 days", window: days(7), wantCurrent: "7 days", wantOK: true},
		{name: "36 hours", window: days(1.5), wantCurrent: "1.5 days", wantOK: false},
		{name: "6 hours", window: days(0.25), wantCurrent: "6 hours", wantOK: false},
	}
	for _, tc := range tests {
		mockMongo(t, tc.name, func(mt *mtest.T, m *Mongo) {
			mt.AddMockResponses(
				mtest.CreateCursorResponse(0, oplogNS, mtest.FirstBatch, oplogEntry(start)),
				mtest.CreateCursorResponse(0, oplogNS, mtest.FirstBatch, oplogEntry(start+tc.window)),
			)

			current, ok, err := m.checkOplogRetention(context.Background())
			require.NoError(mt, err)
			assert.Equal(mt, tc.wantCurrent, current)
			assert.Equal(mt, tc.wantOK, ok)
		})
	}

	mockMongo(t, "no oplog on a standalone server", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(mtest.CreateCursorResponse(0, oplogNS, mtest.FirstBatch))

		current, ok, err := m.checkOplogRetention(context.Background())
		require.NoError(mt, err)
		assert.Equal(mt, "no oplog (standalone)", current)
		assert.False(mt, ok)
	})

	// e.g. no read role on local, or a mongos: reported as unavailable by the runner
	mockMongo(t, "unreadable oplog is returned as an error", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{
			Code: errCodeUnauthorized, Name: "Unauthorized", Message: "not authorized on local",
		}))

		_, ok, err := m.checkOplogRetention(context.Background())
		require.Error(mt, err)
		assert.False(mt, ok)
	})

	mockMongo(t, "error reading the newest entry is returned", func(mt *mtest.T, m *Mongo) {
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, oplogNS, mtest.FirstBatch, oplogEntry(start)),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 2, Name: "BadValue", Message: "bad value"}),
		)

		_, ok, err := m.checkOplogRetention(context.Background())
		require.Error(mt, err)
		assert.False(mt, ok)
	})
}
