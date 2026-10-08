package driver

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsKafkaKeyOnlyDedup(t *testing.T) {
	tests := []struct {
		name      string
		dedupKeys []string
		want      bool
	}{
		{
			name:      "only _kafka_key",
			dedupKeys: []string{Key},
			want:      true,
		},
		{
			name:      "body field",
			dedupKeys: []string{"id"},
			want:      false,
		},
		{
			name:      "_kafka_key and body field",
			dedupKeys: []string{Key, "id"},
			want:      false,
		},
		{
			name:      "empty",
			dedupKeys: nil,
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isKafkaKeyOnlyDedup(tt.dedupKeys))
		})
	}
}

func TestCheckDedupKeysExist(t *testing.T) {
	tests := []struct {
		name        string
		dedupKeys   []string
		data        map[string]any
		kafkaKey    string
		keyFields   map[string]any
		wantErr     error
		wantMissing bool
		check       func(t *testing.T, data map[string]any)
	}{
		// kafka_key_only
		{
			name:      "fill from kafkaKey",
			dedupKeys: []string{Key},
			kafkaKey:  "key1",
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "key1", data[Key])
			},
		},
		{
			name:        "empty kafkaKey does not fill",
			dedupKeys:   []string{Key},
			wantMissing: true,
		},
		{
			name:      "stamped empty string is present",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: ""},
		},
		{
			name:      "json null _kafka_key fails",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: nil},
			wantErr:   errNullDedupKeys,
		},
		// configured_single
		{
			name:      "present id",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   "101",
				"name": "sam",
			},
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "101", data["id"])
			},
		},
		{
			name:      "empty string id is present",
			dedupKeys: []string{"id"},
			data:      map[string]any{"id": ""},
		},
		{
			name:      "missing id is not taken from kafka key",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"name": "noId",
			},
			kafkaKey:    "1222",
			wantMissing: true,
		},
		{
			name:      "json null id fails",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   nil,
				"name": "sam",
			},
			wantErr: errNullDedupKeys,
		},
		{
			name:        "nil data fails missing",
			dedupKeys:   []string{"id"},
			wantMissing: true,
		},
		// configured_composite
		{
			name:      "partial missing still upserts",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":  "101",
				"age": 20,
			},
			check: func(t *testing.T, data map[string]any) {
				_, ok := data["name"]
				assert.False(t, ok)
			},
		},
		{
			name:      "partial null still upserts",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "101",
				"name": nil,
			},
		},
		{
			name:      "all null fails",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   nil,
				"name": nil,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name:        "all five missing fails",
			dedupKeys:   []string{"a", "b", "c", "d", "e"},
			data:        map[string]any{"v": 1},
			wantMissing: true,
		},
		{
			name:      "two of five missing upserts",
			dedupKeys: []string{"a", "b", "c", "d", "e"},
			data: map[string]any{
				"a": "1",
				"b": "2",
				"c": "3",
			},
		},
		{
			name:      "two of five null upserts",
			dedupKeys: []string{"a", "b", "c", "d", "e"},
			data: map[string]any{
				"a": "1",
				"b": "2",
				"c": "3",
				"d": nil,
				"e": nil,
			},
		},
		// fill from keyFields
		{
			name:      "fill id from parsed json key",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"name": nil,
			},
			keyFields: map[string]any{
				"id": "101",
			},
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "101", data["id"])
			},
		},
		{
			name:      "body value wins over keyFields",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id": "from-value",
			},
			keyFields: map[string]any{
				"id": "from-key",
			},
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "from-value", data["id"])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := checkDedupKeysExist(tt.dedupKeys, tt.data, tt.kafkaKey, tt.keyFields)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			if tt.wantMissing {
				require.Error(t, err)
				assert.False(t, errors.Is(err, errNullDedupKeys))
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, data)
			}
		})
	}
}

func TestGenerateOlakeIDFromExistingKeys(t *testing.T) {
	tests := []struct {
		name           string
		dedupKeys      []string
		data           map[string]any
		want           string
		notEqualAppend bool
	}{
		// kafka_key_only
		{
			name:      "plain string is not hashed",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: "user-1"},
			want:      "user-1",
		},
		{
			name:      "json object is md5 of canonical json",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: `{"id":"j1"}`},
			want:      "61f0c9bca503f6cebb8bfce0b5b05244",
		},
		{
			name:      "empty object is md5 of {}",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: "{}"},
			want:      "99914b932bd37a50b983c5e7c90ae93b",
		},
		{
			name:      "empty string is hashed",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: ""},
			want:      "11a32013ca961189c0b6183f637cbab5",
		},
		// configured_single
		{
			name:      "real value is not hashed",
			dedupKeys: []string{"id"},
			data:      map[string]any{"id": "42"},
			want:      "42",
		},
		{
			name:      "empty string is hashed",
			dedupKeys: []string{"id"},
			data:      map[string]any{"id": ""},
			want:      "7142df90fded5d00df2c0ba662f9b662",
		},
		// configured_composite: missing ≡ json null; empty and "<nil>" do not
		{
			name:      "missing name equals json null",
			dedupKeys: []string{"id", "name"},
			data:      map[string]any{"id": "42"},
			want:      "86adbcb5c8924a5f54a7fb595f50c0bb",
		},
		{
			name:      "empty name differs from null",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "42",
				"name": "",
			},
			want: "42607a7031d470702a7265300dffb7e4",
		},
		{
			name:      "missing id equals json null",
			dedupKeys: []string{"id", "name"},
			data:      map[string]any{"name": "sam"},
			want:      "d3cff7bca59118e818080ce26c79c6bc",
		},
		{
			name:      "empty id differs from null",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "",
				"name": "sam",
			},
			want: "699bd425295fb1f9df30b62653fc9731",
		},
		{
			name:      "only customer_id does not collide with only order_id",
			dedupKeys: []string{"customer_id", "order_id"},
			data:      map[string]any{"customer_id": "42"},
			want:      "8d5890df90386eef5cac71c29c04aef1",
		},
		{
			name:      "only order_id does not collide with only customer_id",
			dedupKeys: []string{"customer_id", "order_id"},
			data:      map[string]any{"order_id": "42"},
			want:      "ce22e01cdfae0a0e43fff48d3daebd9d",
		},
		{
			name:      "composite is not append offset-partition hash",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"customer_id": "c1",
				"order_id":    "o1",
			},
			want:           "ae0fc0c5cb5e7367e67dadd276589f2c",
			notEqualAppend: true,
		},
		{
			name:      "five keys two missing",
			dedupKeys: []string{"a", "b", "c", "d", "e"},
			data: map[string]any{
				"a": "1",
				"b": "2",
				"c": "3",
			},
			want: "4142d1620adcd26cdc264a3b65ec8564",
		},
		{
			name:      "five keys empty d differs from missing d",
			dedupKeys: []string{"a", "b", "c", "d", "e"},
			data: map[string]any{
				"a": "1",
				"b": "2",
				"c": "3",
				"d": "",
				"e": nil,
			},
			want: "54139b1108c6418085a976804ed14979",
		},
		{
			name:      "pipe in id does not collide with pipe in name",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "a|b",
				"name": "c",
			},
			want: "183cda45adb4a8a54f79297fe8d71b3a",
		},
		{
			name:      "pipe in name does not collide with pipe in id",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "a",
				"name": "b|c",
			},
			want: "a689a3eb2604ed9f28798b5cef108692",
		},
		{
			name:      "string <nil> does not collide with json null",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "42",
				"name": "<nil>",
			},
			want: "ada70fa21cea5521235e5879940789bc",
		},
		{
			name:      "number 1 does not collide with string 1",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   1,
				"name": "x",
			},
			want: "2d67deb6550cbabe2eb3cf3eb03d3525",
		},
		{
			name:      "string 1 does not collide with number 1",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "1",
				"name": "x",
			},
			want: "1a9caa71e59e4e46c09e3173c14effc9",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			olakeID := generateOlakeIDFromExistingKeys(tt.dedupKeys, tt.data)
			assert.Equal(t, tt.want, olakeID)
			if tt.notEqualAppend {
				appendID := utils.GetKeysHash(map[string]any{
					Offset:    int64(5),
					Partition: int32(0),
				}, Offset, Partition)
				assert.NotEqual(t, appendID, olakeID)
			}
		})
	}
}

func TestCanonicalizeKafkaKey(t *testing.T) {
	k := Kafka{}
	tests := []struct {
		name string
		key  []byte
		want string
	}{
		{
			name: "nil",
			key:  nil,
			want: "",
		},
		{
			name: "empty",
			key:  []byte{},
			want: "",
		},
		{
			name: "plain string is base64",
			key:  []byte("test"),
			want: "dGVzdA==",
		},
		{
			name: "json object is remarshaled",
			key:  []byte(`{"b":1,"a":2}`),
			want: `{"a":2,"b":1}`,
		},
		{
			name: "json with leading space",
			key:  []byte("\t{\"id\":1}"),
			want: `{"id":1}`,
		},
		{
			name: "invalid json object falls back to base64",
			key:  []byte(`{not-json}`),
			want: base64.StdEncoding.EncodeToString([]byte(`{not-json}`)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, k.canonicalizeKafkaKey(tt.key))
		})
	}
}

func TestPreCDCWhitespaceDedupKey(t *testing.T) {
	k := &Kafka{}
	stream := &types.ConfiguredStream{
		Stream: types.NewStream("t", "topics", nil),
		StreamMetadata: types.StreamMetadata{
			AppendMode: false,
			DedupKeys:  []string{"  "},
		},
	}
	err := k.PreCDC(context.Background(), []types.StreamInterface{stream})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty field name")
}
