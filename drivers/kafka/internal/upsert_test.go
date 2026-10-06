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
		name                 string
		dedupKeys            []string
		wantModeOnlyKafkaKey bool
	}{
		{
			name:                 "only _kafka_key",
			dedupKeys:            []string{Key},
			wantModeOnlyKafkaKey: true,
		},
		{
			name:                 "column id is not just _kafka_key",
			dedupKeys:            []string{"id"},
			wantModeOnlyKafkaKey: false,
		},
		{
			name:                 "_kafka_key + column id is not just _kafka_key",
			dedupKeys:            []string{Key, "id"},
			wantModeOnlyKafkaKey: false,
		},
		{
			name:                 "empty is under category not just _kafka_key",
			dedupKeys:            nil,
			wantModeOnlyKafkaKey: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantModeOnlyKafkaKey, isKafkaKeyOnlyDedup(tt.dedupKeys))
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
		{
			name:      "value has id with empty kafka key",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   "101",
				"name": "sam",
				"age":  20,
			},
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "101", data["id"])
			},
		},
		{
			name:      "value has id with kafka key present",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   "101",
				"name": "sam",
				"age":  20,
			},
			kafkaKey: "random_key",
		},
		{
			name:      "Id(passed as dedup key) is missing, dedup not taken from kafka key",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"name": "noId",
				"age":  2000,
			},
			kafkaKey:    "1222",
			wantMissing: true,
		},
		{
			name:      "all selected dedup are null -- fail",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   nil,
				"name": "sam",
				"age":  20,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name:      "dedup key = kafka key(nil) -- fails",
			dedupKeys: []string{Key},
			data: map[string]any{
				Key: nil,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name:      "selected dedup field value is empty string - works",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   "",
				"name": "sam",
				"age":  20,
			},
		},
		{
			name:      "dedupe fields some are absent",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":  "101",
				"age": 20,
			},
			check: func(t *testing.T, data map[string]any) {
				_, ok := data["name"]
				assert.False(t, ok, "absent name must stay absent")
			},
		},
		{
			name:      "dedupe fields are partial null",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "101",
				"name": nil,
				"age":  20,
			},
		},
		{
			name:      "all selected fields are present and all null - fail",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   nil,
				"name": nil,
				"age":  20,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name:      "fill id from keyFields",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"name": nil,
				"age":  20,
			},
			keyFields: map[string]any{
				"id": "101",
			},
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "101", data["id"])
			},
		},
		{
			name:      "fill _kafka_key from kafkaKey(string of Key)",
			dedupKeys: []string{Key},
			data:      nil,
			kafkaKey:  "key1",
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "key1", data[Key])
			},
		},
		{
			name:        "empty kafkaKey doesnot fill _kafka_key(part of data)",
			dedupKeys:   []string{Key},
			data:        nil,
			kafkaKey:    "",
			wantMissing: true,
		},
		{
			name:        "nil data and no field names present",
			dedupKeys:   []string{"id"},
			data:        nil,
			wantMissing: true,
		},
		{
			name:      "value from data over value from keyFields(from JSON of Key)",
			dedupKeys: []string{"id"},
			data: map[string]any{
				"id":   "from-value",
				"name": nil,
				"age":  20,
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
		{
			name:      "plain kafka key is not hashed",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: "azE="},
			want:      "azE=",
		},
		{
			name:      "json kafka key is md5 of canonical json",
			dedupKeys: []string{Key},
			data:      map[string]any{Key: `{"id":"j1"}`},
			want:      "61f0c9bca503f6cebb8bfce0b5b05244",
		},
		{
			name:      "null name hashes like missing name",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "42",
				"name": nil,
			},
			want: "2d93fb3e566ffaa3495538d858ba9eb6",
		},
		{
			name: "missing name hashes like null name",
			dedupKeys: []string{
				"id", "name",
			},
			data: map[string]any{
				"id": "42",
			},
			want: "2d93fb3e566ffaa3495538d858ba9eb6",
		},
		{
			name:      "empty string name hashes differently from null",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "42",
				"name": "",
			},
			want: "28790432152f698b954bc343d2957ad7",
		},
		{
			name:      "null id hashes like missing id",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   nil,
				"name": "sam",
			},
			want: "8fe70e84b495a91d1fd6f5ddb8af8256",
		},
		{
			name:      "missing id hashes like null id",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"name": "sam",
			},
			want: "8fe70e84b495a91d1fd6f5ddb8af8256",
		},
		{
			name:      "empty string id hashes differently from null",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "",
				"name": "sam",
			},
			want: "2b6fb895922079ebeeb2db70c050c78c",
		},
		{
			name:      "only customer_id does not collide with only order_id",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"customer_id": "42",
			},
			want: "2d93fb3e566ffaa3495538d858ba9eb6",
		},
		{
			name:      "only order_id does not collide with only customer_id",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"order_id": "42",
			},
			want: "fda611170999dbc1e9721762dab86de9",
		},
		{
			name:      "composite upsert id is not append offset 5 partition 0",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"customer_id": "c1",
				"order_id":    "o1",
			},
			want:           "ed4e0393723d1ff733dd7719b98bedec",
			notEqualAppend: true,
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

func TestGenerateOlakeIDNullVsMissingVsEmpty(t *testing.T) {
	dedupKeys := []string{"id", "name"}
	nullName := generateOlakeIDFromExistingKeys(dedupKeys, map[string]any{
		"id":   "42",
		"name": nil,
	})
	missingName := generateOlakeIDFromExistingKeys(dedupKeys, map[string]any{
		"id": "42",
	})
	emptyName := generateOlakeIDFromExistingKeys(dedupKeys, map[string]any{
		"id":   "42",
		"name": "",
	})

	assert.Equal(t, nullName, missingName, "GetKeysHash: null and missing are both <nil>")
	assert.NotEqual(t, nullName, emptyName, "empty string is not <nil>")
	assert.Equal(t, "2d93fb3e566ffaa3495538d858ba9eb6", nullName)
	assert.Equal(t, "28790432152f698b954bc343d2957ad7", emptyName)
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
			name: "json object is remarlshaled",
			key:  []byte(`{"b":1,"a":2}`),
			want: `{"a":2,"b":1}`,
		},
		{
			name: "json with leading space",
			key:  []byte(`	{"id":1}`),
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
