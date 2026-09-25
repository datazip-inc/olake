package driver

import (
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

func TestNewUpsertConfig(t *testing.T) {
	tests := []struct {
		name                 string
		meta                 types.StreamMetadata
		dedupKeys            []string
		wantErr              bool
		wantEnabled          bool
		wantTombstoneDeletes bool
	}{
		{
			name:                 "append mode skips dedup validation",
			meta:                 types.StreamMetadata{AppendMode: true},
			dedupKeys:            nil,
			wantErr:              false,
			wantEnabled:          false,
			wantTombstoneDeletes: false,
		},
		{
			name:      "upsert empty dedup keys errors",
			meta:      types.StreamMetadata{AppendMode: false},
			dedupKeys: nil,
			wantErr:   true,
		},
		{
			name:      "upsert whitespace dedup key errors",
			meta:      types.StreamMetadata{AppendMode: false},
			dedupKeys: []string{"  "},
			wantErr:   true,
		},
		{
			name:                 "only kafka key enables tombstone deletes",
			meta:                 types.StreamMetadata{AppendMode: false},
			dedupKeys:            []string{Key},
			wantErr:              false,
			wantEnabled:          true,
			wantTombstoneDeletes: true,
		},
		{
			name:                 "message column dedup is upsert only",
			meta:                 types.StreamMetadata{AppendMode: false},
			dedupKeys:            []string{"id"},
			wantErr:              false,
			wantEnabled:          true,
			wantTombstoneDeletes: false,
		},
		{
			name:                 "kafka key + column as dedup",
			meta:                 types.StreamMetadata{AppendMode: false},
			dedupKeys:            []string{Key, "id"},
			wantErr:              false,
			wantEnabled:          true,
			wantTombstoneDeletes: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewUpsertConfig(tt.meta, tt.dedupKeys)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, cfg.Enabled)
			assert.Equal(t, tt.wantTombstoneDeletes, cfg.AllowTombstoneDeletes)
		})
	}
}

func TestCheckDedupKeysExist(t *testing.T) {
	tests := []struct {
		name        string
		cfg         UpsertConfig
		data        map[string]any
		kafkaKey    string
		keyFields   map[string]any
		wantErr     error
		wantMissing bool
		check       func(t *testing.T, data map[string]any)
	}{
		{
			name: "value has id with empty kafka key",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
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
			name: "value has id with kafka key present",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
			data: map[string]any{
				"id":   "101",
				"name": "sam",
				"age":  20,
			},
			kafkaKey: "random_key",
		},
		{
			name: "Id(passed as dedup key) is missing, dedup not taken from kafka key",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
			data: map[string]any{
				"name": "noId",
				"age":  2000,
			},
			kafkaKey:    "1222",
			wantMissing: true,
		},
		{
			name: "all selected dedup are null -- fail",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
			data: map[string]any{
				"id":   nil,
				"name": "sam",
				"age":  20,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name: "dedup key = kafka key(nil) -- fails",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{Key},
			},
			data: map[string]any{
				Key: nil,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name: "selected dedup field value is empty string - works",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
			data: map[string]any{
				"id":   "",
				"name": "sam",
				"age":  20,
			},
		},
		{
			name: "dedupe fields some are absent",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id", "name"},
			},
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
			name: "dedupe fields are partial null",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id", "name"},
			},
			data: map[string]any{
				"id":   "101",
				"name": nil,
				"age":  20,
			},
		},
		{
			name: "all selected fields are present and all null - fail",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id", "name"},
			},
			data: map[string]any{
				"id":   nil,
				"name": nil,
				"age":  20,
			},
			wantErr: errNullDedupKeys,
		},
		{
			name: "fill id from keyFields",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
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
			name: "fill _kafka_key from kafkaKey(string of Key)",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{Key},
			},
			data:     nil,
			kafkaKey: "key1",
			check: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "key1", data[Key])
			},
		},
		{
			name: "empty kafkaKey doesnot fill _kafka_key(part of data)",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{Key},
			},
			data:        nil,
			kafkaKey:    "",
			wantMissing: true,
		},
		{
			name: "nil data and no field names present",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
			data:        nil,
			wantMissing: true,
		},
		{
			name: "value from data over value from keyFields(from JSON of Key)",
			cfg: UpsertConfig{
				Enabled:   true,
				DedupKeys: []string{"id"},
			},
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
			data, err := tt.cfg.checkDedupKeysExist(tt.data, tt.kafkaKey, tt.keyFields)
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
			name:      "partial null name, same id as missing name - null so drop from olakeID",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "42",
				"name": nil,
			},
			want: "upsert|id=42",
		},
		{
			name: "missing name same id as partial null - missing column name included with empty value in olakeID",
			dedupKeys: []string{
				"id", "name",
			},
			data: map[string]any{
				"id": "42",
			},
			want: "upsert|id=42|name=",
		},
		{
			name:      "empty string name is kept as empty slot - include column name with empy value in olakeID",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "42",
				"name": "",
			},
			want: "upsert|id=42|name=",
		},
		{
			name:      "null id is dropped, name kept",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   nil,
				"name": "sam",
			},
			want: "upsert|name=sam",
		},
		{
			name:      "missing id keeps empty slot, name kept",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"name": "sam",
			},
			want: "upsert|id=|name=sam",
		},
		{
			name:      "empty string id is kept as empty slot, name kept",
			dedupKeys: []string{"id", "name"},
			data: map[string]any{
				"id":   "",
				"name": "sam",
			},
			want: "upsert|id=|name=sam",
		},
		{
			name:      "only customer_id does not effect with only order_id",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"customer_id": "42",
			},
			want: "upsert|customer_id=42|order_id=",
		},
		{
			name:      "only order_id does not effect with only customer_id",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"order_id": "42",
			},
			want: "upsert|customer_id=|order_id=42",
		},
		{
			name:      "composite upsert id is not append offset 5 partition 0",
			dedupKeys: []string{"customer_id", "order_id"},
			data: map[string]any{
				"customer_id": "5",
				"order_id":    "0",
			},
			want:           "upsert|customer_id=5|order_id=0",
			notEqualAppend: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := UpsertConfig{
				DedupKeys: tt.dedupKeys,
			}
			olakeID := cfg.generateOlakeIDFromExistingKeys(tt.data)
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
	cfg := UpsertConfig{DedupKeys: []string{"id", "name"}}

	nullName := cfg.generateOlakeIDFromExistingKeys(map[string]any{
		"id":   "42",
		"name": nil,
	})
	missingName := cfg.generateOlakeIDFromExistingKeys(map[string]any{
		"id": "42",
	})
	emptyName := cfg.generateOlakeIDFromExistingKeys(map[string]any{
		"id":   "42",
		"name": "",
	})

	assert.Equal(t, "upsert|id=42", nullName)
	assert.Equal(t, "upsert|id=42|name=", missingName)
	assert.Equal(t, missingName, emptyName, "missing and empty string must share the empty slot")
	assert.NotEqual(t, nullName, missingName, "null is dropped; missing keeps the column slot")
	assert.NotEqual(t, nullName, emptyName, "null is dropped; empty string is kept")
}
