package driver

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseKafkaData(t *testing.T) {
	k := Kafka{} // schemaRegistryClient is nil
	tests := []struct {
		name          string
		record        *kgo.Record
		wantValue     map[string]interface{}
		wantKey       string
		wantKeyFields map[string]interface{}
		wantErr       bool
	}{
		{
			name: "json value and object key",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   []byte(`{"id":"k1"}`),
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
		},
		{
			name: "tombstone nil value keeps key",
			record: &kgo.Record{
				Value: nil,
				Key:   []byte(`{"id":"k1"}`),
			},
			wantValue:     nil,
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
		},
		{
			name: "empty key",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   nil,
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey: "",
		},
		{
			name: "unparseable key falls back to canonicalize",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   []byte(`not-json`),
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey: k.canonicalizeKafkaKey([]byte(`not-json`)),
		},
		{
			name: "invalid json value errors",
			record: &kgo.Record{
				Value: []byte("not-json"),
			},
			wantErr: true,
		},
		{
			name: "json object key with spaces is canonicalized",
			record: &kgo.Record{
				Value: []byte(`{"a":1}`),
				Key:   []byte(`{"id":   "k1"}`),
			},
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
			wantValue:     map[string]interface{}{"a": json.Number("1")},
		},
		{
			name: "json object key field order is canonicalized",
			record: &kgo.Record{
				Value: []byte(`{"a":1}`),
				Key:   []byte(`{"b":1,"a":2}`),
			},
			wantKey: `{"a":2,"b":1}`,
			wantKeyFields: map[string]interface{}{
				"a": json.Number("2"),
				"b": json.Number("1"),
			},
			wantValue: map[string]interface{}{"a": json.Number("1")},
		},
		{
			name: "tombstone spaced object key matches create",
			record: &kgo.Record{
				Value: nil,
				Key:   []byte(`{"id": "k1"}`),
			},
			wantValue:     nil,
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, key, keyFields, err := k.parseKafkaData(tt.record)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, value)
			assert.Equal(t, tt.wantKey, key)
			assert.Equal(t, tt.wantKeyFields, keyFields)
		})
	}
}
