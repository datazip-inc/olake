package iceberg

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
)

func TestFieldValue(t *testing.T) {
	testCases := []struct {
		name     string
		value    *proto.IcebergPayload_IceRecord_FieldValue
		expected any
	}{
		// an unset value is how the reader says the stored column is NULL
		{name: "unset value is NULL", value: &proto.IcebergPayload_IceRecord_FieldValue{}, expected: nil},
		{name: "string", value: &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "hello"}}, expected: "hello"},
		// text that is not UTF-8 (the arrow writer stores a Postgres bytea as it is) comes as bytes
		{name: "bytes are text that is not UTF-8", value: &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: []byte{0xff, 'a'}}}, expected: "\xffa"},
		{name: "double", value: &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: 1.5}}, expected: 1.5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, fieldValue(tc.value))
		})
	}
}

func TestFetchRowWithoutNormalization(t *testing.T) {
	stored := func(value *proto.IcebergPayload_IceRecord_FieldValue) map[string]*proto.IcebergPayload_IceRecord_FieldValue {
		return map[string]*proto.IcebergPayload_IceRecord_FieldValue{constants.StringifiedData: value}
	}
	text := func(value string) *proto.IcebergPayload_IceRecord_FieldValue {
		return &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: value}}
	}

	testCases := []struct {
		name        string
		stored      *proto.IcebergPayload_IceRecord_FieldValue
		columns     []string
		expected    map[string]any
		expectedErr bool
	}{
		// the key's raw JSON is spliced back unchanged, and the row is parsed once for all keys
		{
			name:     "each key's raw json is returned",
			stored:   text(`{"payload":"v","doc":{"k":1}}`),
			columns:  []string{"payload", "doc"},
			expected: map[string]any{"payload": json.RawMessage(`"v"`), "doc": json.RawMessage(`{"k":1}`)},
		},
		// the stored row predates the column: keep the placeholder rather than invent a NULL
		{name: "a key missing from the stored row keeps the placeholder", stored: text(`{"id":1}`), columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		{name: "a stored JSON null keeps the placeholder", stored: text(`{"id":1,"payload":null}`), columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		{name: "a stored placeholder keeps the placeholder", stored: text(`{"id":1,"payload":"` + placeholder + `"}`), columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		// the file lacks the stored row, or the row is soft-deleted
		{name: "a NULL stored row keeps the placeholder", stored: &proto.IcebergPayload_IceRecord_FieldValue{}, columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		{name: "invalid json", stored: text(`{not json`), columns: []string{"payload"}, expectedErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &unavailableValuesResolver{stream: testConfiguredStream(false), readBuffer: newReadBuffer()}
			err := resolver.fetchRow(&pendingRead{olakeID: "1", columns: tc.columns}, stored(tc.stored))
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, resolver.readBuffer.rows["1"])
		})
	}
}
