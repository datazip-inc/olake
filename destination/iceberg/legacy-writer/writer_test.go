package legacywriter

import (
	"fmt"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToProtoFieldValue(t *testing.T) {
	sample := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	raw := []byte{0xff, 0x00, 0x80, 0x41}

	testCases := []struct {
		name    string
		iceType string
		value   any
		want    any
		wantErr bool
	}{
		{
			name:    "boolean",
			iceType: "boolean",
			value:   true,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: true},
		},
		{
			name:    "int",
			iceType: "int",
			value:   int32(7),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_IntValue{IntValue: 7},
		},
		{
			name:    "long",
			iceType: "long",
			value:   int64(42),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: 42},
		},
		{
			name:    "float",
			iceType: "float",
			value:   float32(1.5),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_FloatValue{FloatValue: 1.5},
		},
		{
			name:    "double",
			iceType: "double",
			value:   2.5,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: 2.5},
		},
		{
			name:    "timestamptz travels as epoch millis",
			iceType: "timestamptz",
			value:   sample,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: sample.UnixMilli()},
		},
		{
			name:    "bytes into a binary column",
			iceType: "binary",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: raw},
		},
		{
			name:    "text into a binary column travels as its bytes",
			iceType: "binary",
			value:   "abc",
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: []byte("abc")},
		},
		{
			name:    "bytes of the exact width",
			iceType: "fixed[4]",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: raw},
		},
		{
			name:    "a short value is zero padded to the fixed width",
			iceType: "fixed[6]",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: []byte{0xff, 0x00, 0x80, 0x41, 0x00, 0x00}},
		},
		{
			name:    "a value wider than the column is rejected",
			iceType: "fixed[2]",
			value:   raw,
			wantErr: true,
		},
		{
			name:    "a value that is not bytes is rejected",
			iceType: "binary",
			value:   42,
			wantErr: true,
		},
		{
			name:    "utf-8 bytes into a string column become text",
			iceType: "string",
			value:   []byte("abc"),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: fmt.Sprintf("%v", []byte("abc"))},
		},
		{
			name:    "a number into a string column becomes text",
			iceType: "string",
			value:   42,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "42"},
		},
		{
			name:    "a type olake does not model becomes text",
			iceType: "date",
			value:   "2024-01-02",
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "2024-01-02"},
		},
		{
			name:    "bytes that are not utf-8",
			iceType: "string",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: fmt.Sprintf("%v", raw)},
		},
		{
			name:    "a value that is not a boolean is rejected",
			iceType: "boolean",
			value:   map[string]any{"a": 1},
			wantErr: true,
		},
		{
			name:    "a value that is not an int is rejected",
			iceType: "int",
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a long is rejected",
			iceType: "long",
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a float is rejected",
			iceType: "float",
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a double is rejected",
			iceType: "double",
			value:   "abc",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fv, err := toProtoFieldValue(tc.iceType, tc.value)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, fv.GetValue())
		})
	}
}

func TestRawDataColumnBuffer(t *testing.T) {
	record := types.RawRecord{
		Data:         map[string]any{"blob": []byte{0xff, 0x00}},
		OlakeColumns: map[string]any{constants.OlakeID: "abc"},
	}
	protoSchema := []*proto.IcebergPayload_SchemaField{
		{Key: constants.OlakeID, IceType: "string"},
		{Key: constants.StringifiedData, IceType: "string"},
		{Key: "absent", IceType: "string"},
	}

	values, err := RawDataColumnBuffer(record, protoSchema)
	require.NoError(t, err)
	require.Len(t, values, 3)

	assert.Equal(t, &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "abc"}, values[0].GetValue())
	assert.Equal(t,
		&proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: `{"blob":"/wA="}`},
		values[1].GetValue(),
		"the denormalized column is json, so raw bytes travel base64 encoded and stay valid utf-8")
	assert.Nil(t, values[2], "a schema field the record does not carry is sent unset")
}
