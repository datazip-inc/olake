package typeutils

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReformatBytes(t *testing.T) {
	raw := []byte{0xff, 0x00, 0x80, 0x41}
	ptr := &raw

	testCases := []struct {
		name     string
		dataType types.DataType
		input    any
		expected []byte
		wantErr  bool
	}{
		{"byte slice passes through", types.Binary, raw, raw, false},
		{"pointer to byte slice", types.Binary, ptr, raw, false},
		{"string contributes its utf8 bytes", types.Binary, "héllo", []byte("héllo"), false},
		{"empty slice stays empty", types.Binary, []byte{}, []byte{}, false},
		{"fixed length matches", types.FixedBinaryOf(4), raw, raw, false},
		{"short fixed value is zero padded", types.FixedBinaryOf(6), raw, []byte{0xff, 0x00, 0x80, 0x41, 0x00, 0x00}, false},
		{"empty fixed value is all zero", types.FixedBinaryOf(2), []byte{}, []byte{0x00, 0x00}, false},
		{"fixed length too long", types.FixedBinaryOf(2), raw, nil, true},
		{"length-less fixed binary accepts any length", types.FixedBinary, raw, raw, false},
		{"numbers are not bytes", types.Binary, int64(42), nil, true},
		{"maps are not bytes", types.Binary, map[string]any{"a": 1}, nil, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReformatBytes(tc.dataType, tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	var nilPtr *[]byte
	_, err := ReformatBytes(types.Binary, nilPtr)
	assert.ErrorIs(t, err, ErrNullValue)
}

// TestReformatValueBinary asserts that ReformatValue routes every binary DataType, sized or not,
// through ReformatBytes and never through the String branch that would re-encode as text.
func TestReformatValueBinary(t *testing.T) {
	raw := []byte{0xff, 0x00}

	got, err := ReformatValue(types.Binary, raw)
	require.NoError(t, err)
	assert.Equal(t, raw, got)

	got, err = ReformatValue(types.FixedBinaryOf(2), raw)
	require.NoError(t, err)
	assert.Equal(t, raw, got)

	got, err = ReformatValue(types.FixedBinaryOf(3), raw)
	require.NoError(t, err)
	assert.Equal(t, []byte{0xff, 0x00, 0x00}, got, "a short fixed value is padded like the source stores it")

	_, err = ReformatValue(types.FixedBinaryOf(1), raw)
	require.Error(t, err, "a fixed binary must reject an over-long value before parquet-go panics on it")

	got, err = ReformatValue(types.Binary, nil)
	require.NoError(t, err)
	assert.Nil(t, got)

	// a String column still turns bytes into text, as it always did
	got, err = ReformatValue(types.String, []byte("abc"))
	require.NoError(t, err)
	assert.Equal(t, "abc", got)
}

func TestExtractAndMapColumnTypeBinary(t *testing.T) {
	mapping := map[string]types.DataType{"varbinary": types.Binary, "varchar": types.String, "int": types.Int32}
	assert.Equal(t, types.Binary, ExtractAndMapColumnType("varbinary(64)", mapping))
	assert.Equal(t, types.String, ExtractAndMapColumnType("varchar(10)", mapping))
	assert.Equal(t, types.Int32, ExtractAndMapColumnType("int", mapping))
}

// TestFlattenBytes asserts the flattener keeps byte values as bytes instead of casting them to
// a string, which is what used to send non-UTF-8 bytes into a proto string field.
func TestFlattenBytes(t *testing.T) {
	raw := []byte{0xff, 0x00}
	flattener := NewFlattener(func(s string) string { return s })
	out, err := flattener.Flatten(types.Record{"payload": raw})
	require.NoError(t, err)
	assert.Equal(t, raw, out["payload"])
}

// TestFieldsProcessFixedBinary asserts that a byte value does not widen an existing
// fixed_binary(n) field to Binary: values cannot reveal a width, so they are taken as fitting.
func TestFieldsProcessFixedBinary(t *testing.T) {
	fields := Fields{"digest": NewField(types.FixedBinaryOf(4)), "blob": NewField(types.Binary)}
	changed, typeChanged, mutations := fields.Process(types.Record{"digest": []byte{1, 2, 3, 4}, "blob": []byte{0xff}})
	assert.False(t, changed)
	assert.False(t, typeChanged)
	assert.Empty(t, mutations)
	assert.Equal(t, types.FixedBinaryOf(4), fields["digest"].getType())

	// a new column seen only through values is plain Binary
	changed, _, mutations = fields.Process(types.Record{"digest": []byte{1, 2, 3, 4}, "blob": []byte{0xff}, "extra": []byte{0x00}})
	assert.True(t, changed)
	assert.Equal(t, types.Binary, mutations["extra"].getType())
}
