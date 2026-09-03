package legacywriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToProtoFieldValueBinary asserts that binary and fixed[n] iceberg columns are sent through
// the proto bytes_value field, never through string_value, whose UTF-8 check rejected them.
func TestToProtoFieldValueBinary(t *testing.T) {
	raw := []byte{0xff, 0x00, 0x80, 0x41}

	fv, err := toProtoFieldValue("binary", raw)
	require.NoError(t, err)
	assert.Equal(t, raw, fv.GetBytesValue())
	assert.Equal(t, "", fv.GetStringValue())

	fv, err = toProtoFieldValue("fixed[4]", raw)
	require.NoError(t, err)
	assert.Equal(t, raw, fv.GetBytesValue())

	fv, err = toProtoFieldValue("fixed[6]", raw)
	require.NoError(t, err)
	assert.Equal(t, append(raw, 0x00, 0x00), fv.GetBytesValue(), "a short value is zero padded to the fixed width")

	_, err = toProtoFieldValue("fixed[2]", raw)
	require.Error(t, err, "a fixed[2] column must reject 4 bytes")

	_, err = toProtoFieldValue("binary", 42)
	require.Error(t, err)

	// a string column receiving bytes still becomes text, as before
	fv, err = toProtoFieldValue("string", []byte("abc"))
	require.NoError(t, err)
	assert.Equal(t, "abc", fv.GetStringValue())

	fv, err = toProtoFieldValue("string", 42)
	require.NoError(t, err)
	assert.Equal(t, "42", fv.GetStringValue())
}
