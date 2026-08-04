package indexdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefixEnd(t *testing.T) {
	assert.Equal(t, []byte{0x02}, prefixEnd([]byte{0x01}))
	assert.Equal(t, []byte{0x01, 0x03}, prefixEnd([]byte{0x01, 0x02}))
	assert.Equal(t, []byte{0x02}, prefixEnd([]byte{0x01, 0xff}))
	assert.Nil(t, prefixEnd([]byte{0xff, 0xff}))
}

func TestEncodeDecodeRowRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		fileID   uint64
		position uint64
		want     int64
	}{{0, 0, 0}, {1, 127, 127}, {128, 128, 128}, {1 << 20, 1 << 40, 1 << 40}} {
		fileID, position, err := decodeRow(encodeRow(tc.fileID, tc.position))
		require.NoError(t, err)
		assert.Equal(t, tc.fileID, fileID)
		assert.Equal(t, tc.want, position)
	}
}

func TestDecodeRowRejectsCorruptValues(t *testing.T) {
	_, _, err := decodeRow(nil)
	require.Error(t, err)

	// A file id with no position following it.
	_, _, err = decodeRow([]byte{0x01})
	require.Error(t, err)
}

// Each family must own a distinct prefix, otherwise a range delete of one would
// take another with it.
func TestKeyFamiliesDoNotOverlap(t *testing.T) {
	families := map[byte]string{
		prefixRow:        "row",
		prefixFileByID:   "file by id",
		prefixFileByPath: "file by path",
		prefixMeta:       "meta",
	}
	assert.Len(t, families, 4, "two key families share a prefix")

	// 0x04 is retired and must stay unused so nothing resurrects the undo log.
	_, taken := families[0x04]
	assert.False(t, taken, "0x04 is the retired undo family and must stay free")
}
