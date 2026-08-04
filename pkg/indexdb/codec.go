package indexdb

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

// Key space, one database per stream. Each family gets a distinct single-byte
// prefix so it can be scanned and range-deleted without touching the others:
//
//	0x01 <row id>       -> uvarint(file id) uvarint(position)
//	0x02 <be64 file id> -> file path
//	0x03 <file path>    -> be64 file id
//	0x04                -> retired; once held undo records
//	0x05 <name>         -> counter or snapshot metadata
const (
	prefixRow        byte = 0x01
	prefixFileByID   byte = 0x02
	prefixFileByPath byte = 0x03
	prefixMeta       byte = 0x05
)

var (
	metaSnapshot      = []byte("snapshot")
	metaNextFileID    = []byte("next_file_id")
	metaFormatVersion = []byte("format_version")
)

func rowKey(id string) []byte {
	return append(append(make([]byte, 0, 1+len(id)), prefixRow), id...)
}

func fileByIDKey(id uint64) []byte {
	return append([]byte{prefixFileByID}, be64(id)...)
}

func fileByPathKey(path string) []byte {
	return append(append(make([]byte, 0, 1+len(path)), prefixFileByPath), path...)
}

func metaKey(name []byte) []byte {
	return append(append(make([]byte, 0, 1+len(name)), prefixMeta), name...)
}

func be64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

// encodeRow packs a row value as an interned file id and an ordinal. Referencing
// the file by id rather than by its full object-store URI is what keeps a table
// of hundreds of millions of rows spending a couple of bytes per row on file
// identity instead of a hundred.
func encodeRow(fileID, position uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(make([]byte, 0, 2*binary.MaxVarintLen64), fileID), position)
}

func decodeRow(value []byte) (fileID uint64, position int64, err error) {
	fileID, read := binary.Uvarint(value)
	if read <= 0 {
		return 0, 0, fmt.Errorf("unreadable file id")
	}

	raw, readPosition := binary.Uvarint(value[read:])
	if readPosition <= 0 {
		return 0, 0, fmt.Errorf("unreadable position")
	}
	if raw > math.MaxInt64 {
		return 0, 0, fmt.Errorf("position %d out of range", raw)
	}

	return fileID, int64(raw), nil
}

func setCounter(batch *pebble.Batch, name []byte, value uint64) error {
	if err := batch.Set(metaKey(name), binary.AppendUvarint(nil, value), nil); err != nil {
		return fmt.Errorf("failed to stage row index counter %s: %s", name, err)
	}
	return nil
}

// prefixEnd returns the exclusive upper bound covering every key that starts
// with prefix. A nil result means the range runs to the end of the key space.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)

	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}

	return nil
}
