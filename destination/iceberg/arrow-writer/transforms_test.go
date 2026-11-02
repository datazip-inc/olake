package arrowwriter

import (
	"bytes"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketTransform checks bucket[N] on binary and fixed columns against Iceberg. Every want is
// what iceberg-go v0.7.0's bucket transform returns for the same bytes, and 00 01 02 03 is the
// Iceberg spec's hash test vector (murmur3 -188683207).
func TestBucketTransform(t *testing.T) {
	testCases := []struct {
		name    string
		val     any
		buckets int
		colType string
		want    int32
		wantErr bool
	}{
		{
			name:    "binary hashes the spec test vector",
			val:     []byte{0x00, 0x01, 0x02, 0x03},
			buckets: 16,
			colType: "binary",
			want:    9,
		},
		{
			name:    "fixed hashes the same bytes as binary",
			val:     []byte{0x00, 0x01, 0x02, 0x03},
			buckets: 16,
			colType: "fixed[4]",
			want:    9,
		},
		{
			name:    "fixed[16] value of the mysql fixture",
			val:     []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00},
			buckets: 4,
			colType: "fixed[16]",
			want:    2,
		},
		{
			name:    "variable-length binary",
			val:     []byte{0x00, 0xff, 0x10, 0xfe},
			buckets: 7,
			colType: "binary",
			want:    4,
		},
		{
			name:    "empty binary",
			val:     []byte{},
			buckets: 16,
			colType: "binary",
			want:    0,
		},
		{
			name:    "text bytes",
			val:     []byte("olake"),
			buckets: 100,
			colType: "binary",
			want:    92,
		},
		{
			name:    "largest bucket count",
			val:     bytes.Repeat([]byte{0xff}, 16),
			buckets: math.MaxInt32,
			colType: "fixed[16]",
			want:    1696150638,
		},
		{
			name:    "zero buckets",
			val:     []byte{0x00},
			buckets: 0,
			colType: "binary",
			wantErr: true,
		},
		{
			name:    "a number hashes the bytes of its text",
			val:     42,
			buckets: 16,
			colType: "binary",
			want:    6,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathStr, typedVal, err := bucketTransform(tc.val, tc.buckets, tc.colType)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, typedVal)
			assert.Equal(t, strconv.Itoa(int(tc.want)), pathStr)
		})
	}
}

// TestTruncateTransform checks truncate[W] on binary columns against Iceberg, which keeps the first
// W bytes; every want matches iceberg-go v0.7.0. Iceberg defines no truncate on fixed.
func TestTruncateTransform(t *testing.T) {
	testCases := []struct {
		name     string
		val      any
		width    int
		colType  string
		want     []byte
		wantPath string
		wantErr  bool
	}{
		{
			name:     "binary longer than the width keeps its first bytes",
			val:      []byte{0x00, 0x01, 0x02, 0x03, 0x04},
			width:    3,
			colType:  "binary",
			want:     []byte{0x00, 0x01, 0x02},
			wantPath: "AAEC",
		},
		{
			name:     "binary shorter than the width stays whole",
			val:      []byte{0xde, 0xad},
			width:    4,
			colType:  "binary",
			want:     []byte{0xde, 0xad},
			wantPath: "3q0=",
		},
		{
			name:     "binary as long as the width stays whole",
			val:      []byte{0x00, 0xff, 0x10, 0xfe},
			width:    4,
			colType:  "binary",
			want:     []byte{0x00, 0xff, 0x10, 0xfe},
			wantPath: "AP8Q/g==",
		},
		{
			name:     "bytes that are not utf-8 are cut by byte, not by rune",
			val:      []byte{0xff, 0xfe, 0xfd},
			width:    2,
			colType:  "binary",
			want:     []byte{0xff, 0xfe},
			wantPath: "//4=",
		},
		{
			name:    "fixed has no truncate in iceberg",
			val:     []byte{0x00, 0x01, 0x02, 0x03},
			width:   2,
			colType: "fixed[4]",
			wantErr: true,
		},
		{
			name:    "zero width",
			val:     []byte{0x00},
			width:   0,
			colType: "binary",
			wantErr: true,
		},
		{
			name:     "a number truncates the bytes of its text",
			val:      42,
			width:    1,
			colType:  "binary",
			want:     []byte("4"),
			wantPath: "NA==",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathStr, typedVal, err := truncateTransform(tc.val, tc.width, tc.colType)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, typedVal)
			assert.Equal(t, tc.wantPath, pathStr)
		})
	}
}
