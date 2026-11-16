package arrowwriter

import (
	"bytes"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
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

// TestTransformValue checks every partition transform against the iceberg spec. A bucket[2147483647]
// keeps the masked hash whole, so those cases pin the spec's appendix B hashes.
func TestTransformValue(t *testing.T) {
	ts := time.Date(2017, 11, 16, 22, 31, 8, 0, time.UTC)
	raw := []byte{0, 1, 2, 3}

	testCases := []struct {
		name      string
		transform string
		colType   string
		value     any
		path      string
		typed     any
		wantErr   bool
	}{
		{
			name:      "a null value is null under any transform",
			transform: "bucket[16]",
			colType:   "int",
			value:     nil,
			path:      NULL,
			typed:     nil,
		},
		{
			name:      "void drops the value",
			transform: "void",
			colType:   "string",
			value:     "abc",
			path:      NULL,
			typed:     nil,
		},
		{
			name:      "identity boolean",
			transform: "identity",
			colType:   "boolean",
			value:     true,
			path:      "true",
			typed:     true,
		},
		{
			name:      "identity int",
			transform: "identity",
			colType:   "int",
			value:     int32(7),
			path:      "7",
			typed:     int32(7),
		},
		{
			name:      "identity long",
			transform: "identity",
			colType:   "long",
			value:     int64(42),
			path:      "42",
			typed:     int64(42),
		},
		{
			name:      "identity float",
			transform: "identity",
			colType:   "float",
			value:     float32(1.5),
			path:      "1.5",
			typed:     float32(1.5),
		},
		{
			name:      "identity double",
			transform: "identity",
			colType:   "double",
			value:     2.5,
			path:      "2.5",
			typed:     2.5,
		},
		{
			name:      "identity string",
			transform: "identity",
			colType:   "string",
			value:     "abc",
			path:      "abc",
			typed:     "abc",
		},
		{
			name:      "identity timestamptz is epoch micros",
			transform: "identity",
			colType:   "timestamptz",
			value:     ts,
			path:      "2017-11-16T22:31:08+00:00",
			typed:     ts.UnixMicro(),
		},
		{
			name:      "identity zero timestamptz is null",
			transform: "identity",
			colType:   "timestamptz",
			value:     time.Time{},
			path:      NULL,
			typed:     nil,
		},
		{
			name:      "identity binary prints base64",
			transform: "identity",
			colType:   "binary",
			value:     raw,
			path:      "AAECAw==",
			typed:     raw,
		},
		{
			name:      "identity fixed prints base64",
			transform: "identity",
			colType:   "fixed[4]",
			value:     raw,
			path:      "AAECAw==",
			typed:     raw,
		},
		{
			name:      "identity decimal keeps every digit of the scale",
			transform: "identity",
			colType:   "decimal(10, 2)",
			value:     "7",
			path:      "7.00",
			typed:     decimalText("7.00"),
		},
		{
			name:      "identity decimal reads a decimal value",
			transform: "identity",
			colType:   "decimal(11, 3)",
			value:     decimalValue(t, types.DecimalOf(10, 2), "-1.5"),
			path:      "-1.500",
			typed:     decimalText("-1.500"),
		},
		{
			name:      "identity of another type passes the value through",
			transform: "identity",
			colType:   "uuid",
			value:     "f79c3e09",
			path:      "f79c3e09",
			typed:     "f79c3e09",
		},
		{
			name:      "identity string rejects a number",
			transform: "identity",
			colType:   "string",
			value:     7,
			wantErr:   true,
		},
		{
			name:      "identity decimal rejects a value past its precision",
			transform: "identity",
			colType:   "decimal(10, 2)",
			value:     "123456789",
			wantErr:   true,
		},
		{
			name:      "year",
			transform: "year",
			colType:   "timestamptz",
			value:     ts,
			path:      "2017",
			typed:     int32(47),
		},
		{
			name:      "month",
			transform: "month",
			colType:   "timestamptz",
			value:     ts,
			path:      "2017-11",
			typed:     int32(574),
		},
		{
			name:      "day",
			transform: "day",
			colType:   "timestamptz",
			value:     ts,
			path:      "2017-11-16",
			typed:     int32(17486),
		},
		{
			name:      "hour",
			transform: "hour",
			colType:   "timestamptz",
			value:     ts,
			path:      "2017-11-16-22",
			typed:     int32(419686),
		},
		{
			name:      "a time transform needs a timestamp column",
			transform: "year",
			colType:   "long",
			value:     int64(1),
			wantErr:   true,
		},
		{
			name:      "bucket int hashes as spec",
			transform: "bucket[2147483647]",
			colType:   "int",
			value:     int32(34),
			path:      "2017239379",
			typed:     int32(2017239379),
		},
		{
			name:      "bucket long hashes as spec",
			transform: "bucket[2147483647]",
			colType:   "long",
			value:     int64(34),
			path:      "2017239379",
			typed:     int32(2017239379),
		},
		{
			name:      "bucket timestamptz hashes as spec",
			transform: "bucket[2147483647]",
			colType:   "timestamptz",
			value:     ts,
			path:      "99539207",
			typed:     int32(99539207),
		},
		{
			name:      "bucket string hashes as spec",
			transform: "bucket[2147483647]",
			colType:   "string",
			value:     "iceberg",
			path:      "1210000089",
			typed:     int32(1210000089),
		},
		{
			name:      "bucket binary hashes as spec",
			transform: "bucket[2147483647]",
			colType:   "binary",
			value:     raw,
			path:      "1958800441",
			typed:     int32(1958800441),
		},
		{
			name:      "bucket fixed hashes as spec",
			transform: "bucket[2147483647]",
			colType:   "fixed[4]",
			value:     raw,
			path:      "1958800441",
			typed:     int32(1958800441),
		},
		{
			name:      "bucket decimal hashes the unscaled bytes as spec",
			transform: "bucket[2147483647]",
			colType:   "decimal(9, 2)",
			value:     "14.20",
			path:      "1646729059",
			typed:     int32(1646729059),
		},
		{
			name:      "bucket takes the hash modulo the bucket count",
			transform: "bucket[16]",
			colType:   "int",
			value:     int32(34),
			path:      "3",
			typed:     int32(3),
		},
		{
			name:      "bucket zero timestamptz is null",
			transform: "bucket[16]",
			colType:   "timestamptz",
			value:     time.Time{},
			path:      NULL,
			typed:     nil,
		},
		{
			name:      "bucket needs a positive count",
			transform: "bucket[0]",
			colType:   "int",
			value:     int32(34),
			wantErr:   true,
		},
		{
			name:      "bucket rejects a boolean column",
			transform: "bucket[16]",
			colType:   "boolean",
			value:     true,
			wantErr:   true,
		},
		{
			name:      "truncate int",
			transform: "truncate[10]",
			colType:   "int",
			value:     int32(1),
			path:      "0",
			typed:     int32(0),
		},
		{
			name:      "truncate int floors a negative value",
			transform: "truncate[10]",
			colType:   "int",
			value:     int32(-1),
			path:      "-10",
			typed:     int32(-10),
		},
		{
			name:      "truncate long floors a negative value",
			transform: "truncate[10]",
			colType:   "long",
			value:     int64(-1),
			path:      "-10",
			typed:     int64(-10),
		},
		{
			name:      "truncate string keeps the first runes",
			transform: "truncate[3]",
			colType:   "string",
			value:     "iceberg",
			path:      "ice",
			typed:     "ice",
		},
		{
			name:      "truncate string shorter than the width",
			transform: "truncate[3]",
			colType:   "string",
			value:     "ic",
			path:      "ic",
			typed:     "ic",
		},
		{
			name:      "truncate binary keeps the first bytes",
			transform: "truncate[3]",
			colType:   "binary",
			value:     raw,
			path:      "AAEC",
			typed:     []byte{0, 1, 2},
		},
		{
			name:      "truncate decimal floors the unscaled value",
			transform: "truncate[50]",
			colType:   "decimal(9, 2)",
			value:     "10.65",
			path:      "10.50",
			typed:     decimalText("10.50"),
		},
		{
			name:      "truncate decimal floors a negative value",
			transform: "truncate[50]",
			colType:   "decimal(9, 2)",
			value:     "-0.05",
			path:      "-0.50",
			typed:     decimalText("-0.50"),
		},
		{
			name:      "truncate needs a positive width",
			transform: "truncate[0]",
			colType:   "int",
			value:     int32(1),
			wantErr:   true,
		},
		{
			name:      "truncate rejects a fixed column",
			transform: "truncate[2]",
			colType:   "fixed[4]",
			value:     raw,
			wantErr:   true,
		},
		{
			name:      "an empty transform is rejected",
			transform: "",
			colType:   "int",
			value:     int32(1),
			wantErr:   true,
		},
		{
			name:      "a malformed transform is rejected",
			transform: "bucket[x]",
			colType:   "int",
			value:     int32(1),
			wantErr:   true,
		},
		{
			name:      "an unknown transform is rejected",
			transform: "foo",
			colType:   "int",
			value:     int32(1),
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path, typed, err := TransformValue(tc.value, tc.transform, tc.colType)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.path, path)
			require.Equal(t, tc.typed, typed)
		})
	}
}
