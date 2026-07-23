package waljs

import (
	"strings"

	"github.com/jackc/pgtype"
)

// NumericBinaryBytes returns the size (in bytes) of a PostgreSQL NUMERIC value from
// its text representation (as sent by pgoutput or scanned by pgx), using PostgreSQL's base-10000 digit encoding
func NumericBinaryBytes(s string) int64 {
	if s == "" {
		return 3
	}
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	sl := strings.ToLower(s)
	if sl == "nan" || sl == "infinity" || sl == "inf" {
		return 3
	}

	var intPart, fracPart string
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	} else {
		intPart = s
	}
	intPart = strings.TrimLeft(intPart, "0")
	fracPart = strings.TrimRight(fracPart, "0")

	intDigits := (len(intPart) + 3) / 4
	fracDigits := (len(fracPart) + 3) / 4
	ndigits := int64(intDigits + fracDigits)
	return 3 + 2*ndigits
}

// oidStorageBytes returns the size of the data Olake reads for a column value with
// the given OID. Fixed-width types use their natural width; NUMERIC uses the
// base-10000 encoding; variable-width types use the length of the value pgoutput sent (as text).
func oidStorageBytes(oid uint32, data []byte) int64 {
	switch oid {
	case pgtype.Int2OID:
		return 2
	case pgtype.Int4OID:
		return 4
	case pgtype.Int8OID:
		return 8
	case pgtype.Float4OID:
		return 4
	case pgtype.Float8OID:
		return 8
	case pgtype.BoolOID:
		return 1
	case pgtype.DateOID:
		return 4
	case pgtype.TimeOID:
		return 8
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return 8
	case pgtype.UUIDOID:
		return 16
	case pgtype.NumericOID:
		// NUMERIC is variable-length; pgoutput sends it as text (e.g. "3.52").
		return NumericBinaryBytes(string(data))
	default:
		// Variable-width: VARCHAR, TEXT, BYTEA, JSON, JSONB, arrays, etc.
		return int64(len(data))
	}
}
