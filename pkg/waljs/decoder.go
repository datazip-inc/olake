package waljs

import (
	"encoding/json"
	"fmt"

	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pgtype"
)

// PgtypeDecoder uses pgtype for structured decoding of PostgreSQL binary data
type PgtypeDecoder struct {
	connInfo *pgtype.ConnInfo
}

// NewPgtypeDecoder creates a decoder with registered pgtype handlers
func NewPgtypeDecoder() *PgtypeDecoder {
	connInfo := pgtype.NewConnInfo()
	return &PgtypeDecoder{
		connInfo: connInfo,
	}
}

// DecodeBinary decodes PostgreSQL binary data based on OID into a Go value
// This eliminates the need to convert binary -> string -> type
func (d *PgtypeDecoder) DecodeBinary(data []byte, oid uint32) (interface{}, error) {
	if data == nil {
		return nil, typeutils.ErrNullValue
	}

	// Handle common types with direct decoding
	switch oid {
	case pgtype.JSONOID:
		return d.decodeJSON(data)
	case pgtype.JSONBOID:
		return d.decodeJSONB(data)
	case pgtype.UUIDOID:
		return d.decodeUUID(data)
	case pgtype.Int8OID:
		return d.decodeInt8(data)
	case pgtype.Int4OID:
		return d.decodeInt4(data)
	case pgtype.Int2OID:
		return d.decodeInt2(data)
	case pgtype.Float8OID:
		return d.decodeFloat8(data)
	case pgtype.Float4OID:
		return d.decodeFloat4(data)
	case pgtype.BoolOID:
		return d.decodeBool(data)
	case pgtype.TimestampOID:
		return d.decodeTimestamp(data)
	case pgtype.TimestamptzOID:
		return d.decodeTimestamptz(data)
	case pgtype.DateOID:
		return d.decodeDate(data)
	case pgtype.ByteaOID:
		return d.decodeBytea(data)
	case pgtype.NumericOID:
		return d.decodeNumeric(data)
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return d.decodeText(data)
	}

	// For unknown types or arrays, try generic decode or fall back to string
	dt, ok := d.connInfo.DataTypeForOID(oid)
	if !ok {
		// Try as text for unknown types
		return string(data), nil
	}

	value := dt.Value
	if decoder, ok := value.(pgtype.BinaryDecoder); ok {
		if err := decoder.DecodeBinary(d.connInfo, data); err == nil {
			return d.extractGoValue(value, oid)
		}
	}

	// Fallback to text decode
	if decoder, ok := value.(pgtype.TextDecoder); ok {
		if err := decoder.DecodeText(d.connInfo, data); err == nil {
			return d.extractGoValue(value, oid)
		}
	}

	// Final fallback to string
	return string(data), nil
}

// Type-specific decoders
func (d *PgtypeDecoder) decodeJSON(data []byte) (interface{}, error) {
	var v pgtype.JSON
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		// Try text format
		if err := v.DecodeText(d.connInfo, data); err != nil {
			return string(data), nil
		}
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return d.parseJSON(v.Bytes)
}

func (d *PgtypeDecoder) decodeJSONB(data []byte) (interface{}, error) {
	var v pgtype.JSONB
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		// Try text format
		if err := v.DecodeText(d.connInfo, data); err != nil {
			return string(data), nil
		}
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return d.parseJSON(v.Bytes)
}

func (d *PgtypeDecoder) parseJSON(data []byte) (interface{}, error) {
	// Try to parse as map first
	var mapResult map[string]interface{}
	if err := json.Unmarshal(data, &mapResult); err == nil {
		return mapResult, nil
	}

	// Try to parse as array
	var arrayResult []interface{}
	if err := json.Unmarshal(data, &arrayResult); err == nil {
		return arrayResult, nil
	}

	// Fallback to string
	return string(data), nil
}

func (d *PgtypeDecoder) decodeUUID(data []byte) (interface{}, error) {
	var v pgtype.UUID
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return string(data), nil
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", v.Bytes[0:4], v.Bytes[4:6], v.Bytes[6:8], v.Bytes[8:10], v.Bytes[10:16]), nil
}

func (d *PgtypeDecoder) decodeInt8(data []byte) (interface{}, error) {
	var v pgtype.Int8
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Int, nil
}

func (d *PgtypeDecoder) decodeInt4(data []byte) (interface{}, error) {
	var v pgtype.Int4
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Int, nil
}

func (d *PgtypeDecoder) decodeInt2(data []byte) (interface{}, error) {
	var v pgtype.Int2
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Int, nil
}

func (d *PgtypeDecoder) decodeFloat8(data []byte) (interface{}, error) {
	var v pgtype.Float8
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Float, nil
}

func (d *PgtypeDecoder) decodeFloat4(data []byte) (interface{}, error) {
	var v pgtype.Float4
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Float, nil
}

func (d *PgtypeDecoder) decodeBool(data []byte) (interface{}, error) {
	var v pgtype.Bool
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Bool, nil
}

func (d *PgtypeDecoder) decodeTimestamp(data []byte) (interface{}, error) {
	var v pgtype.Timestamp
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Time, nil
}

func (d *PgtypeDecoder) decodeTimestamptz(data []byte) (interface{}, error) {
	var v pgtype.Timestamptz
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Time, nil
}

func (d *PgtypeDecoder) decodeDate(data []byte) (interface{}, error) {
	var v pgtype.Date
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Time, nil
}

func (d *PgtypeDecoder) decodeBytea(data []byte) (interface{}, error) {
	var v pgtype.Bytea
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.Bytes, nil
}

func (d *PgtypeDecoder) decodeNumeric(data []byte) (interface{}, error) {
	var v pgtype.Numeric
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		return nil, err
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	// Convert numeric to float64
	var f float64
	if err := v.AssignTo(&f); err != nil {
		return string(data), nil
	}
	return f, nil
}

func (d *PgtypeDecoder) decodeText(data []byte) (interface{}, error) {
	var v pgtype.Text
	if err := v.DecodeBinary(d.connInfo, data); err != nil {
		// Try as raw string
		return string(data), nil
	}
	if v.Status != pgtype.Present {
		return nil, typeutils.ErrNullValue
	}
	return v.String, nil
}

// extractGoValue converts pgtype values to appropriate Go types (for generic cases)
func (d *PgtypeDecoder) extractGoValue(value pgtype.Value, oid uint32) (interface{}, error) {
	// For array types and other complex types, use Get() method
	if getter, ok := value.(interface{ Get() interface{} }); ok {
		result := getter.Get()
		if result == nil {
			return nil, typeutils.ErrNullValue
		}
		return result, nil
	}

	// Fallback: convert to AssignTo string
	var s string
	if err := value.AssignTo(&s); err == nil {
		return s, nil
	}

	return fmt.Sprintf("%v", value), nil
}
