package utils

import (
	"database/sql"
	"encoding/json"

	"strconv"
	"strings"
)

func Converter(value interface{}, columnType string) (interface{}, error) {

	if value == nil {
		return nil, nil
	}

	if strings.HasPrefix(columnType, "_") || strings.Contains(strings.ToUpper(columnType), "ARRAY") {
		return value, nil
	}

	// Remove any length specifiers from types (e.g., varchar(50) -> varchar)
	baseType := strings.Split(columnType, "(")[0]
	baseType = strings.ToLower(strings.TrimSpace(baseType))

	switch baseType {

	case "date", "timestamp", "timestamptz", "timestamp without time zone", "timestamp with time zone", "time", "timetz", "time without time zone", "time with time zone":
		return value, nil

	// Integer types
	case "bigint", "tinyint", "integer", "smallint", "smallserial", "int", "int2", "int4", "int8", "int64", "serial", "serial2", "serial4", "serial8", "bigserial":
		switch v := value.(type) {
		case float64:
			return int64(v), nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case json.Number:
			return v.Int64()
		}

	// Float types
	case "decimal", "numeric", "double precision", "float", "float4", "float8", "real":
		switch v := value.(type) {
		case float64:
			return v, nil
		case string:
			return strconv.ParseFloat(v, 64)
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case json.Number:
			return v.Float64()
		}

	// Boolean types
	case "bool", "boolean":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		case float64:
			return v != 0, nil
		case int:
			return v != 0, nil
		case int64:
			return v != 0, nil
		}

	// Handle binary data (bytea)
	case "bytea":
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		}
	}

	return value, nil
}

func MapScan(rows *sql.Rows, dest map[string]any) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	scanValues := make([]any, len(columns))
	for i := range scanValues {
		scanValues[i] = new(any) // Allocate pointers for scanning
	}

	if err := rows.Scan(scanValues...); err != nil {
		return err
	}

	for i, col := range columns {
		rawData := *(scanValues[i].(*any)) // Dereference pointer before storing
		dbType := types[i].DatabaseTypeName()
		conv, err := Converter(rawData, dbType)
		if err != nil {
			return err
		}
		dest[col] = conv
	}

	return nil
}
