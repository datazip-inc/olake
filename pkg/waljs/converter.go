package waljs

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func converter(value interface{}, columnType string) (interface{}, error) {

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

	// Timestamp types
	case "time", "timez", "date", "timestamp", "timestampz", "timestamp with time zone", "timestamp without time zone":
		switch v := value.(type) {
		case string:
			// Parsing as RFC3339 format first
			t, err := time.Parse(time.RFC3339, v)
			if err == nil {
				return t, nil
			}

			// Other common PostgreSQL date formats
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02",
				"15:04:05",
			}

			for _, format := range formats {
				if t, err := time.Parse(format, v); err == nil {
					return t, nil
				}
			}

			return v, nil
		default:
			return v, nil
		}

	// Handle binary data (bytea)
	case "bytea":
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		}

	// All other types keep as string
	default:

		switch v := value.(type) {
		case string:
			return v, nil
		case float64:
			return fmt.Sprintf("%v", v), nil
		case int:
			return fmt.Sprintf("%d", v), nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		case bool:
			return fmt.Sprintf("%t", v), nil
		case []byte:
			return string(v), nil
		case nil:
			return nil, nil
		case map[string]interface{}, []interface{}:
			// For JSON/JSONB values
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			return string(jsonBytes), nil
		}
	}

	return value, nil
}
