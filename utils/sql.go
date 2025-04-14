package utils

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
)

func MapScan(rows *sql.Rows, dest map[string]any) error {
	columns, err := rows.Columns()
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
		val := *(scanValues[i].(*any))
		switch v := val.(type) {
		case []byte:
			s := string(v)
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil && json.Valid(decoded) {
				dest[col] = string(decoded)
			} else {
				dest[col] = s
			}
		default:
			dest[col] = val
		}
	}

	return nil
}
