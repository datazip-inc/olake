package utils

import "database/sql"

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
		dest[col] = *(scanValues[i].(*any)) // Dereference pointer before storing
		switch v := dest[col].(type) {
		case []byte:
			dest[col] = string(v)
		}
	}

	return nil
}
