package utils

import "database/sql"

func MapScan(rows *sql.Rows, dest map[string]any) error {
	// Scan each row and store data in a map
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	scanValues := make([]any, len(columns))
	for i := range scanValues {
		scanValues[i] = new(any)
	}

	err = rows.Scan(scanValues...)
	if err != nil {
		return err
	}

	for i, val := range scanValues {
		dest[columns[i]] = val
	}

	return nil
}
