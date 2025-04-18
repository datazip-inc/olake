package utils

import (
	"database/sql"
)

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
		conv, err := converter(rawData, dbType)
		if err != nil {
			return err
		}
		dest[col] = conv
	}

	return nil
}
