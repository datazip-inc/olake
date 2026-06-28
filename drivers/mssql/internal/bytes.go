package driver

import (
	"database/sql"
	"fmt"
	"strings"
)

// mssqlColumnBytes returns the SQL Server on-disk byte count for a single column value.
//
// Fixed-width types use their documented SQL Server storage sizes.
// Variable-width types (varchar, nvarchar, binary, etc.) use actual byte length.
// NCHAR/NVARCHAR are stored as UTF-16 in SQL Server: len([]rune)*2 bytes per rune.
// NULL values return 0 — SQL Server stores no data bytes for NULL columns.
//
// DatabaseTypeName() from go-mssqldb returns uppercase names (BIGINT, INT, etc.).
func mssqlColumnBytes(rawVal any, typeName string) int64 {
	if rawVal == nil {
		return 0
	}
	t := strings.ToUpper(strings.TrimSpace(typeName))
	switch t {
	case "TINYINT":
		return 1
	case "SMALLINT", "SMALLMONEY":
		return 2
	case "INT":
		return 4
	case "BIGINT", "MONEY", "FLOAT", "DATETIME", "DATETIME2":
		return 8
	case "REAL":
		return 4
	case "BIT":
		return 1
	case "DATE":
		return 3
	case "TIME":
		return 5 // TIME(7) max
	case "SMALLDATETIME":
		return 4
	case "DATETIMEOFFSET":
		return 10 // DATETIMEOFFSET(7) max
	case "UNIQUEIDENTIFIER":
		return 16
	case "ROWVERSION", "TIMESTAMP": // TIMESTAMP is rowversion synonym in SQL Server
		return 8
	case "NCHAR", "NVARCHAR", "NTEXT":
		// SQL Server stores N-types as UTF-16LE: 2 bytes per BMP rune.
		if s, ok := rawVal.(string); ok {
			return int64(len([]rune(s))) * 2
		}
		return int64(len(fmt.Sprintf("%v", rawVal)))
	default:
		// CHAR, VARCHAR, TEXT, BINARY, VARBINARY, IMAGE,
		// DECIMAL, NUMERIC, XML, SQL_VARIANT, GEOMETRY, GEOGRAPHY,
		// HIERARCHYID, SYSNAME, JSON and any unknown types.
		switch v := rawVal.(type) {
		case string:
			return int64(len(v))
		case []byte:
			return int64(len(v))
		default:
			return int64(len(fmt.Sprintf("%v", v)))
		}
	}
}

// mssqlRowBytes sums column bytes for a complete row scanned via database/sql.
func mssqlRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		total += mssqlColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}

// addRowBytes is the MapScan/MapScanConcurrent callback for backfill and incremental.
func (m *MSSQL) addRowBytes(vals []any, colTypes []*sql.ColumnType) {
	m.bytesRead.Add(mssqlRowBytes(vals, colTypes))
}

// newCDCBytesCounter returns a MapScan-compatible callback for CDC rows that:
//   - Skips before-image rows (SQL Server CDC op-code 3 = UPDATE BEFORE) to avoid
//     double-counting updates — only the after-image (op-code 4) is counted.
//   - Excludes the four CDC metadata columns (__$operation, __$start_lsn,
//     __$seqval, __$update_mask) from the byte sum.
//
// The returned closure is stateful (caches opIdx on the first call) and must
// be created once per result set — not once per row.
func (m *MSSQL) newCDCBytesCounter() func([]any, []*sql.ColumnType) {
	opIdx := -1 // index of __$operation column; initialized on first call
	return func(vals []any, colTypes []*sql.ColumnType) {
		// Find __$operation column index once for this result set.
		if opIdx < 0 {
			for i, ct := range colTypes {
				if ct.Name() == "__$operation" {
					opIdx = i
					break
				}
			}
		}
		// Skip before-image (op-code 3 = UPDATE BEFORE).
		// Raw value from go-mssqldb for INT column is int64.
		if opIdx >= 0 {
			var code int64
			switch v := vals[opIdx].(type) {
			case int32:
				code = int64(v)
			case int64:
				code = v
			}
			if code == 3 {
				return
			}
		}
		// Sum only actual data columns, excluding CDC metadata (__$*).
		var total int64
		for i, v := range vals {
			if strings.HasPrefix(colTypes[i].Name(), "__$") {
				continue
			}
			total += mssqlColumnBytes(v, colTypes[i].DatabaseTypeName())
		}
		m.bytesRead.Add(total)
	}
}
