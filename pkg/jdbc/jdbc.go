package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/jmoiron/sqlx"
)

// MinMaxQuery returns the query to fetch MIN and MAX values of a column in a table
func MinMaxQuery(stream protocol.Stream, column string) string {
	return fmt.Sprintf(`SELECT MIN(%[1]s) AS min_value, MAX(%[1]s) AS max_value FROM %[2]s.%[3]s`, column, stream.Namespace(), stream.Name())
}

func MinMaxQueryMySQL(stream protocol.Stream, columns []string) string {

	concatCols := fmt.Sprintf("CONCAT_WS(',', %s)", strings.Join(columns, ", "))

	orderAsc := strings.Join(columns, ", ")
	descCols := make([]string, len(columns))
	for i, col := range columns {
		descCols[i] = col + " DESC"
	}
	orderDesc := strings.Join(descCols, ", ")

	return fmt.Sprintf(`
		SELECT
			(SELECT %s FROM %s.%s ORDER BY %s LIMIT 1) AS min_value,
			(SELECT %s FROM %s.%s ORDER BY %s LIMIT 1) AS max_value
	`,
		concatCols, stream.Namespace(), stream.Name(), orderAsc,
		concatCols, stream.Namespace(), stream.Name(), orderDesc,
	)
}

// NextChunkEndQuery returns the query to calculate the next chunk boundary
func NextChunkEndQuery(stream protocol.Stream, columns []string, chunkSize int) string {
	var query strings.Builder

	// SELECT with quoted and concatenated values
	fmt.Fprintf(&query, "SELECT MAX(key_str) FROM (SELECT CONCAT_WS(',', %s) AS key_str FROM `%s`.`%s`",
		strings.Join(columns, ", "),
		stream.Namespace(),
		stream.Name(),
	)

	// WHERE clause for lexicographic "greater than"
	fmt.Fprintf(&query, " WHERE ")
	for i := 0; i < len(columns); i++ {
		if i > 0 {
			query.WriteString(" OR ")
		}
		query.WriteString("(")
		for j := 0; j < i; j++ {
			fmt.Fprintf(&query, "`%s` = ? AND ", columns[j])
		}
		fmt.Fprintf(&query, "`%s` > ?", columns[i])
		query.WriteString(")")
	}

	// ORDER + LIMIT
	fmt.Fprintf(&query, " ORDER BY %s", strings.Join(columns, ", "))
	fmt.Fprintf(&query, " LIMIT %d) AS subquery", chunkSize)

	return query.String()
}

// buildChunkCondition builds the condition for a chunk
func buildChunkCondition(filterColumn string, chunk types.Chunk) string {
	if chunk.Min != nil && chunk.Max != nil {
		return fmt.Sprintf("%s >= %v AND %s < %v", filterColumn, chunk.Min, filterColumn, chunk.Max)
	} else if chunk.Min != nil {
		return fmt.Sprintf("%s >= %v", filterColumn, chunk.Min)
	}
	return fmt.Sprintf("%s < %v", filterColumn, chunk.Max)
}

func buildChunkConditionMySQL(filterColumns []string, chunk types.Chunk) string {
	// Helper to format tuple: (col1, col2, col3)
	colTuple := "(" + strings.Join(filterColumns, ", ") + ")"

	if chunk.Min != nil && chunk.Max != nil {

		chunkMin := strings.Split(chunk.Min.(string), ",")
		chunkMax := strings.Split(chunk.Max.(string), ",")

		for i, part := range chunkMin {
			chunkMin[i] = fmt.Sprintf("'%s'", strings.TrimSpace(part))
		}

		for i, part := range chunkMax {
			chunkMax[i] = fmt.Sprintf("'%s'", strings.TrimSpace(part))
		}

		chunkMinTuple := strings.Join(chunkMin, ", ")
		chunkMaxTuple := strings.Join(chunkMax, ", ")

		condition := ""

		condition += fmt.Sprintf("%s >= (%s) AND %s < (%s)", colTuple, chunkMinTuple, colTuple, chunkMaxTuple)


		return condition
	} else if chunk.Min != nil {
		chunkMin := strings.Split(chunk.Min.(string), ",")
		for i, part := range chunkMin {
			chunkMin[i] = fmt.Sprintf("'%s'", strings.TrimSpace(part))
		}
		chunkMinTuple := strings.Join(chunkMin, ", ")
		return fmt.Sprintf("%s >= (%s)", colTuple, chunkMinTuple)
	} else if chunk.Max != nil {
		chunkMax := strings.Split(chunk.Max.(string), ",")
		for i, part := range chunkMax {
			chunkMax[i] = fmt.Sprintf("'%s'", strings.TrimSpace(part))
		}
		chunkMaxTuple := strings.Join(chunkMax, ", ")
		return fmt.Sprintf("%s < (%s)", colTuple, chunkMaxTuple)
	}
	return ""
}

func CalculateTotalRows(stream protocol.Stream) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", stream.Namespace(), stream.Name())

}

func MysqlLimitOffsetScanQuery(stream protocol.Stream, filterColumns []string, chunk types.Chunk) string {
	tempString := ""

	if chunk.Min == nil {
		maxVal, _ := strconv.Atoi(chunk.Max.(string))
		tempString = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", stream.Namespace(), stream.Name(), maxVal)
	} else if chunk.Min != nil && chunk.Max != nil {
		minVal, _ := strconv.Atoi(chunk.Min.(string))
		maxVal, _ := strconv.Atoi(chunk.Max.(string))
		tempString = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d OFFSET %d", stream.Namespace(), stream.Name(), maxVal-minVal, minVal)
	} else {
		minVal, _ := strconv.Atoi(chunk.Min.(string))
		tempString = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 18446744073709551615 OFFSET %d", stream.Namespace(), stream.Name(), minVal)
	}

	return tempString
}

// PostgreSQL-Specific Queries
// TODO: Rewrite queries for taking vars as arguments while execution.

// PostgresWithoutState returns the query for a simple SELECT without state
func PostgresWithoutState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor())
}

// PostgresWithState returns the query for a SELECT with state
func PostgresWithState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
}

// PostgresRowCountQuery returns the query to fetch the estimated row count in PostgreSQL
func PostgresRowCountQuery(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT reltuples::bigint AS approx_row_count FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = '%s' AND n.nspname = '%s';`, stream.Name(), stream.Namespace())
}

// PostgresRelPageCount returns the query to fetch relation page count in PostgreSQL
func PostgresRelPageCount(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT relpages FROM pg_class WHERE relname = '%s' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s')`, stream.Name(), stream.Namespace())
}

// PostgresWalLSNQuery returns the query to fetch the current WAL LSN in PostgreSQL
func PostgresWalLSNQuery() string {
	return `SELECT pg_current_wal_lsn()::text::pg_lsn`
}

// PostgresNextChunkEndQuery generates a SQL query to fetch the maximum value of a specified column
func PostgresNextChunkEndQuery(stream protocol.Stream, filterColumn string, filterValue interface{}, batchSize int) string {
	return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM "%s"."%s" WHERE %s > %v ORDER BY %s ASC LIMIT %d) AS T`, filterColumn, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue, filterColumn, batchSize)
}

// PostgresMinQuery returns the query to fetch the minimum value of a column in PostgreSQL
func PostgresMinQuery(stream protocol.Stream, filterColumn string, filterValue interface{}) string {
	return fmt.Sprintf(`SELECT MIN(%s) FROM "%s"."%s" WHERE %s > %v`, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue)
}

// PostgresBuildSplitScanQuery builds a chunk scan query for PostgreSQL
func PostgresChunkScanQuery(stream protocol.Stream, filterColumn string, chunk types.Chunk) string {
	condition := buildChunkCondition(filterColumn, chunk)
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s`, stream.Namespace(), stream.Name(), condition)
}

// MySQL-Specific Queries

// MySQLWithoutState builds a chunk scan query for MySql
func MysqlChunkScanQuery(stream protocol.Stream, filterColumns []string, chunk types.Chunk) string {
	condition := buildChunkConditionMySQL(filterColumns, chunk)
	tempString := fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), condition)
	return tempString
}

func SelectAllQuery(stream protocol.Stream) string {
	tempString := fmt.Sprintf("SELECT * FROM `%s`.`%s`", stream.Namespace(), stream.Name())
	return tempString
}

// MySQLDiscoverTablesQuery returns the query to discover tables in a MySQL database
func MySQLDiscoverTablesQuery() string {
	return `
		SELECT 
			TABLE_NAME, 
			TABLE_SCHEMA 
		FROM 
			INFORMATION_SCHEMA.TABLES 
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_TYPE = 'BASE TABLE'
	`
}

// MySQLTableSchemaQuery returns the query to fetch schema information for a table in MySQL
func MySQLTableSchemaQuery() string {
	return `
		SELECT 
			COLUMN_NAME, 
			COLUMN_TYPE,
			DATA_TYPE, 
			IS_NULLABLE,
			COLUMN_KEY
		FROM 
			INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY 
			ORDINAL_POSITION
	`
}

// MySQLPrimaryKeyQuery returns the query to fetch the primary key column of a table in MySQL
func MySQLPrimaryKeyQuery() string {
	return `
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = ? 
        AND CONSTRAINT_NAME = 'PRIMARY' 
        LIMIT 1
	`
}

// MySQLTableRowsQuery returns the query to fetch the estimated row count of a table in MySQL
func MySQLTableRowsQuery() string {
	return `
		SELECT TABLE_ROWS
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		AND TABLE_NAME = ?
	`
}

// MySQLMasterStatusQuery returns the query to fetch the current binlog position in MySQL: mysql v8.3 and below
func MySQLMasterStatusQuery() string {
	return "SHOW MASTER STATUS"
}

// MySQLMasterStatusQuery returns the query to fetch the current binlog position in MySQL: mysql v8.4 and above
func MySQLMasterStatusQueryNew() string {
	return "SHOW BINARY LOG STATUS"
}

// MySQLTableColumnsQuery returns the query to fetch column names of a table in MySQL
func MySQLTableColumnsQuery() string {
	return `
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION
	`
}

// MySQLVersion returns the version of the MySQL server
// It returns the major and minor version of the MySQL server
func MySQLVersion(client *sqlx.DB) (int, int, error) {
	var version string
	err := client.QueryRow("SELECT @@version").Scan(&version)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get MySQL version: %w", err)
	}

	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid version format")
	}
	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid major version: %s", err)
	}

	minorVersion, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minor version: %s", err)
	}

	return majorVersion, minorVersion, nil
}

func WithIsolation(ctx context.Context, client *sqlx.DB, fn func(tx *sql.Tx) error) error {
	tx, err := client.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			fmt.Printf("transaction rollback failed: %v\n", rerr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
