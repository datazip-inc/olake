package jdbc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

// MinMaxQuery returns the query to fetch MIN and MAX values of a column in a table
func MinMaxQuery(stream protocol.Stream, column string) string {
	return fmt.Sprintf(`SELECT MIN(%[1]s) AS min_value, MAX(%[1]s) AS max_value FROM %[2]s.%[3]s`, column, stream.Namespace(), stream.Name())
}

// NextChunkEndQuery returns the query to calculate the next chunk boundary
func NextChunkEndQuery(stream protocol.Stream, column string, chunkSize int) string {
	return fmt.Sprintf(`SELECT MAX(%[1]s) FROM (SELECT %[1]s FROM %[2]s.%[3]s WHERE %[1]s > ? ORDER BY %[1]s LIMIT %[4]d) AS subquery`, column, stream.Namespace(), stream.Name(), chunkSize)
}

// buildChunkCondition builds the condition for a chunk
func buildChunkCondition(filterColumn string, chunk types.Chunk) string {
	if chunk.Min != nil && chunk.Max != nil {
		return fmt.Sprintf("%s >= %v AND %s <= %v", filterColumn, chunk.Min, filterColumn, chunk.Max)
	} else if chunk.Min != nil {
		return fmt.Sprintf("%s >= %v", filterColumn, chunk.Min)
	}
	return fmt.Sprintf("%s <= %v", filterColumn, chunk.Max)
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
func MysqlChunkScanQuery(stream protocol.Stream, filterColumn string, chunk types.Chunk) string {
	condition := buildChunkCondition(filterColumn, chunk)
	return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), condition)
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

// MySQLMasterStatusQuery returns the query to fetch the current binlog position in MySQL
func MySQLMasterStatusQuery() string {
	return "SHOW MASTER STATUS"
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
func WithIsolation(ctx context.Context, client *sql.DB, fn func(tx *sql.Tx) error) error {
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
