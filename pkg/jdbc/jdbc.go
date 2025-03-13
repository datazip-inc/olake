package jdbc

import (
	"fmt"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

func PostgresWithoutState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor())
}

func PostgresWithState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
}

func PostgresRowCountQuery(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT reltuples::bigint AS approx_row_count FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace  WHERE c.relname = '%s' AND n.nspname = '%s';`, stream.Name(), stream.Namespace())
}

func PostgresMinMaxQuery(stream protocol.Stream, filterColumn string) string {
	return fmt.Sprintf(`SELECT MIN(%s) AS min_value, MAX(%s) AS max_value FROM "%s"."%s";`, filterColumn, filterColumn, stream.Namespace(), stream.Name())
}

func PostgresRelPageCount(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT relpages FROM pg_class WHERE relname = '%s' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s')`, stream.Name(), stream.Namespace())
}

func PostgresWalLSNQuery() string {
	return `SELECT pg_current_wal_lsn()::text::pg_lsn`
}
func NextChunkEndQuery(stream protocol.Stream, filterColumn string, filterValue interface{}, batchSize int) string {
	return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM "%s"."%s" WHERE %s > %v ORDER BY %s ASC LIMIT %d) AS T`, filterColumn, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue, filterColumn, batchSize)
}

func MinQuery(stream protocol.Stream, filterColumn string, filterValue interface{}) string {
	return fmt.Sprintf(`SELECT MIN(%s) FROM "%s"."%s" WHERE %s > %v`, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue)
}

func BuildSplitScanQuery(stream protocol.Stream, filterColumn string, chunk types.Chunk) string {
	condition := ""
	if chunk.Min != nil && chunk.Max != nil {
		condition = fmt.Sprintf("%s >= %v AND %s <= %v", filterColumn, chunk.Min, filterColumn, chunk.Max)
	} else if chunk.Min != nil {
		condition = fmt.Sprintf("%s >= %v", filterColumn, chunk.Min)
	} else if chunk.Max != nil {
		condition = fmt.Sprintf("%s <= %v", filterColumn, chunk.Max)
	}

	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s`, stream.Namespace(), stream.Name(), condition)
}
