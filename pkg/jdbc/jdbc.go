package jdbc

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

const CDCDeletedAt = "_cdc_deleted_at"
const CDCLSN = "_cdc_lsn"
const CDCUpdatedAt = "_cdc_updated_at"

var CDCColumns = map[string]types.DataType{
	CDCDeletedAt: types.Timestamp,
	CDCLSN:       types.String,
	CDCUpdatedAt: types.Timestamp,
}

// Order by Cursor
func PostgresWithoutState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor())
}

// Order by Cursor
func PostgresWithState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
}

// Order by primary keys
func PostgresFullRefresh(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(),
		strings.Join(stream.GetStream().SourceDefinedPrimaryKey.Array(), ", "))
}
