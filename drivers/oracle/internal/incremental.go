package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// StreamIncrementalChanges implements incremental sync for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	cursorField := stream.Cursor()
	lastCursorValue := o.state.GetCursor(stream.Self(), cursorField)

	filter, err := jdbc.SQLFilter(stream, o.Type())
	if err != nil {
		return fmt.Errorf("failed to create sql filter during incremental sync: %s", err)
	}

	datatype, err := stream.Self().Stream.Schema.GetType(strings.ToLower(cursorField))
	if err != nil {
		return fmt.Errorf("cursor field %s not found in schema: %s", cursorField, err)
	}
	isTimestamp := strings.Contains(string(datatype), "timestamp")

	incrementalCondition := ""
	
	// Convert Go time format to Oracle TO_TIMESTAMP_TZ format with timezone support
	if isTimestamp {
		parsedTime := fmt.Sprintf("TO_TIMESTAMP_TZ('%s','YYYY-MM-DD\"T\"HH24:MI:SS.FF9\"Z\"')", lastCursorValue.(time.Time).UTC().Format(time.RFC3339Nano))
		incrementalCondition = fmt.Sprintf("%q >= %s", cursorField, parsedTime)
	} else {
		incrementalCondition = fmt.Sprintf("%q >= '%v'", cursorField, lastCursorValue)
	}

	filter = utils.Ternary(filter != "", fmt.Sprintf("%s AND %s", filter, incrementalCondition), incrementalCondition).(string)

	query := fmt.Sprintf("SELECT * FROM %q.%q WHERE %s ORDER BY %q",
		stream.Namespace(), stream.Name(), filter, cursorField)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s", stream.ID(), filter)

	rows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, o.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}

		if err := processFn(record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}

	return rows.Err()
}
