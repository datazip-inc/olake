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

// oracleTimestampFormat defines the specific format required for Oracle's TO_TIMESTAMP_TZ function.
const oracleSQLTimestampLayout = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9\"Z\""

// StreamIncrementalChanges implements incremental sync for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := o.state.GetCursor(stream.Self(), primaryCursor)
	var lastSecondaryCursorValue any
	if secondaryCursor != "" {
		lastSecondaryCursorValue = o.state.GetCursor(stream.Self(), secondaryCursor)
	}

	filter, err := jdbc.SQLFilter(stream, o.Type())
	if err != nil {
		return fmt.Errorf("failed to create sql filter during incremental sync: %s", err)
	}

	incrementalCondition, err := formatCursorCondition(primaryCursor, secondaryCursor, stream, lastPrimaryCursorValue, lastSecondaryCursorValue)
	if err != nil {
		return fmt.Errorf("failed to format cursor condition: %s", err)
	}
	filter = utils.Ternary(filter != "", fmt.Sprintf("%s AND %s", filter, incrementalCondition), incrementalCondition).(string)

	query := fmt.Sprintf("SELECT * FROM %q.%q WHERE %s",
		stream.Namespace(), stream.Name(), filter)

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

// formatCursorCondition generates the incremental condition SQL based on datatype and cursor value.
func formatCursorCondition(primaryCursorField string, secondaryCursorField string, stream types.StreamInterface, lastPrimaryCursorValue any, lastSecondaryCursorValue any) (string, error) {

	formattedValue := func(cursorField string, lastCursorValue any) (string, error) {
		datatype, err := stream.Self().Stream.Schema.GetType(strings.ToLower(cursorField))
		if err != nil {
			return "", fmt.Errorf("cursor field %s not found in schema: %s", cursorField, err)
		}

		isTimestamp := strings.Contains(string(datatype), "timestamp")
		var formattedValue string

		if isTimestamp {
			switch val := lastCursorValue.(type) {
			case time.Time:
				formattedValue = fmt.Sprintf("TO_TIMESTAMP_TZ('%s','%s')", val.UTC().Format(time.RFC3339Nano), oracleSQLTimestampLayout)
			default:
				formattedValue = fmt.Sprintf("TO_TIMESTAMP_TZ('%s','%s')", val, oracleSQLTimestampLayout)
			}
		} else {
			formattedValue = fmt.Sprintf("'%v'", lastCursorValue)
		}

		return formattedValue, nil
	}

	primaryFormattedValue, err := formattedValue(primaryCursorField, lastPrimaryCursorValue)
	if err != nil {
		return "", err
	}

	incrementalCondition := fmt.Sprintf("(%q >= %s)", primaryCursorField, primaryFormattedValue)
	if secondaryCursorField != "" {
		secondaryFormattedValue, err := formattedValue(secondaryCursorField, lastSecondaryCursorValue)
		if err != nil {
			return "", err
		}
		incrementalCondition = fmt.Sprintf("((%q IS NULL AND %q >= %s) OR %s)", primaryCursorField, secondaryCursorField, secondaryFormattedValue, incrementalCondition)
	}

	return incrementalCondition, nil
}
