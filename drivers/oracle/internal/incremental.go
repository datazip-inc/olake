package driver

import (
	"context"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

const (
	oracleColumnDatatypeQuery = "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'"
)

// StreamIncrementalChanges implements incremental sync for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	filter, err := jdbc.SQLFilter(stream, o.Type())
	if err != nil {
		return fmt.Errorf("failed to create sql filter during incremental sync: %s", err)
	}

	incrementalCondition, queryArgs, err := o.buildIncrementalCondition(stream)
	if err != nil {
		return fmt.Errorf("failed to format cursor condition: %s", err)
	}
	filter = utils.Ternary(filter != "", fmt.Sprintf("(%s) AND (%s)", filter, incrementalCondition), incrementalCondition).(string)

	query := fmt.Sprintf("SELECT * FROM %q.%q WHERE %s", stream.Namespace(), stream.Name(), filter)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s and args: %v", stream.ID(), filter, queryArgs)

	rows, err := o.client.QueryContext(ctx, query, queryArgs...)
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

// buildIncrementalCondition generates the incremental condition SQL based on datatype and cursor value.
func (o *Oracle) buildIncrementalCondition(stream types.StreamInterface) (string, []any, error) {
	primaryCursorField, secondaryCursorField := stream.Cursor()
	lastPrimaryCursorValue := o.state.GetCursor(stream.Self(), primaryCursorField)
	lastSecondaryCursorValue := o.state.GetCursor(stream.Self(), secondaryCursorField)
	if lastPrimaryCursorValue == nil {
		logger.Warnf("Stored primary cursor value is nil for the stream [%s]", stream.ID())
	}
	if secondaryCursorField != "" && lastSecondaryCursorValue == nil {
		logger.Warnf("Stored secondary cursor value is nil for the stream [%s]", stream.ID())
	}

	formattedValue := func(cursorField string, lastCursorValue any, argumentPosition int) (string, any, error) {
		// Get the datatype of the cursor field from streams
		datatype, err := stream.Self().Stream.Schema.GetType(strings.ToLower(cursorField))
		if err != nil {
			return "", nil, fmt.Errorf("cursor field %s not found in schema: %s", cursorField, err)
		}

		isTimestamp := strings.Contains(string(datatype), "timestamp")
		formattedValue, err := typeutils.ReformatValue(datatype, lastCursorValue)
		if err != nil {
			return "", nil, fmt.Errorf("failed to reformat value: %s", err)
		}

		query := fmt.Sprintf(oracleColumnDatatypeQuery, stream.Namespace(), stream.Name(), cursorField)
		err = o.client.QueryRow(query).Scan(&datatype)
		if err != nil {
			return "", nil, fmt.Errorf("failed to get column datatype: %s", err)
		}
		// if the cursor field is a timestamp and not timezone aware, we need to cast the value to a timestamp
		if isTimestamp && !strings.Contains(string(datatype), "TIME ZONE") {
			return fmt.Sprintf("%q >= CAST(:%d AS TIMESTAMP)", cursorField, argumentPosition), formattedValue, nil
		}

		return fmt.Sprintf("%q >= :%d", cursorField, argumentPosition), formattedValue, nil
	}
	// Last argument in formatterd value function is position of the argument in the incremental query
	incrementalCondition, primaryArg, err := formattedValue(primaryCursorField, lastPrimaryCursorValue, 1)
	if err != nil {
		return "", nil, err
	}
	queryArgs := []any{primaryArg}

	if secondaryCursorField != "" {
		secondaryIncrementalString, secondaryArg, err := formattedValue(secondaryCursorField, lastSecondaryCursorValue, 2)
		if err != nil {
			return "", nil, fmt.Errorf("failed to format secondary cursor value: %s", err)
		}

		incrementalCondition = fmt.Sprintf("%s OR (%q IS NULL AND %s)",
			incrementalCondition, primaryCursorField, secondaryIncrementalString)
		queryArgs = append(queryArgs, secondaryArg)
	}
	return incrementalCondition, queryArgs, nil
}
