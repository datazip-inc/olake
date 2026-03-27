package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// StreamIncrementalChanges implements incremental sync for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.Oracle,
		Stream: stream,
		State:  o.state,
		Client: o.client,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}

	rows, err := o.client.QueryContext(ctx, incrementalQuery, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, o.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}

		if err := processFn(ctx, record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}
	return rows.Err()
}

func (o *Oracle) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimary, maxSecondary, err := jdbc.GetMaxCursorValues(ctx, o.client, constants.Oracle, stream)
	if err != nil {
		return nil, nil, err
	}

	primaryCursor, secondaryCursor := stream.Cursor()
	maxPrimary = o.normalizeCursorValue(ctx, stream, primaryCursor, maxPrimary)
	if secondaryCursor != "" {
		maxSecondary = o.normalizeCursorValue(ctx, stream, secondaryCursor, maxSecondary)
	}
	return maxPrimary, maxSecondary, nil
}

// normalizeCursorTime converts a cursor time.Time from GetMaxCursorValues to plain UTC,
// applying wall-clock strip for TZ-naive Oracle columns and .UTC() for TZ-aware ones.
func (o *Oracle) normalizeCursorValue(ctx context.Context, stream types.StreamInterface, cursorField string, value any) any {
	t, ok := value.(time.Time)
	if !ok {
		return value
	}
	var dataType string
	query := jdbc.OracleColumnDataTypeQuery(stream.Namespace(), stream.Name(), strings.ToUpper(cursorField))
	if err := o.client.QueryRowContext(ctx, query).Scan(&dataType); err != nil {
		logger.Warnf("normalizeCursorValue: failed to get DATA_TYPE for %s.%s.%s, cursor may be incorrect: %s",
			stream.Namespace(), stream.Name(), cursorField, err)
		return value
	}
	// FetchMaxCursorValues bypasses dataTypeConverter, so go-ora attaches the session timezone
	// to TZ-naive columns and the original timezone to TZ-aware columns. Normalize here so
	// the cursor values entering FormatCursorValue are already plain UTC.
	upper := strings.ToUpper(dataType)
	if strings.Contains(upper, "WITH TIME ZONE") || strings.Contains(upper, "WITH LOCAL TIME ZONE") {
		// TZ-aware: convert to the correct UTC instant.
		return t.UTC()
	}
	// TZ-naive (TIMESTAMP, DATE): strip session-TZ offset, preserve wall-clock as UTC.
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}
