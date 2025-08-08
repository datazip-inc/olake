package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// IncrementalChanges is not supported for PostgreSQL
func (p *Postgres) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	filter, err := jdbc.SQLFilter(stream, p.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	incrementalCondition, err := p.buildIncrementalCondition(stream)
	if err != nil {
		return fmt.Errorf("failed to format cursor condition: %s", err)
	}
	filter = utils.Ternary(filter != "", fmt.Sprintf("(%s) AND (%s)", filter, incrementalCondition), incrementalCondition).(string)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", stream.Namespace(), stream.Name(), filter)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s", stream.ID(), filter)

	rows, err := p.client.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, p.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}

		if err := processFn(record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}
	return rows.Err()
}

// buildIncrementalCondition generates the incremental condition SQL for PostgreSQL
func (p *Postgres) buildIncrementalCondition(stream types.StreamInterface) (string, error) {
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := p.state.GetCursor(stream.Self(), primaryCursor)
	lastSecondaryCursorValue := p.state.GetCursor(stream.Self(), secondaryCursor)
	if lastPrimaryCursorValue == nil {
		logger.Warnf("Stored primary cursor value is nil for the stream [%s]", stream.ID())
	}
	if secondaryCursor != "" && lastSecondaryCursorValue == nil {
		logger.Warnf("Stored secondary cursor value is nil for the stream [%s]", stream.ID())
	}

	
	incrementalCondition := fmt.Sprintf("%s >= '%v'", primaryCursor, lastPrimaryCursorValue)

	if secondaryCursor != "" && lastSecondaryCursorValue != nil {
		incrementalCondition = fmt.Sprintf(" %s OR (%s IS NULL AND %s >= '%v')", incrementalCondition, primaryCursor, secondaryCursor, lastSecondaryCursorValue)
	}

	return incrementalCondition, nil
}
