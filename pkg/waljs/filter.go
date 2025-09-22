package waljs

import (
	"bytes"
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
)

type ChangeFilter struct {
	tables    map[string]types.StreamInterface
	converter func(value interface{}, columnType string) (interface{}, error)
}

func NewChangeFilter(typeConverter func(value interface{}, columnType string) (interface{}, error), streams ...types.StreamInterface) ChangeFilter {
	filter := ChangeFilter{
		converter: typeConverter,
		tables:    make(map[string]types.StreamInterface),
	}

	for _, stream := range streams {
		filter.tables[stream.ID()] = stream
	}
	return filter
}

func (c ChangeFilter) FilterWalJsChange(ctx context.Context, change []byte, OnFiltered abstract.CDCMsgFn) (*pglogrepl.LSN, int, error) {
	var changes WALMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		return nil, 0, fmt.Errorf("failed to parse change received from wal logs: %s", err)
	}
	nextLSN, err := pglogrepl.ParseLSN(changes.NextLSN)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse received lsn: %s", err)
	}

	if len(changes.Change) == 0 {
		return &nextLSN, 0, nil
	}
	buildChangesMap := func(values []interface{}, types []string, names []string) (map[string]any, error) {
		data := make(map[string]any)
		for i, val := range values {
			colType := types[i]
			conv, err := c.converter(val, colType)
			if err != nil && err != typeutils.ErrNullValue {
				return nil, err
			}
			data[names[i]] = conv
		}
		return data, nil
	}
	rowsCount := 0
	for _, ch := range changes.Change {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			continue
		}
		rowsCount++
		var changesMap map[string]any
		var err error

		if ch.Kind == "delete" {
			changesMap, err = buildChangesMap(ch.Oldkeys.Keyvalues, ch.Oldkeys.Keytypes, ch.Oldkeys.Keynames)
		} else {
			changesMap, err = buildChangesMap(ch.Columnvalues, ch.Columntypes, ch.Columnnames)
		}

		if err != nil {
			return nil, rowsCount, fmt.Errorf("failed to convert change data: %s", err)
		}

		if err := OnFiltered(ctx, abstract.CDCChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Timestamp: changes.Timestamp.Time,
			Data:      changesMap,
		}); err != nil {
			return nil, rowsCount, fmt.Errorf("failed to write filtered change: %s", err)
		}
	}
	return &nextLSN, rowsCount, nil
}

func (c ChangeFilter) FilterPgOutputChange(ctx context.Context, change []byte, OnFiltered abstract.CDCMsgFn) (*pglogrepl.LSN, int, error) {
	var changes WALMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		return nil, 0, fmt.Errorf("failed to parse change received from wal logs: %s", err)
	}
	nextLSN, err := pglogrepl.ParseLSN(changes.NextLSN)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse received lsn: %s", err)
	}

	if len(changes.Change) == 0 {
		return &nextLSN, 0, nil
	}
	buildChangesMap := func(values []interface{}, types []string, names []string) (map[string]any, error) {
		data := make(map[string]any)
		for i, val := range values {
			colType := types[i]
			conv, err := c.converter(val, colType)
			if err != nil && err != typeutils.ErrNullValue {
				return nil, err
			}
			data[names[i]] = conv
		}
		return data, nil
	}
	rowsCount := 0
	for _, ch := range changes.Change {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			continue
		}
		rowsCount++
		var changesMap map[string]any
		var err error

		if ch.Kind == "delete" {
			changesMap, err = buildChangesMap(ch.Oldkeys.Keyvalues, ch.Oldkeys.Keytypes, ch.Oldkeys.Keynames)
		} else {
			changesMap, err = buildChangesMap(ch.Columnvalues, ch.Columntypes, ch.Columnnames)
		}

		if err != nil {
			return nil, rowsCount, fmt.Errorf("failed to convert change data: %s", err)
		}

		if err := OnFiltered(ctx, abstract.CDCChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Timestamp: changes.Timestamp.Time,
			Data:      changesMap,
		}); err != nil {
			return nil, rowsCount, fmt.Errorf("failed to write filtered change: %s", err)
		}
	}
	return &nextLSN, rowsCount, nil
}
