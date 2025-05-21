package waljs

import (
	"bytes"
	"fmt"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
)

type ChangeFilter struct {
	tables    map[string]protocol.Stream
	converter func(value interface{}, columnType string) (interface{}, error)
}

func NewChangeFilter(typeConverter func(value interface{}, columnType string) (interface{}, error), streams ...protocol.Stream) ChangeFilter {
	filter := ChangeFilter{
		converter: typeConverter,
		tables:    make(map[string]protocol.Stream),
	}

	for _, stream := range streams {
		filter.tables[stream.ID()] = stream
	}

	return filter
}

func (c ChangeFilter) FilterChange(lsn pglogrepl.LSN, change []byte, OnFiltered OnMessage) error {
	var changes WALMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		return fmt.Errorf("failed to parse change received from wal logs: %s", err)
	}
	if len(changes.Change) == 0 {
		return nil
	}

	buildChangesMap := func(values []interface{}, types []string, names []string) (map[string]any, error) {
		data := make(map[string]any)
		for i, val := range values {
			colType := types[i]
			logger.Infof("💙 WAL Raw Data - Column: %s, Value: %v, Type: %T, PostgreSQL Type: %s",
				names[i], val, val, colType)

			conv, err := c.converter(val, colType)
			if err != nil && err != typeutils.ErrNullValue {
				return nil, err
			}
			logger.Infof("💙 WAL Converted Data - Column: %s, Value: %v, Type: %T",
				names[i], conv, conv)
			data[names[i]] = conv
		}
		return data, nil
	}

	for _, ch := range changes.Change {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			continue
		}

		var changesMap map[string]any
		var err error

		if ch.Kind == "delete" {
			changesMap, err = buildChangesMap(ch.Oldkeys.Keyvalues, ch.Oldkeys.Keytypes, ch.Oldkeys.Keynames)
		} else {
			changesMap, err = buildChangesMap(ch.Columnvalues, ch.Columntypes, ch.Columnnames)
		}

		if err != nil {
			return fmt.Errorf("failed to convert change data: %s", err)
		}

		if err := OnFiltered(CDCChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Schema:    ch.Schema,
			Table:     ch.Table,
			Timestamp: changes.Timestamp,
			LSN:       lsn,
			Data:      changesMap,
		}); err != nil {
			return fmt.Errorf("failed to write filtered change: %s", err)
		}
	}
	return nil
}
