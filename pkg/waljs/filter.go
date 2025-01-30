package waljs

import (
	"bytes"
	"fmt"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pglogrepl"
)

type ChangeFilter struct {
	tables map[string]protocol.Stream
}

type Filtered func(change WalJSChange)

func NewChangeFilter(streams ...protocol.Stream) ChangeFilter {
	filter := ChangeFilter{
		tables: make(map[string]protocol.Stream),
	}

	for _, stream := range streams {
		filter.tables[stream.ID()] = stream
	}

	return filter
}

func (c ChangeFilter) FilterChange(lsn pglogrepl.LSN, change []byte, OnFiltered Filtered) error {
	var changes WALMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		return fmt.Errorf("cant parse change from database to filter it: %s", err)
	}

	if len(changes.Change) == 0 {
		return nil
	}

	for _, ch := range changes.Change {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			continue
		}

		// builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		changesMap := map[string]any{}
		if ch.Kind == "delete" {
			for i, changedValue := range ch.Oldkeys.Keyvalues {
				changesMap[ch.Oldkeys.Keynames[i]] = changedValue
			}
		} else {
			for i, changedValue := range ch.Columnvalues {
				changesMap[ch.Columnnames[i]] = changedValue
			}
		}

		OnFiltered(WalJSChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Schema:    ch.Schema,
			Table:     ch.Table,
			Timestamp: &changes.Timestamp,
			LSN:       &lsn,
			Data:      changesMap,
		})
	}

	return nil
}
