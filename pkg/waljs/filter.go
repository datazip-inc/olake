package waljs

import (
	"bytes"
	"fmt"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils"
	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
)

type ChangeFilter struct {
	tables map[string]protocol.Stream
}

func NewChangeFilter(streams ...protocol.Stream) ChangeFilter {
	filter := ChangeFilter{
		tables: make(map[string]protocol.Stream),
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

	for _, ch := range changes.Change {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			continue
		}

		data := make(map[string]any)
		if ch.Kind == "delete" {
			for i, val := range ch.Oldkeys.Keyvalues {
				colType := ch.Oldkeys.Keytypes[i]
				conv, err := utils.Converter(val, colType)
				if err != nil {
					return err
				}
				data[ch.Oldkeys.Keynames[i]] = conv
			}
		} else {
			for i, val := range ch.Columnvalues {
				colType := ch.Columntypes[i]
				conv, err := utils.Converter(val, colType)
				if err != nil {
					return err
				}
				data[ch.Columnnames[i]] = conv
			}
		}

		if err := OnFiltered(CDCChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Schema:    ch.Schema,
			Table:     ch.Table,
			Timestamp: changes.Timestamp,
			LSN:       lsn,
			Data:      data,
		}); err != nil {
			return fmt.Errorf("failed to write filtered change: %s", err)
		}
	}
	return nil
}
