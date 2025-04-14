package waljs

import (
	"bytes"
	"context"
	"fmt"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils"
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

	// Create a concurrency group with a limit (adjust as needed)
	concurrencyLimit := len(changes.Change)
	group := utils.NewCGroupWithLimit(context.Background(), concurrencyLimit)

	utils.ConcurrentInGroup(group, changes.Change, func(ctx context.Context, ch ChangeStruct) error {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			return nil
		}

		changesMap := make(map[string]any) // Prevents race conditions

		if ch.Kind == "delete" {
			for i, changedValue := range ch.Oldkeys.Keyvalues {
				changesMap[ch.Oldkeys.Keynames[i]] = changedValue
			}
		} else {
			for i, changedValue := range ch.Columnvalues {
				changesMap[ch.Columnnames[i]] = changedValue
			}
		}

		err := OnFiltered(CDCChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Schema:    ch.Schema,
			Table:     ch.Table,
			Timestamp: changes.Timestamp,
			LSN:       lsn,
			Data:      changesMap,
		})

		if err != nil {
			return fmt.Errorf("failed to write filtered change: %s", err)
		}
		return nil
	})

	// Wait for all goroutines to finish and return any errors
	return group.Block()
}
