package waljs

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
)

// Change represents a single WAL change record
type Change struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	Columnnames  []string      `json:"columnnames"`
	Columntypes  []string      `json:"columntypes"`
	Columnvalues []interface{} `json:"columnvalues"`
	Oldkeys      struct {
		Keynames  []string      `json:"keynames"`
		Keytypes  []string      `json:"keytypes"`
		Keyvalues []interface{} `json:"keyvalues"`
	} `json:"oldkeys"`
}

// WALMessage represents the structure of WAL messages

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
		// var changes WALMessage
	}
	return filter
}

func (c ChangeFilter) FilterChange(change []byte, OnFiltered abstract.CDCMsgFn) (*pglogrepl.LSN, int, error) {
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

	var rowsCount int64

	changesList := make([]Change, len(changes.Change))
	for i, ch := range changes.Change {
		changesList[i] = Change{
			Kind:         ch.Kind,
			Schema:       ch.Schema,
			Table:        ch.Table,
			Columnnames:  ch.Columnnames,
			Columntypes:  ch.Columntypes,
			Columnvalues: ch.Columnvalues,
			Oldkeys: struct {
				Keynames  []string      `json:"keynames"`
				Keytypes  []string      `json:"keytypes"`
				Keyvalues []interface{} `json:"keyvalues"`
			}{
				Keynames:  ch.Oldkeys.Keynames,
				Keytypes:  ch.Oldkeys.Keytypes,
				Keyvalues: ch.Oldkeys.Keyvalues,
			},
		}
	}

	// Use utils.Concurrent to process changes in parallel
	// Concurrency count of 10 is a reasonable default that can be made configurable if needed
	err = utils.Concurrent(context.Background(), changesList, 10, func(_ context.Context, ch Change, _ int) error {
		stream, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			return nil
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

		if err := OnFiltered(abstract.CDCChange{
			Stream:    stream,
			Kind:      ch.Kind,
			Timestamp: changes.Timestamp,
			Data:      changesMap,
		}); err != nil {
			return fmt.Errorf("failed to write filtered change: %s", err)
		}

		atomic.AddInt64(&rowsCount, 1)
		return nil
	})

	if err != nil {
		return nil, int(rowsCount), err
	}

	return &nextLSN, int(rowsCount), nil
}
