package waljs

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

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

			// Handle bigint values with special type conversion rules to preserve int64 precision.
			// This ensures that numeric values maintain their exact representation during CDC operations.
			if colType == "bigint" {
				switch v := val.(type) {
				case string:
					// Validate string format according to PostgreSQL bigint rules:
					// - No empty strings
					// - No leading plus signs
					// - No leading zeros (except single zero)
					if len(v) == 0 {
						break
					}
					if v[0] == '+' || (len(v) > 1 && v[0] == '0' && v[1] != '.') {
						break
					}
					// Convert string to int64, preserving exact value
					if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
						data[names[i]] = parsed
						continue
					}
				case float64:
					// Handle special float64 cases and ensure precision safety:
					// - Reject NaN and Infinity
					// - Enforce JavaScript's safe integer limit (2^53)
					// - Ensure whole number values only
					if math.IsNaN(v) || math.IsInf(v, 0) {
						break
					}

					const safeIntegerLimit = 1 << 53
					if math.Abs(v) >= safeIntegerLimit {
						break
					}

					if math.Floor(v) != v {
						break
					}

					// Verify lossless conversion to int64
					i64 := int64(v)
					if float64(i64) == v {
						data[names[i]] = i64
						continue
					}
				case int64:
					// Direct int64 values can be used as-is
					data[names[i]] = v
					continue
				}
			}

			// For non-bigint types or failed conversions, use standard converter
			conv, err := c.converter(val, colType)
			if err != nil && err != typeutils.ErrNullValue {
				return nil, err
			}
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
