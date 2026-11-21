package destination

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

// unavailableJSON is the placeholder as the stored JSON row (normalization off) holds it.
const unavailableJSON = `"` + constants.UnavailableValue + `"`

// pendingRead is one stored row to read: the row it is the previous version of and the source columns wanted from it.
type pendingRead struct {
	olakeID string
	columns []string
}

// rowsToRead lists the records' rows whose previous version must be read, each once.
func (f *destinationValuesFetcher) rowsToRead(records []types.RawRecord) []*pendingRead {
	var rows []*pendingRead
	seen := make(map[string]struct{})
	for idx := range records {
		record := &records[idx]
		olakeID := olakeIDOf(record)
		if _, done := seen[olakeID]; done {
			continue
		}
		seen[olakeID] = struct{}{}

		if columns := f.columnsToRead(record); len(columns) > 0 {
			rows = append(rows, &pendingRead{olakeID: olakeID, columns: columns})
		}
	}
	return rows
}

// readRows reads the pending rows in one call of the destination. A row the destination has no previous version of
// (a filtered row, or an UPDATE that changed the primary key) gets the placeholder. The destination ends the read
// after the row that takes the bytes it sent past its limit on one read; the rows left are read by a later call.
func (f *destinationValuesFetcher) readRows(ctx context.Context, rows []*pendingRead) error {
	reads := make([]RowRead, len(rows))
	for idx, pending := range rows {
		reads[idx] = RowRead{OlakeID: pending.olakeID, Columns: f.readColumns(pending.columns)}
	}
	return f.reader.ReadRows(ctx, reads, func(row int, stored map[string]any) error {
		pending := rows[row]
		if stored == nil { // no previous version
			for _, column := range pending.columns {
				f.fetch(pending.olakeID, column, constants.UnavailableValue)
			}
			return nil
		}
		if err := f.fetchRow(pending, stored); err != nil {
			return fmt.Errorf("failed to read row[%s]: %w", pending.olakeID, err)
		}
		return nil
	})
}

// readColumns lists what to read of one stored row: _op_type, which the destination checks to send a soft-deleted
// row's columns as NULL, then the columns holding the wanted values; without normalization, the one JSON column.
func (f *destinationValuesFetcher) readColumns(columns []string) []string {
	if !f.stream.NormalizationEnabled() {
		return []string{constants.OpType, constants.StringifiedData}
	}

	read := make([]string, 0, len(columns)+1)
	read = append(read, constants.OpType)
	for _, column := range columns {
		read = append(read, f.stream.ResolveColumnName(column))
	}
	return read
}

// fetchRow keeps the wanted values of one stored row, or one part of a split row. NULL stays the placeholder:
// Postgres never leaves out a NULL, so a stored NULL means the value was never written. The destination sends NULL
// for a soft-deleted row and for a column the file lacks.
func (f *destinationValuesFetcher) fetchRow(pending *pendingRead, stored map[string]any) error {
	if f.stream.NormalizationEnabled() {
		for _, column := range pending.columns {
			value, present := stored[f.stream.ResolveColumnName(column)]
			if !present {
				continue // in another part of the row
			}
			if value == nil {
				value = constants.UnavailableValue
			}
			f.fetch(pending.olakeID, column, value)
		}
		return nil
	}

	field, present := stored[constants.StringifiedData] // without normalization the row is one JSON document
	if !present {
		return nil // in another part of the row
	}
	var row map[string]json.RawMessage
	if encoded, _ := field.(string); encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &row); err != nil {
			return fmt.Errorf("failed to parse stored row: %s", err)
		}
	}
	for _, column := range pending.columns {
		var value any = constants.UnavailableValue // also for a key missing from the stored row, which predates the column
		if raw, found := row[column]; found && string(raw) != "null" && string(raw) != unavailableJSON {
			value = raw
		}
		f.fetch(pending.olakeID, column, value)
	}
	return nil
}

// fetch keeps a value read for a row until fillRecord moves it into the row's record. A value can come twice when
// two source columns share one destination column (Foo and foo), so it replaces the old one.
func (f *destinationValuesFetcher) fetch(olakeID, column string, value any) {
	if value != constants.UnavailableValue {
		f.fromTable++
	}
	values := f.read[olakeID]
	if values == nil {
		values = make(map[string]any)
		f.read[olakeID] = values
	}
	values[column] = value
}
