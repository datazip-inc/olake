// File: driver/incremental.go
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/lib/pq"
)

const (
	bmkField      = "cursor_field"
	bmkVal        = "cursor_value"
	defaultCursor = "created_at"
)

func (p *Postgres) incremental(pool *protocol.WriterPool, stream protocol.Stream) error {
	ctx := context.TODO()
	cstream := stream.Self()

	cursorCol := pickCursor(stream)

	colType, err := p.pgColumnType(stream, cursorCol)
	if err != nil {
		return err
	}

	stateField := p.State.GetCursor(cstream, bmkField)
	stateVal := p.State.GetCursor(cstream, bmkVal)

	if stateField != cursorCol || stateVal == nil {
		if err := p.backfill(pool, stream); err != nil {
			return err
		}
		return p.initBookmark(stream, cursorCol, colType)
	}
	bookmark := stateVal

	maxVal, err := p.highWatermark(stream, cursorCol, colType)
	if err != nil {
		return err
	}
	if maxVal == nil || utils.CompareInterfaceValue(maxVal, bookmark) <= 0 {
		logger.Infof("nothing new for %s (bookmark=%v)", stream.ID(), bookmark)
		return nil
	}

	writer, err := pool.NewThread(ctx, stream, protocol.WithBackfill(false))
	if err != nil {
		return err
	}
	defer writer.Close()

	lastSeen := bookmark
	for {
		q := fmt.Sprintf(
			`SELECT * FROM %s.%s
			  WHERE %s > $1
			  ORDER BY %s ASC
			  LIMIT %d`,
			pq.QuoteIdentifier(stream.Namespace()),
			pq.QuoteIdentifier(stream.Name()),
			pq.QuoteIdentifier(cursorCol),
			pq.QuoteIdentifier(cursorCol),
			p.config.BatchSize,
		)
		rows, err := p.client.QueryContext(ctx, q, lastSeen)
		if err != nil {
			return err
		}
		defer rows.Close()

		var count int
		for rows.Next() {
			rec := make(types.Record)
			if err := jdbc.MapScan(rows, rec, p.dataTypeConverter); err != nil {
				rows.Close()
				return err
			}
			lastSeen = rec[cursorCol]
			hash := utils.GetKeysHash(rec, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			if err := writer.Insert(types.CreateRawRecord(hash, rec, "r", time.Unix(0, 0))); err != nil {
				rows.Close()
				return err
			}
			count++
		}
		rows.Close()
		if count < p.config.BatchSize {
			break
		}
	}

	p.State.SetCursor(cstream, bmkField, cursorCol)
	p.State.SetCursor(cstream, bmkVal, lastSeen)
	p.State.LogState()
	return nil
}

func pickCursor(stream protocol.Stream) string {
	if cur := stream.Self().CursorField; cur != "" {
		return cur
	}
	avail := stream.GetStream().AvailableCursorFields.Array()

	for _, f := range avail {
		if f == defaultCursor {
			return f
		}
	}
	if len(avail) == 1 {
		return avail[0]
	}
	panic(fmt.Sprintf("ambiguous cursor choices for %s", stream.ID()))
}

func (p *Postgres) pgColumnType(stream protocol.Stream, col string) (string, error) {
	var dt string
	err := p.client.QueryRow(
		`SELECT data_type
		   FROM information_schema.columns
		  WHERE table_schema=$1 AND table_name=$2 AND column_name=$3`,
		stream.Namespace(), stream.Name(), col).
		Scan(&dt)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("column %s not found in %s.%s", col,
			stream.Namespace(), stream.Name())
	}
	return dt, err
}

func (p *Postgres) highWatermark(stream protocol.Stream, col, typ string) (interface{}, error) {
	var q string
	switch typ {
	case "uuid", "json", "jsonb":
		q = fmt.Sprintf(`SELECT %s FROM %s.%s ORDER BY %s DESC LIMIT 1`,
			pq.QuoteIdentifier(col),
			pq.QuoteIdentifier(stream.Namespace()),
			pq.QuoteIdentifier(stream.Name()),
			pq.QuoteIdentifier(col))
	default:
		q = fmt.Sprintf(`SELECT MAX(%s) FROM %s.%s`,
			pq.QuoteIdentifier(col),
			pq.QuoteIdentifier(stream.Namespace()),
			pq.QuoteIdentifier(stream.Name()))
	}
	var max interface{}
	return max, p.client.QueryRow(q).Scan(&max)
}

func (p *Postgres) initBookmark(stream protocol.Stream, col, typ string) error {
	max, err := p.highWatermark(stream, col, typ)
	if err != nil {
		return err
	}
	if max != nil {
		cs := stream.Self()
		p.State.SetCursor(cs, bmkField, col)
		p.State.SetCursor(cs, bmkVal, max)
		p.State.LogState()
		logger.Infof("bookmark initialised to %v on %s for %s", max, col, stream.ID())
	}
	return nil
}
