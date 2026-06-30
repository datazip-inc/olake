// Package encoder defines the file-format contract for the writers pipeline:
// turning arrow records into encoded bytes on a single io.Writer. The parquet
// implementation lives in subpackage encoder/parquetenc; the roller consumes
// these types.
package encoder

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// Encoder turns arrow records into encoded bytes (parquet today) written to a
// single io.Writer.
//
// It knows nothing about where the bytes ultimately live. It only encodes and
// reports how big the encoded output is so far, so the Roller can decide when to
// roll.
type Encoder interface {
	// Write appends a record's rows into the current (single) row group.
	Write(ctx context.Context, record arrow.Record) error
	// Size reports the bytes encoded into the current file so far. The Roller
	// compares this against its limit after each chunk to decide on rolling.
	Size() int64
	// Close finalizes the file (writes the footer). It does NOT close the
	// underlying io.Writer — sink-level finalization belongs to the Sink.
	Close() error
}

// NewEncoder is the constructor signature the Roller uses to wrap each freshly
// opened sink writer with an encoder.
type NewEncoder func(output io.Writer) (Encoder, error)
