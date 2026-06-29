// Package roller is the per-partition layer of the writers pipeline: it drives
// one file stream — chunk -> convert -> encode -> size-check -> roll — over an
// encoder.Encoder and a sink.StagedSink. The top-level partitioned writer (package
// writers) holds one Roller per partition key; this package knows nothing about
// partitioning.
package roller

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/writers/encoder"
	"github.com/datazip-inc/olake/writers/sink"
)

// Converter turns a slice of domain rows into one arrow record. It is the only
// type-specific piece a backend supplies per Roller:
//
//	data files         -> Converter[*types.RawRecord] (createArrowRecord)
//	equality deletes   -> Converter[string]           (_olake_id only)
//	positional deletes -> Converter[PositionalDelete]  (file_path + pos)
//
// Because conversion happens per chunk (not per batch), only chunkRows worth of
// arrow data is materialized at a time — that is what bounds peak memory.
type Converter[T any] func(rows []T) (arrow.Record, error)

// RollerConfig tunes rolling behaviour. Zero values fall back to package
// defaults (see constants.go).
type RollerConfig struct {
	// MaxFileBytes is the encoded-size threshold that triggers a roll. The check
	// runs after each chunk, so a file can overshoot by at most one chunk.
	MaxFileBytes int64
	// SizeCheckInterval is how many rows are converted+encoded between size checks.
	// Smaller = tighter memory and file-size precision; larger = fewer, fatter
	// columnar writes.
	SizeCheckInterval int
}

func (c RollerConfig) withDefaults() RollerConfig {
	if c.MaxFileBytes <= 0 {
		c.MaxFileBytes = DefaultMaxFileBytes
	}
	if c.SizeCheckInterval <= 0 {
		c.SizeCheckInterval = DefaultSizeCheckInterval
	}
	return c
}

// Roller owns all the orchestration shared by every parquet backend: it chunks
// incoming rows, encodes each chunk, checks the encoded size, and rolls to a new
// file (flush current + stage via Sink + open next) once the threshold
// is crossed. It is generic over the row type so Iceberg can run several rollers
// (data + delete files), each with its own Converter, over the same machinery.
//
// A Roller is single-writer (one open file at a time); partition fan-out is the
// caller's job — hold one Roller per partition key (see package writers).
type Roller[T any] struct {
	s          sink.StagedSink
	newEncoder encoder.NewEncoder
	convert    Converter[T]
	cfg        RollerConfig

	// state for the currently open file (nil when none is open)
	enc      encoder.Encoder
	handle   sink.FileHandle
	rowCount int64
}

func NewRoller[T any](s sink.StagedSink, enc encoder.NewEncoder, convert Converter[T], cfg RollerConfig) *Roller[T] {
	return &Roller[T]{
		s:          s,
		newEncoder: enc,
		convert:    convert,
		cfg:        cfg.withDefaults(),
	}
}

// Write encodes rows into the current file, rolling to fresh files as the size
// threshold is crossed. Files are opened lazily, so an empty rows slice (or a
// Roller that never receives data) produces no files.
func (r *Roller[T]) Write(ctx context.Context, rows []T) error {
	for start := 0; start < len(rows); start += r.cfg.SizeCheckInterval {
		end := min(start+r.cfg.SizeCheckInterval, len(rows))

		if err := r.ensureOpen(ctx); err != nil {
			return err
		}

		record, err := r.convert(rows[start:end])
		if err != nil {
			return fmt.Errorf("failed to convert rows[%d:%d]: %w", start, end, err)
		}

		err = r.enc.Write(ctx, record)
		record.Release()
		if err != nil {
			return fmt.Errorf("failed to encode rows[%d:%d]: %w", start, end, err)
		}
		r.rowCount += int64(end - start)

		// Post-write check: overshoot is bounded by one chunk.
		if r.enc.Size() >= r.cfg.MaxFileBytes {
			if err := r.finalize(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close finalizes any open file. Call exactly once when no more rows will be
// written; the trailing file may be smaller than the size threshold.
func (r *Roller[T]) Close(ctx context.Context) error {
	return r.finalize(ctx)
}

// ensureOpen lazily opens a new sink file + encoder if none is open.
func (r *Roller[T]) ensureOpen(ctx context.Context) error {
	if r.enc != nil {
		return nil
	}
	w, handle, err := r.s.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open sink file: %w", err)
	}
	enc, err := r.newEncoder(w)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}
	r.enc, r.handle, r.rowCount = enc, handle, 0
	return nil
}

// finalize flushes the current file (footer) and stages it with the Sink, then
// clears state so the next Write opens a fresh file.
func (r *Roller[T]) finalize(ctx context.Context) error {
	if r.enc == nil {
		return nil
	}
	if err := r.enc.Close(); err != nil {
		return fmt.Errorf("failed to flush encoder: %w", err)
	}
	if err := r.s.Stage(ctx, r.handle, r.rowCount); err != nil {
		return fmt.Errorf("failed to stage file: %w", err)
	}
	r.enc, r.handle, r.rowCount = nil, nil, 0
	return nil
}
