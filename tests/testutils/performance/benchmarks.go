package performance

import (
	"fmt"
	"os"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
)

const (
	// BenchmarkThreshold is the share of the recorded average RPS a run must reach to pass.
	BenchmarkThreshold = 0.9
	maxRPSHistorySize  = 5
)

// SyncSpeed is the shape of the stats.json a sync writes; its Speed reads "<rps> rps".
type SyncSpeed struct {
	Speed string `json:"Speed"`
}

// history stores the RPS values and the last updated time for a given mode.
type history struct {
	RPS       []float64 `json:"rps"`
	UpdatedAt time.Time `json:"updated_at"`
}

// benchmarkStore stores the benchmark RPS history for backfill and CDC modes.
type benchmarkStore struct {
	Backfill history `json:"backfill"`
	CDC      history `json:"cdc"`
	FilePath string  `json:"-"`
}

// initializes the benchmark store with the given path and loads the stored benchmarks data from the file.
func loadBenchmarks(path string) (*benchmarkStore, error) {
	store := &benchmarkStore{
		Backfill: history{
			RPS:       make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now().UTC(),
		},
		CDC: history{
			RPS:       make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now().UTC(),
		},
		FilePath: path,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

// load loads the stored benchmarks data from the file.
func (s *benchmarkStore) load() error {
	if err := testutils.UnmarshalFile(s.FilePath, s, false); err != nil {
		if _, statErr := os.Stat(s.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %s", s.FilePath, err)
	}

	return nil
}

// record records a new benchmark RPS value for the given driver and mode, and persists it to the file.
func (s *benchmarkStore) record(
	isBackfill bool,
	rps float64,
) error {
	rpsValues := testutils.Ternary(
		isBackfill,
		s.Backfill.RPS,
		s.CDC.RPS,
	).([]float64)

	rpsValues = append(rpsValues, rps)

	// Truncate history to maintain a rolling window of the last maxRPSHistorySize values.
	if len(rpsValues) > maxRPSHistorySize {
		rpsValues = rpsValues[1:]
	}

	if isBackfill {
		s.Backfill.RPS = rpsValues
		s.Backfill.UpdatedAt = time.Now().UTC()
	} else {
		s.CDC.RPS = rpsValues
		s.CDC.UpdatedAt = time.Now().UTC()
	}

	return testutils.FileLoggerWithPath(s, s.FilePath)
}

// stats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (s *benchmarkStore) stats(
	isBackfill bool,
) (averageRPS float64, observations int) {
	rpsValues := testutils.Ternary(
		isBackfill,
		s.Backfill.RPS,
		s.CDC.RPS,
	).([]float64)

	if len(rpsValues) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return testutils.Average(rpsValues), len(rpsValues)
}
