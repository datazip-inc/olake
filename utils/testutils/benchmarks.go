package testutils

import (
	"fmt"
	"os"
	"time"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	maxRPSHistorySize = 5
)

type history struct {
	RPS       []float64 `json:"rps"`
	UpdatedAt time.Time `json:"updated_at"`
}

type benchmarkStore struct {
	Backfill history `json:"backfill"`
	CDC      history `json:"cdc"`
	FilePath string  `json:"-"`
}

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

func (s *benchmarkStore) load() error {
	if err := utils.UnmarshalFile(s.FilePath, s, false); err != nil {
		if _, statErr := os.Stat(s.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %w", s.FilePath, err)
	}

	return nil
}

// record records a new value for the given driver and mode, and persists it to the file.
func (s *benchmarkStore) record(
	isBackfill bool,
	rps float64,
) error {
	rpsValues := utils.Ternary(
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

	return logger.FileLoggerWithPath(s, s.FilePath)
}

// stats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (s *benchmarkStore) stats(
	isBackfill bool,
) (averageRPS float64, observations int) {
	rpsValues := utils.Ternary(
		isBackfill,
		s.Backfill.RPS,
		s.CDC.RPS,
	).([]float64)

	if len(rpsValues) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return utils.Average(rpsValues), len(rpsValues)
}
