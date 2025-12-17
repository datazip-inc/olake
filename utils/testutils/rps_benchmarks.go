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

type rpsHistory struct {
	Values    []float64 `json:"values"`
	UpdatedAt time.Time `json:"updated_at"`
}

type RPSBenchmarkStore struct {
	Backfill rpsHistory `json:"backfill"`
	CDC      rpsHistory `json:"cdc"`
	FilePath string     `json:"-"`
}

func LoadRPSBenchmarks(path string) (*RPSBenchmarkStore, error) {
	store := &RPSBenchmarkStore{
		Backfill: rpsHistory{
			Values:    make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now(),
		},
		CDC: rpsHistory{
			Values:    make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now(),
		},
		FilePath: path,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *RPSBenchmarkStore) load() error {
	if err := utils.UnmarshalFile(store.FilePath, store, false); err != nil {
		if _, statErr := os.Stat(store.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %w", store.FilePath, err)
	}

	return nil
}

// recordRPS records a new RPS value for the given driver and mode, and persists it to the file.
func (store *RPSBenchmarkStore) recordRPS(
	isBackfill bool,
	rps float64,
) error {
	values := utils.Ternary(
		isBackfill,
		store.Backfill.Values,
		store.CDC.Values,
	).([]float64)

	values = append(values, rps)

	// Truncate history to maintain a rolling window of the last maxRPSHistorySize values.
	if len(values) > maxRPSHistorySize {
		values = values[len(values)-maxRPSHistorySize:]
	}

	if isBackfill {
		store.Backfill.Values = values
		store.Backfill.UpdatedAt = time.Now()
	} else {
		store.CDC.Values = values
		store.CDC.UpdatedAt = time.Now()
	}

	return logger.FileLoggerWithPath(store, store.FilePath)
}

// stats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (store *RPSBenchmarkStore) stats(
	isBackfill bool,
) (averageRPS float64, count int) {
	values := utils.Ternary(
		isBackfill,
		store.Backfill.Values,
		store.CDC.Values,
	).([]float64)

	if len(values) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return utils.Average(values), len(values)
}
