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

type BenchmarkStore struct {
	Backfill history `json:"backfill"`
	CDC      history `json:"cdc"`
	FilePath string  `json:"-"`
}

func LoadBenchmarks(path string) (*BenchmarkStore, error) {
	store := &BenchmarkStore{
		Backfill: history{
			RPS:       make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now(),
		},
		CDC: history{
			RPS:       make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now(),
		},
		FilePath: path,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *BenchmarkStore) load() error {
	if err := utils.UnmarshalFile(store.FilePath, store, false); err != nil {
		if _, statErr := os.Stat(store.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %w", store.FilePath, err)
	}

	return nil
}

// record records a new value for the given driver and mode, and persists it to the file.
func (store *BenchmarkStore) record(
	isBackfill bool,
	rps float64,
) error {
	rpsValues := utils.Ternary(
		isBackfill,
		store.Backfill.RPS,
		store.CDC.RPS,
	).([]float64)

	rpsValues = append(rpsValues, rps)

	// Truncate history to maintain a rolling window of the last maxRPSHistorySize values.
	if len(rpsValues) > maxRPSHistorySize {
		rpsValues = rpsValues[len(rpsValues)-maxRPSHistorySize:]
	}

	if isBackfill {
		store.Backfill.RPS = rpsValues
		store.Backfill.UpdatedAt = time.Now()
	} else {
		store.CDC.RPS = rpsValues
		store.CDC.UpdatedAt = time.Now()
	}

	return logger.FileLoggerWithPath(store, store.FilePath)
}

// stats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (store *BenchmarkStore) stats(
	isBackfill bool,
) (averageRPS float64, count int) {
	rpsValues := utils.Ternary(
		isBackfill,
		store.Backfill.RPS,
		store.CDC.RPS,
	).([]float64)

	if len(rpsValues) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return utils.Average(rpsValues), len(rpsValues)
}
