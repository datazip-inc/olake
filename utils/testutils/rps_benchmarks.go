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

type RPSBenchmarks struct {
	Backfill rpsHistory `json:"backfill"`
	CDC      rpsHistory `json:"cdc"`
	FilePath string     `json:"-"`
}

func LoadRPSBenchmarks(path string) (*RPSBenchmarks, error) {
	benchmarks := &RPSBenchmarks{
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
	if err := benchmarks.loadFromFile(); err != nil {
		return nil, err
	}
	return benchmarks, nil
}

func (benchmarks *RPSBenchmarks) loadFromFile() error {
	if err := utils.UnmarshalFile(benchmarks.FilePath, benchmarks, false); err != nil {
		if _, statErr := os.Stat(benchmarks.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %w", benchmarks.FilePath, err)
	}

	return nil
}

// writeRPSHistory records a new RPS value for the given driver and mode, and persists it to the file.
func (benchmarks *RPSBenchmarks) writeRPSHistory(
	isBackfill bool,
	rps float64,
) error {
	values := utils.Ternary(
		isBackfill,
		benchmarks.Backfill.Values,
		benchmarks.CDC.Values,
	).([]float64)

	values = append(values, rps)

	// Truncate history to maintain a rolling window of the last maxRPSHistorySize values.
	if len(values) > maxRPSHistorySize {
		values = values[len(values)-maxRPSHistorySize:]
	}

	if isBackfill {
		benchmarks.Backfill.Values = values
		benchmarks.Backfill.UpdatedAt = time.Now()
	} else {
		benchmarks.CDC.Values = values
		benchmarks.CDC.UpdatedAt = time.Now()
	}

	return logger.FileLoggerWithPath(benchmarks, benchmarks.FilePath)
}

// pastRPSStats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (benchmarks *RPSBenchmarks) pastRPSStats(
	isBackfill bool,
) (averageRPS float64, pastRPSCount int) {
	pastRPSValues := utils.Ternary(
		isBackfill,
		benchmarks.Backfill.Values,
		benchmarks.CDC.Values,
	).([]float64)

	if len(pastRPSValues) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return utils.Average(pastRPSValues), len(pastRPSValues)
}
