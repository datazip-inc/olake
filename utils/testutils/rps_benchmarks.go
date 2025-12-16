package testutils

import (
	"fmt"
	"os"
	"time"

	"github.com/datazip-inc/olake/constants"
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

type driverRPSHistory struct {
	Backfill rpsHistory `json:"backfill"`
	CDC      rpsHistory `json:"cdc"`
}

type RPSBenchmarks struct {
	Drivers  map[constants.DriverType]driverRPSHistory `json:"drivers"`
	FilePath string                                    `json:"file_path"`
}

func LoadRPSBenchmarks(path string) (*RPSBenchmarks, error) {
	benchmarks := &RPSBenchmarks{
		Drivers:  make(map[constants.DriverType]driverRPSHistory),
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
			// if the file does not exist, we don't return an error.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %w", benchmarks.FilePath, err)
	}

	return nil
}

// Writes the new history to the file
func (benchmarks *RPSBenchmarks) writeRPSHistory(
	driver constants.DriverType,
	isBackfill bool,
	rps float64,
) error {
	driverHistory, exists := benchmarks.Drivers[driver]
	if !exists {
		// initialize the driver history for all the modes for the driver.
		driverHistory = driverRPSHistory{
			Backfill: rpsHistory{
				Values:    make([]float64, 0, maxRPSHistorySize),
				UpdatedAt: time.Now(),
			},
			CDC: rpsHistory{
				Values:    make([]float64, 0, maxRPSHistorySize),
				UpdatedAt: time.Now(),
			},
		}
	}

	values := utils.Ternary(
		isBackfill,
		driverHistory.Backfill.Values,
		driverHistory.CDC.Values,
	).([]float64)

	values = append(values, rps)

	// keep the history size within the limit.
	if len(values) > maxRPSHistorySize {
		values = values[len(values)-maxRPSHistorySize:]
	}

	if isBackfill {
		driverHistory.Backfill.Values = values
		driverHistory.Backfill.UpdatedAt = time.Now()
	} else {
		driverHistory.CDC.Values = values
		driverHistory.CDC.UpdatedAt = time.Now()
	}
	benchmarks.Drivers[driver] = driverHistory

	return logger.FileLoggerWithPath(benchmarks, benchmarks.FilePath)
}

// Returns the average RPS and the number of past RPS values for a given driver and mode
// past RPS values cannot be greater than maxRPSHistorySize and less than 0
// if the driver is not found in the benchmark history file, returns (0, 0, false)
func (benchmarks *RPSBenchmarks) pastRPSStats(
	driver constants.DriverType,
	isBackfill bool,
) (averageRPS float64, pastRPSCount int, ok bool) {
	driverHistory, exists := benchmarks.Drivers[driver]
	if !exists {
		return 0, 0, false
	}

	pastRPSValues := utils.Ternary(
		isBackfill,
		driverHistory.Backfill.Values,
		driverHistory.CDC.Values,
	).([]float64)

	if len(pastRPSValues) == 0 {
		// we initialize the driver history for all the modes for the driver.
		// this will happen when the driver is found, but the benchmark for this particular mode have not been recorded yet.
		return 0, 0, true
	}

	return utils.Average(pastRPSValues), len(pastRPSValues), true
}
