package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	_ "github.com/datazip-inc/olake/destination/iceberg"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/viper"
)

func main() {
	viper.Set(constants.ConfigFolder, ".")
	logger.Init()

	if len(os.Args) < 2 {
		logger.Fatalf("Usage: go run main/main.go <command> [args...]")
	}

	if os.Args[1] == "read-index" {
		if len(os.Args) < 3 {
			logger.Fatalf("Usage: go run main/main.go read-index <suffix>")
		}
		suffix, err := strconv.Atoi(os.Args[2])
		if err != nil {
			logger.Fatalf("Invalid table suffix (must be integer): %s", err)
		}
		if err := destination.ReadIndex(suffix); err != nil {
			logger.Fatalf("ReadIndex failed: %s", err)
		}
		return
	}

	if os.Args[1] == "drop" {
		if len(os.Args) < 3 {
			logger.Fatalf("Usage: go run main/main.go drop <suffix>")
		}
		suffix, err := strconv.Atoi(os.Args[2])
		if err != nil {
			logger.Fatalf("Invalid table suffix (must be integer): %s", err)
		}
		destination.DropTable(suffix, true)
		return
	}

	if len(os.Args) < 4 {
		logger.Fatalf("Usage: go run main.go <table_suffix> <c|u> <num_records>\n\n  c = insert (create) records\n  u = update that many records\n\nExample: go run main.go 4 c 100   # insert 100 records\nExample: go run main.go 4 u 50    # update 50 records")
	}

	suffix, err := strconv.Atoi(os.Args[2])
	if err != nil {
		logger.Fatalf("Invalid table suffix (must be integer): %s", err)
	}

	operation := strings.ToLower(strings.TrimSpace(os.Args[3]))
	if operation != "c" && operation != "u" {
		logger.Fatalf("Operation must be 'c' (insert) or 'u' (update), got %q", os.Args[3])
	}

	numRecords, err := strconv.Atoi(os.Args[4])
	if err != nil || numRecords <= 0 {
		logger.Fatalf("num_records must be a positive integer: %s", os.Args[4])
	}

	logger.Infof("Table suffix: %d, operation: %s, num records: %d", suffix, operation, numRecords)

	if err := destination.WriteData(os.Args[1], suffix, operation, numRecords); err != nil {
		logger.Fatalf("WriteData failed: %s", err)
	}

	logger.Info("Done.")
}
