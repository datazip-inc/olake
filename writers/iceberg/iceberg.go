package iceberg

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
)

// PartitionField holds information about a partition field
type PartitionField struct {
	Transform    string // The transform to apply (e.g., "hour", "day", "month")
	DefaultValue string // Default value to use if field is missing
}

type Iceberg struct {
	options       *protocol.Options
	config        *Config
	stream        protocol.Stream
	records       atomic.Int64
	cmd           *exec.Cmd
	client        proto.RecordIngestServiceClient
	conn          *grpc.ClientConn
	port          int
	backfill      bool
	configHash    string
	partitionInfo map[string]*PartitionField // map of field names to partition information
}

func (i *Iceberg) GetConfigRef() protocol.Config {
	i.config = &Config{}
	return i.config
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) Setup(stream protocol.Stream, options *protocol.Options) error {
	i.options = options
	i.stream = stream
	i.backfill = options.Backfill
	i.partitionInfo = make(map[string]*PartitionField)

	// Parse partition regex from stream metadata
	partitionRegex := i.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := i.parsePartitionRegex(partitionRegex)
		if err != nil {
			return fmt.Errorf("failed to parse partition regex: %v", err)
		}
	}

	return i.SetupIcebergClient(!options.Backfill)
}

// parsePartitionRegex parses the partition regex and populates the partitionInfo map
func (i *Iceberg) parsePartitionRegex(pattern string) error {
	// path pattern example: /{col_name, default_value_if_not_present, partition_transform}/{col_name, default_value_if_not_present, partition_transform}
	// This strictly identifies column name, default value, and partition transform entries
	patternRegex := regexp.MustCompile(`\{([^,]+),\s*([^,]*),\s*([^}]+)\}`)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)
	for _, match := range matches {
		if len(match) < 4 {
			continue // We need at least 4 matches: full match, column name, default value, transform
		}

		colName := strings.TrimSpace(strings.Trim(match[1], `'"`))
		defaultValue := strings.TrimSpace(strings.Trim(match[2], `'"`))
		transform := strings.TrimSpace(strings.Trim(match[3], `'"`))

		// Special handling for now() function
		if colName == "now()" {
			// Create a special field for timestamps in Iceberg
			// We'll use __source_ts_ms which is present in all records
			field := "__ts_ms"

			i.partitionInfo[field] = &PartitionField{
				Transform:    transform,
				DefaultValue: "", // No default value needed for timestamp field
			}

			logger.Infof("Added timestamp partition field: %s with transform: %s (from now() function)", field, transform)
			continue
		}

		// Store both transform and default value for this field
		i.partitionInfo[colName] = &PartitionField{
			Transform:    transform,
			DefaultValue: defaultValue,
		}
	}

	return nil
}

func (i *Iceberg) Write(_ context.Context, record types.RawRecord) error {
	// Add default values for missing partition fields and validate required fields
	if len(i.partitionInfo) > 0 {

		for field, info := range i.partitionInfo {
			// Skip validation for internal fields like __source_ts_ms which are automatically added
			if strings.HasPrefix(field, "__") {
				continue
			}

			_, exists := record.Data[field]
			if !exists {
				// If field doesn't exist, check if we have a default value
				if info.DefaultValue != "" {
					// Add the default value to the record
					record.Data[field] = info.DefaultValue
				} else {
					// No default value available, must error
					return fmt.Errorf("required partition field '%s' not found in record and no default value provided", field)
				}
			}
		}
	}

	// Convert record to Debezium format
	debeziumRecord, err := record.ToDebeziumFormat(i.config.IcebergDatabase, i.stream.Name(), i.config.Normalization)
	if err != nil {
		return fmt.Errorf("failed to convert record: %v", err)
	}

	// Add the record to the batch
	flushed, err := addToBatch(i.configHash, debeziumRecord, i.client)
	if err != nil {
		return fmt.Errorf("failed to add record to batch: %v", err)
	}

	// If the batch was flushed, log the event
	if flushed {
		logger.Infof("Batch flushed to Iceberg server for stream %s", i.stream.Name())
	}

	i.records.Add(1)
	return nil
}

func (i *Iceberg) Close() error {
	err := flushBatch(i.configHash, i.client)
	if err != nil {
		logger.Infof("Error flushing batch on close: %v", err)
		return err
	}

	err = i.CloseIcebergClient()
	if err != nil {
		return fmt.Errorf("error closing Iceberg client: %v", err)
	}

	return nil
}

func (i *Iceberg) Check() error {
	// Save the current stream reference
	originalStream := i.stream
	originalPartitionInfo := i.partitionInfo

	// Temporarily set stream to nil and clear partition fields to force a new server for the check
	i.stream = nil
	i.partitionInfo = make(map[string]*PartitionField)

	// Create a temporary setup for checking
	err := i.SetupIcebergClient(false)
	if err != nil {
		// Restore original stream and partition info before returning
		i.stream = originalStream
		i.partitionInfo = originalPartitionInfo
		return fmt.Errorf("failed to setup iceberg: %v", err)
	}

	defer func() {
		i.Close()
		// Restore original stream and partition info
		i.stream = originalStream
		i.partitionInfo = originalPartitionInfo
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to send a test message
	req := &proto.RecordIngestRequest{
		Messages: []string{getTestDebeziumRecord()},
	}

	// Call the remote procedure
	res, err := i.client.SendRecords(ctx, req)
	if err != nil {
		return fmt.Errorf("error sending record to Iceberg RPC Server: %v", err)
	}
	// Print the response from the server
	logger.Infof("Server Response: %s", res.GetResult())

	return nil
}

func (i *Iceberg) ReInitiationOnTypeChange() bool {
	return true
}

func (i *Iceberg) ReInitiationOnNewColumns() bool {
	return true
}

func (i *Iceberg) Type() string {
	return "iceberg"
}

func (i *Iceberg) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (i *Iceberg) Normalization() bool {
	return i.config.Normalization
}

func (i *Iceberg) EvolveSchema(_ bool, _ bool, _ map[string]*types.Property, _ types.Record) error {
	// Schema evolution is handled by Iceberg
	return nil
}

func init() {
	protocol.RegisteredWriters[types.Iceberg] = func() protocol.Writer {
		return new(Iceberg)
	}
}
