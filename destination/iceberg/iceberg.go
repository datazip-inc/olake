package iceberg

import (
	"context"
	"fmt"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"google.golang.org/grpc"
)

type Iceberg struct {
	options       *destination.Options
	config        *Config
	stream        types.StreamInterface
	records       atomic.Int64
	cmd           *exec.Cmd
	client        proto.RecordIngestServiceClient
	conn          *grpc.ClientConn
	port          int
	backfill      bool
	configHash    string
	partitionInfo map[string]string // map of field names to partition transform
}

func (i *Iceberg) GetConfigRef() destination.Config {
	i.config = &Config{}
	return i.config
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) Setup(stream types.StreamInterface, options *destination.Options) error {
	i.options = options
	i.stream = stream
	i.backfill = options.Backfill
	i.partitionInfo = make(map[string]string)

	// Parse partition regex from stream metadata
	partitionRegex := i.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := i.parsePartitionRegex(partitionRegex)
		if err != nil {
			return fmt.Errorf("failed to parse partition regex: %s", err)
		}
	}

	if i.stream.Self().StreamMetadata.AppendMode {
		// marking upsert mode to false
		return i.SetupIcebergClient(false)
	}
	return i.SetupIcebergClient(!options.Backfill)
}

func (i *Iceberg) Write(_ context.Context, record types.RawRecord) error {
	// Convert record to Debezium format
	debeziumRecord, err := record.ToDebeziumFormat(i.config.IcebergDatabase, i.stream.Name(), i.stream.NormalizationEnabled())

	if err != nil {
		return fmt.Errorf("failed to convert record: %v", err)
	}
	// Add the record to the batch
	flushed, err := addToBatch(i.configHash, debeziumRecord, i.client)
	if err != nil {
		return fmt.Errorf("failed to add record to batch: %s", err)
	}

	// If the batch was flushed, log the event
	if flushed {
		logger.Infof("Batch flushed to Iceberg server for stream %s", i.stream.Name())
	}

	i.records.Add(1)
	return nil
}

func (i *Iceberg) Close(_ context.Context) error {
	err := flushBatch(i.configHash, i.client)
	if err != nil {
		logger.Errorf("Error flushing batch on close: %s", err)
		return err
	}

	err = i.CloseIcebergClient()
	if err != nil {
		return fmt.Errorf("error closing Iceberg client: %s", err)
	}

	return nil
}

func (i *Iceberg) Check(ctx context.Context) error {
	// Save the current stream reference
	originalStream := i.stream
	originalPartitionInfo := i.partitionInfo

	// Temporarily set stream to nil and clear partition fields to force a new server for the check
	i.stream = nil
	i.partitionInfo = make(map[string]string)

	// Create a temporary setup for checking
	err := i.SetupIcebergClient(false)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg: %s", err)
	}

	defer func() {
		i.Close(ctx)
		// Restore original stream and partition info
		i.stream = originalStream
		i.partitionInfo = originalPartitionInfo
	}()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Try to send a test message
	req := &proto.RecordIngestRequest{
		Messages: []string{getTestDebeziumRecord()},
	}

	// Call the remote procedure
	res, err := i.client.SendRecords(ctx, req)
	if err != nil {
		return fmt.Errorf("error sending record to Iceberg RPC Server: %s", err)
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
	return string(types.Iceberg)
}

func (i *Iceberg) Flattener() destination.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (i *Iceberg) EvolveSchema(_ bool, _ bool, _ map[string]*types.Property, _ types.Record, _ time.Time) error {
	// Schema evolution is handled by Iceberg
	return nil
}

func init() {
	destination.RegisteredWriters[types.Iceberg] = func() destination.Writer {
		return new(Iceberg)
	}
}
