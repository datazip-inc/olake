package iceberg

import (
	"context"
	"fmt"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
)

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
	partitionInfo map[string]string // map of field names to partition transform
	// Local buffer for this writer thread
	localBuffer   []string
	bufferSize    int64
	maxBufferSize int64
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
	i.partitionInfo = make(map[string]string)
	i.localBuffer = make([]string, 0, 1000)
	i.bufferSize = 0
	i.maxBufferSize = determineMaxBatchSize()

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

func (i *Iceberg) Write(_ context.Context, record types.RawRecord) error {
	// Convert record to Debezium format
	debeziumRecord, err := record.ToDebeziumFormat(i.config.IcebergDatabase, i.stream.Name(), i.stream.NormalizationEnabled())
	if err != nil {
		return fmt.Errorf("failed to convert record: %v", err)
	}

	// Add record to local buffer
	if debeziumRecord != "" {
		i.localBuffer = append(i.localBuffer, debeziumRecord)
		i.bufferSize += int64(len(debeziumRecord))

		// Check if buffer threshold is reached
		if i.bufferSize >= i.maxBufferSize {
			err := i.flushBuffer()
			if err != nil {
				return fmt.Errorf("failed to flush buffer: %v", err)
			}
		}
	}

	i.records.Add(1)
	return nil
}

func (i *Iceberg) flushBuffer() error {
	if len(i.localBuffer) == 0 {
		return nil
	}

	recordCount := len(i.localBuffer)

	// Send records directly to Java writer
	err := i.sendRecords(i.localBuffer)
	if err != nil {
		return err
	}

	// Reset buffer
	i.localBuffer = make([]string, 0, 1000)
	i.bufferSize = 0

	logger.Infof("Flushed buffer with %d records to Iceberg server on port %d", recordCount, i.port)
	return nil
}

func (i *Iceberg) sendRecords(records []string) error {
	if len(records) == 0 {
		return nil
	}

	// Filter out empty records
	validRecords := make([]string, 0, len(records))
	for _, record := range records {
		if record != "" {
			validRecords = append(validRecords, record)
		}
	}

	if len(validRecords) == 0 {
		return nil
	}

	// Create request
	req := &proto.RecordIngestRequest{
		Messages: validRecords,
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	res, err := i.client.SendRecords(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send records: %v", err)
	}

	logger.Infof("Sent %d records to Iceberg server on port %d, response: %s", len(validRecords), i.port, res.GetResult())
	return nil
}

func (i *Iceberg) Close() error {
	// Flush any remaining records in buffer
	err := i.flushBuffer()
	if err != nil {
		logger.Errorf("Error flushing buffer on close: %v", err)
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
	i.partitionInfo = make(map[string]string)

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

func (i *Iceberg) EvolveSchema(_ bool, _ bool, _ map[string]*types.Property, _ types.Record, _ time.Time) error {
	// Schema evolution is handled by Iceberg
	return nil
}

func init() {
	protocol.RegisteredWriters[types.Iceberg] = func() protocol.Writer {
		return new(Iceberg)
	}
}
