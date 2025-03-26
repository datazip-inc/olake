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
	options    *protocol.Options
	config     *Config
	stream     protocol.Stream
	records    atomic.Int64
	cmd        *exec.Cmd
	client     proto.RecordIngestServiceClient
	conn       *grpc.ClientConn
	port       int
	backfill   bool
	configHash string
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
	return i.SetupIcebergClient(options.Backfill)
}

func (i *Iceberg) Write(_ context.Context, record types.RawRecord) error {
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

	// Temporarily set stream to nil to force a new server for the check
	i.stream = nil

	// Create a temporary setup for checking
	err := i.SetupIcebergClient(false)
	defer i.Close()
	if err != nil {
		// Restore original stream before returning
		i.stream = originalStream
		return fmt.Errorf("failed to setup iceberg: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to send a test message
	req := &proto.RecordIngestRequest{
		Messages: []string{getTestDebeziumRecord()},
	}

	// Call the remote procedure
	res, err := i.client.SendRecords(ctx, req)
	if err != nil {
		i.stream = originalStream
		return fmt.Errorf("error sending record to Iceberg RPC Server: %v", err)
	}
	// Print the response from the server
	logger.Infof("Server Response: %s", res.GetResult())

	// Restore original stream
	i.stream = originalStream

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
