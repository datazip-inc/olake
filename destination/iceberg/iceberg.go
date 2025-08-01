package iceberg

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/datazip-inc/olake/constants"
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
	cmd           *exec.Cmd
	client        proto.RecordIngestServiceClient
	conn          *grpc.ClientConn
	port          int
	backfill      bool
	configHash    string
	partitionInfo []PartitionInfo // ordered slice to preserve partition column order
	localBuffer   *LocalBuffer    // Buffer for this thread
	threadID      string
}

// PartitionInfo represents a Iceberg partition column with its transform, preserving order
type PartitionInfo struct {
	Field     string
	Transform string
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
	i.partitionInfo = make([]PartitionInfo, 0)
	i.threadID = getGoroutineID()

	// Initialize the local buffer
	i.localBuffer = &LocalBuffer{
		records: make([]string, 0, 100),
		size:    0,
	}

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

func (i *Iceberg) Write(ctx context.Context, records []types.RawRecord) error {
	// Convert record to Debezium format with thread ID
	// We are adding the thread ID to process the records from multiple threads in parallel and separately so that we can commit when each thread finishes.
	var iceRecords []types.IceRecord
	for _, record := range records {
		debeziumRecord, err := record.ToDebeziumFormat(i.config.IcebergDatabase, i.stream.Name(), i.stream.NormalizationEnabled(), i.threadID)
		if err != nil {
			return fmt.Errorf("failed to convert record: %v", err)
		}
		iceRecords = append(iceRecords, *debeziumRecord)
	}
	finalPayload := types.IcebergWriterPayload{
		Type: "records",
		Metadata: types.Metadata{
			DestTableName: i.stream.Name(),
			ThreadID:      i.threadID,
			PrimaryKey:    constants.OlakeID,
		},
		Records: iceRecords,
	}

	err := i.sendRecords(ctx, finalPayload)
	if err != nil {
		return fmt.Errorf("thread id %s: failed to flush buffer: %s", i.threadID, err)
	}
	logger.Infof("thread id %s: Batch flushed to Iceberg server for stream %s", i.threadID, i.stream.Name())

	return nil
}

func (i *Iceberg) Close(ctx context.Context, finalFlush bool) error {
	// skip flushing on error
	defer func() {
		err := i.CloseIcebergClient()
		if err != nil {
			logger.Errorf("thread id %s: Error closing Iceberg client: %s", i.threadID, err)
		}
	}()
	if ctx.Err() != nil {
		return nil
	}
	if finalFlush {
		// Send commit request for this thread using a special message format
		ctx, cancel := context.WithTimeout(ctx, 1000*time.Second)
		defer cancel()

		// Create a special commit message
		// commitMessage := fmt.Sprintf(`{"commit": true, "thread_id": "%s"}`, i.threadID)

		req := &proto.IcebergPayload{
			Type: "commit",
			Metadata: &proto.IcebergPayload_Metadata{
				ThreadId: i.threadID,
			},
		}
		res, err := i.client.SendRecords(ctx, req)
		if err != nil {
			logger.Errorf("thread id %s: Error sending commit message on close: %s", i.threadID, err)
			return fmt.Errorf("thread id %s: failed to send commit message: %s", i.threadID, err)
		}

		logger.Infof("thread id %s: Sent commit message: %s", i.threadID, res.GetResult())
	}

	return nil
}

func (i *Iceberg) Check(ctx context.Context) error {
	// Save the current stream reference
	originalStream := i.stream
	originalPartitionInfo := i.partitionInfo
	i.threadID = getGoroutineID()

	// Temporarily set stream to nil and clear partition fields to force a new server for the check
	i.stream = nil
	i.partitionInfo = make([]PartitionInfo, 0)

	// Create a temporary setup for checking
	err := i.SetupIcebergClient(false)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg: %s", err)
	}

	defer func() {
		i.Close(ctx, true)
		// Restore original stream and partition info
		i.stream = originalStream
		i.partitionInfo = originalPartitionInfo
	}()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// // Try to send a test message
	// req := &proto.RecordIngestRequest{
	// 	Messages: []string{getTestDebeziumRecord(i.threadID)},
	// }

	// // Call the remote procedure
	// res, err := i.client.SendRecords(ctx, req)
	// if err != nil {
	// 	return fmt.Errorf("error sending record to Iceberg RPC Server: %s", err)
	// }
	// Print the response from the server
	// logger.Infof("thread id %s: Server Response: %s", i.threadID, res.GetResult())

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

func (i *Iceberg) DropStreams(_ context.Context, _ []string) error {
	logger.Info("iceberg destination not support clear destination, skipping clear operation")

	// logger.Infof("Clearing Iceberg destination for %d selected streams: %v", len(selectedStreams), selectedStreams)

	// TODO: Implement Iceberg table clearing logic
	// 1. Connect to the Iceberg catalog
	// 2. Use Iceberg's delete API or drop/recreate the table
	// 3. Handle any Iceberg-specific cleanup

	// logger.Info("Successfully cleared Iceberg destination for selected streams")
	return nil
}

func init() {
	destination.RegisteredWriters[types.Iceberg] = func() destination.Writer {
		return new(Iceberg)
	}
}
