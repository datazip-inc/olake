package iceberg

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type serverInstance struct {
	port       int
	cmd        *exec.Cmd
	client     proto.RecordIngestServiceClient
	conn       *grpc.ClientConn
	refCount   int    // Tracks how many clients are using this server instance to manage shared resources. Comes handy when we need to close the server after all clients are done.
	configHash string // Hash representing the server config
	upsert     bool
}

type Iceberg struct {
	options       *destination.Options
	config        *Config
	stream        types.StreamInterface
	conn          *grpc.ClientConn
	partitionInfo []PartitionInfo // ordered slice to preserve partition column order
	threadID      string
	server        *serverInstance // java server instance
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

func (i *Iceberg) Setup(ctx context.Context, stream types.StreamInterface, createOrLoadSchema bool, options *destination.Options) (any, error) {
	i.options = options
	i.stream = stream
	i.partitionInfo = make([]PartitionInfo, 0)
	i.threadID = getGoroutineID()

	// Parse partition regex from stream metadata
	partitionRegex := i.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := i.parsePartitionRegex(partitionRegex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition regex: %s", err)
		}
	}

	upsertMode := true
	if i.stream.Self().StreamMetadata.AppendMode {
		// marking upsert mode to false
		upsertMode = false
	}
	upsertMode = upsertMode || !options.Backfill
	err := i.SetupIcebergClient(upsertMode)
	if err != nil {
		return nil, fmt.Errorf("failed to start java server: %s", err)
	}

	primaryKey := constants.OlakeID
	if createOrLoadSchema {
		requestPayload := proto.IcebergPayload{
			Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
			Metadata: &proto.IcebergPayload_Metadata{
				Schema:        stream.Schema().ToIceberg(),
				DestTableName: i.stream.Name(),
				ThreadId:      i.threadID,
				PrimaryKey:    &primaryKey,
			},
		}
		resp, err := i.server.client.SendRecords(ctx, &requestPayload)
		if err != nil {
			return nil, fmt.Errorf("failed to load or create table: %s", err)
		}
		return parseSchema(resp.Result)
	}

	return nil, nil
}

func (i *Iceberg) Write(ctx context.Context, records []types.RawRecord) error {
	var protoRecords []*proto.IcebergPayload_IceRecord
	for _, record := range records {
		protoColumns := make(map[string]*structpb.Value)
		record.Data[constants.OlakeID] = record.OlakeID
		record.Data[constants.CdcTimestamp] = record.CdcTimestamp.Format(time.RFC3339)
		record.Data[constants.OlakeTimestamp] = time.Now().UTC().Format(time.RFC3339)
		record.Data[constants.OpType] = record.OperationType
		for key, value := range record.Data {
			if value == nil {
				continue
			}
			switch v := value.(type) {
			case time.Time:
				protoColumns[key] = structpb.NewStringValue(v.UTC().Format(time.RFC3339))
			default:
				protoColumns[key] = structpb.NewStringValue(fmt.Sprintf("%v", value))
			}

		}

		// release record data to save memory
		record.Data = nil

		if len(protoColumns) > 0 {
			protoRecords = append(protoRecords, &proto.IcebergPayload_IceRecord{
				Fields:     protoColumns,
				RecordType: record.OperationType,
			})
		}
	}

	if len(protoRecords) == 0 {
		logger.Debug("no record found in batch")
		return nil
	}
	req := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			DestTableName: i.stream.Name(),
			ThreadId:      i.threadID,
		},
		Records: protoRecords,
	}

	// Send to gRPC server with timeout
	reqCtx, cancel := context.WithTimeout(ctx, 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := i.server.client.SendRecords(reqCtx, req)
	if err != nil {
		logger.Errorf("failed to send batch: %s", err)
		return err
	}
	logger.Infof("Sent batch to Iceberg server: %d records, response: %s",
		len(records),
		res.GetResult())
	return nil
}

func (i *Iceberg) Close(ctx context.Context) error {
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
	// Send commit request for this thread using a special message format
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Second)
	defer cancel()

	// Create a special commit message
	// commitMessage := fmt.Sprintf(`{"commit": true, "thread_id": "%s"}`, i.threadID)

	req := &proto.IcebergPayload{
		Type: proto.IcebergPayload_COMMIT,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      i.threadID,
			DestTableName: i.stream.Name(),
		},
	}
	res, err := i.server.client.SendRecords(ctx, req)
	if err != nil {
		logger.Errorf("thread id %s: Error sending commit message on close: %s", i.threadID, err)
		return fmt.Errorf("thread id %s: failed to send commit message: %s", i.threadID, err)
	}

	logger.Infof("thread id %s: Sent commit message: %s", i.threadID, res.GetResult())

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
		i.Close(ctx)
		// Restore original stream and partition info
		i.stream = originalStream
		i.partitionInfo = originalPartitionInfo
	}()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// TODO: add back
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

// validate schema change & evolution and removes null records
func (i *Iceberg) ValidateSchema(rawOldSchema any, records []types.RawRecord) (bool, any, error) {
	extractSchemaFromRecords := func(records []types.RawRecord) (map[string]string, error) {
		newSchema := make(map[string]string)

		for _, record := range records {
			for key, value := range record.Data {
				detectedType := typeutils.TypeFromValue(value)

				if detectedType == types.Null {
					// remove element from data if it is null
					delete(record.Data, key)
					continue
				}

				detecteIceType := detectedType.ToIceberg()
				if persistedType, exists := newSchema[key]; exists && !isValidIcebergType(persistedType, detecteIceType) {
					return nil, fmt.Errorf(
						"failed to validate schema (got two different types in batch), expected type: %s, got type: %s",
						persistedType, detecteIceType,
					)
				}
				newSchema[key] = detecteIceType
			}
		}

		return newSchema, nil
	}

	compareSchemas := func(oldSchema, newSchema map[string]string) (bool, error) {
		schemaChange := false

		for fieldName, newType := range newSchema {
			oldType, exists := oldSchema[fieldName]

			if !exists {
				schemaChange = true
				continue
			}

			// TODO: we can break on schema change detection but
			// should we check for type conversion is possible or not according to tree (need a discussion)

			if !isValidIcebergType(oldType, newType) {
				return false, fmt.Errorf(
					"different type detected in schema, old type: %s, new type: %s for field: %s",
					oldType, newType, fieldName,
				)
			}
		}

		return schemaChange, nil
	}

	oldSchema, ok := rawOldSchema.(map[string]string)
	if !ok {
		return false, nil, fmt.Errorf("failed to convert past schema of type[%T] to map[string]string", rawOldSchema)
	}

	newSchema, err := extractSchemaFromRecords(records)
	if err != nil {
		return false, nil, err
	}

	schemaChange, err := compareSchemas(oldSchema, newSchema)
	if err != nil {
		return false, nil, err
	}

	return schemaChange, newSchema, nil
}

func (i *Iceberg) EvolveSchema(ctx context.Context, newSchema any, _ []types.RawRecord, _ time.Time) error {
	newSchemaMap, ok := newSchema.(map[string]string)
	if !ok {
		return fmt.Errorf("failed to convert new schema of type %T to map[string]string", newSchema)
	}
	var schema []*proto.IcebergPayload_SchemaField
	for field, fieldType := range newSchemaMap {
		schema = append(schema, &proto.IcebergPayload_SchemaField{
			Key:     field,
			IceType: fieldType,
		})
	}
	req := proto.IcebergPayload{
		Type: proto.IcebergPayload_EVOLVE_SCHEMA,
		Metadata: &proto.IcebergPayload_Metadata{
			DestTableName: i.stream.Name(),
			Schema:        schema,
		},
	}

	resp, err := i.server.client.SendRecords(ctx, &req)
	if err != nil {
		return fmt.Errorf("failed to send records to evolve schema: %s", err)
	}
	logger.Debug("response received after schema evolution: %s", resp.Result)
	return nil
}

func isValidIcebergType(oldType, newType string) bool {
	// Note: not added decimal precision as we not iceberg decimal type support as of now
	switch oldType {
	case "int":
		return newType == "long" || newType == "int"
	case "float":
		return newType == "float" || newType == "double"
	default:
		return oldType == newType
	}
}

// parsePartitionRegex parses the partition regex and populates the partitionInfo slice
func (i *Iceberg) parsePartitionRegex(pattern string) error {
	// path pattern example: /{col_name, partition_transform}/{col_name, partition_transform}
	// This strictly identifies column name and partition transform entries
	patternRegex := regexp.MustCompile(constants.PartitionRegexIceberg)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue // We need at least 3 matches: full match, column name, transform
		}

		colName := strings.Replace(strings.TrimSpace(strings.Trim(match[1], `'"`)), "now()", constants.OlakeTimestamp, 1)
		transform := strings.TrimSpace(strings.Trim(match[2], `'"`))

		// Append to ordered slice to preserve partition order
		i.partitionInfo = append(i.partitionInfo, PartitionInfo{
			Field:     colName,
			Transform: transform,
		})
	}

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
func parseSchema(schemaStr string) (map[string]string, error) {
	// Remove the outer "table {" and "}"
	schemaStr = strings.TrimPrefix(schemaStr, "table {")
	schemaStr = strings.TrimSuffix(schemaStr, "}")

	// Process each line
	lines := strings.Split(schemaStr, "\n")
	fields := make(map[string]string)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse line like: "1: dispatching_base_num: optional string"
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}

		name := strings.TrimSpace(parts[1])
		typeInfo := strings.TrimSpace(parts[2])
		// typeInfo will contain `required type (id)` or `optional type`
		types := strings.Split(typeInfo, " ")
		fields[name] = types[1]

	}
	return fields, nil
}

func init() {
	destination.RegisteredWriters[types.Iceberg] = func() destination.Writer {
		return new(Iceberg)
	}
}
