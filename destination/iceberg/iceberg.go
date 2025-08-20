package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type Iceberg struct {
	options       *destination.Options
	config        *Config
	stream        types.StreamInterface
	partitionInfo []PartitionInfo // ordered slice to preserve partition column order
	server        *serverInstance // java server instance
}

// PartitionInfo represents a Iceberg partition column with its transform, preserving order
type PartitionInfo struct {
	field     string
	transform string
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

	// Parse partition regex from stream metadata
	partitionRegex := i.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := i.parsePartitionRegex(partitionRegex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition regex: %s", err)
		}
	}

	upsertMode := utils.Ternary(i.stream.Self().StreamMetadata.AppendMode, false, !options.Backfill).(bool)
	server, err := newIcebergClient(i.config, i.partitionInfo, false, upsertMode)
	if err != nil {
		return nil, fmt.Errorf("failed to start iceberg server: %s", err)
	}

	// persist server details
	i.server = server

	// check for identifier fields setting
	primaryKey := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)

	if createOrLoadSchema {
		var requestPayload proto.IcebergPayload
		iceSchema := utils.Ternary(stream.NormalizationEnabled(), stream.Schema().ToIceberg(), icebergRawSchema()).([]*proto.IcebergPayload_SchemaField)
		requestPayload = proto.IcebergPayload{
			Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
			Metadata: &proto.IcebergPayload_Metadata{
				Schema:        iceSchema,
				DestTableName: i.stream.Name(),
				ThreadId:      i.server.serverID,
				PrimaryKey:    &primaryKey,
			},
		}

		resp, err := i.server.sendClientRequest(ctx, &requestPayload)
		if err != nil {
			return nil, fmt.Errorf("failed to load or create table: %s", err)
		}
		return parseSchema(resp)
	}
	// note: calling function variable only update if createOrLoadSchema is true
	return icebergRawSchema(), nil
}

// note: java server parses time from long value which will in milliseconds
func (i *Iceberg) Write(ctx context.Context, schema any, records []types.RawRecord) error {
	schemaMap, ok := schema.(map[string]string)
	if !ok {
		return fmt.Errorf("failed to convert schema map[%T] to map[string]string", schema)
	}

	protoSchema := make([]*proto.IcebergPayload_SchemaField, 0, len(schemaMap))
	for field, dType := range schemaMap {
		protoSchema = append(protoSchema, &proto.IcebergPayload_SchemaField{
			Key:     field,
			IceType: dType,
		})
	}

	if len(i.partitionInfo) > 0 {
		// sort record based on partition order
		sort.Slice(records, func(idx, jdx int) bool {
			iRecord := records[idx].Data
			jRecord := records[jdx].Data

			for _, partition := range i.partitionInfo {
				iField, iOk := iRecord[partition.field]
				jField, jOk := jRecord[partition.field]

				if !iOk && !jOk {
					continue // i == j
				} else if !iOk {
					return true // i < j
				} else if !jOk {
					return false // i > j
				}

				if cmp := utils.CompareInterfaceValue(iField, jField); cmp != 0 {
					return cmp == -1 // i < j
				}
			}
			return true // i == j
		})
	}

	protoRecords := make([]*proto.IcebergPayload_IceRecord, 0, len(records))
	for _, record := range records {
		if record.Data == nil {
			continue
		}

		protoColumns := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))
		if !i.stream.NormalizationEnabled() {
			protoCols, err := rawDataColumnBuffer(record, protoSchema)
			if err != nil {
				return fmt.Errorf("failed to create raw data column buffer")
			}
			protoColumns = protoCols
		} else {
			for _, field := range protoSchema {
				val, ok := record.Data[field.Key]
				if !ok {
					protoColumns = append(protoColumns, nil)
					continue
				}
				switch field.IceType {
				case "boolean":
					boolVal, err := typeutils.ReformatBool(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] as bool value: %s", val, err)
					}
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: boolVal}})
				case "int":
					intValue, err := typeutils.ReformatInt32(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as int32 value: %s", val, val, err)
					}
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_IntValue{IntValue: intValue}})
				case "long":
					longValue, err := typeutils.ReformatInt64(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as long value: %s", val, val, err)
					}
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: longValue}})
				case "float":
					floatValue, err := typeutils.ReformatFloat32(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as float32 value: %s", val, val, err)
					}
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_FloatValue{FloatValue: floatValue}})
				case "double":
					doubleValue, err := typeutils.ReformatFloat64(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as float64 value: %s", val, val, err)
					}
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: doubleValue}})
				case "timestamptz":
					timeValue, err := typeutils.ReformatDate(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as time value: %s", val, val, err)
					}
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: timeValue.UnixMilli()}})
				default:
					protoColumns = append(protoColumns, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: fmt.Sprintf("%v", val)}})
				}
			}
		}

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
			ThreadId:      i.server.serverID,
			Schema:        protoSchema,
		},
		Records: protoRecords,
	}

	// Send to gRPC server with timeout
	reqCtx, cancel := context.WithTimeout(ctx, 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := i.server.sendClientRequest(reqCtx, req)
	if err != nil {
		logger.Errorf("failed to send batch: %s", err)
		return err
	}

	logger.Infof("Sent batch to Iceberg server, response: %s", res)
	return nil
}

func (i *Iceberg) Close(ctx context.Context) error {
	// skip flushing on error
	defer func() {
		if i.server == nil {
			return
		}
		err := i.server.closeIcebergClient(i.server)
		if err != nil {
			logger.Errorf("thread id %s: Error closing Iceberg client: %s", i.server.serverID, err)
		}
	}()
	if ctx.Err() != nil {
		return nil
	}
	// Send commit request for this thread using a special message format
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Second)
	defer cancel()

	req := &proto.IcebergPayload{
		Type: proto.IcebergPayload_COMMIT,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      i.server.serverID,
			DestTableName: i.stream.Name(),
		},
	}
	res, err := i.server.sendClientRequest(ctx, req)
	if err != nil {
		logger.Errorf("thread id %s: Error sending commit message on close: %s", i.server.serverID, err)
		return fmt.Errorf("thread id %s: failed to send commit message: %s", i.server.serverID, err)
	}

	logger.Infof("thread id %s: Sent commit message: %s", i.server.serverID, res)
	return nil
}

func (i *Iceberg) Check(ctx context.Context) error {
	// Create a temporary setup for checking
	server, err := newIcebergClient(i.config, []PartitionInfo{}, true, false)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg server: %s", err)
	}
	i.server = server // to close properly
	defer func() {
		i.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// try to create table
	req := &proto.IcebergPayload{
		Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      server.serverID,
			DestTableName: "test_olake",
			Schema:        icebergRawSchema(),
		},
	}

	res, err := server.sendClientRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create or get table: %s", err)
	}
	logger.Infof("table created or loaded test olake: %s", res)

	// try writing record in dest table
	currentTime := time.Now().UTC()
	protoSchema := icebergRawSchema()
	record := types.CreateRawRecord("olake_test", map[string]any{"name": "olake"}, "r", &currentTime)
	protoColumns, err := rawDataColumnBuffer(record, protoSchema)
	if err != nil {
		return fmt.Errorf("failed to create raw data column buffer: %s", err)
	}
	recrodInsertReq := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      server.serverID,
			DestTableName: "test_olake",
			Schema:        protoSchema,
		},
		Records: []*proto.IcebergPayload_IceRecord{{
			Fields:     protoColumns,
			RecordType: "r",
		}},
	}

	resInsert, err := server.sendClientRequest(ctx, recrodInsertReq)
	if err != nil {
		return fmt.Errorf("failed to insert request: %s", err)
	}

	logger.Infof("record inserted successfully: %s", resInsert)
	return nil
}

func (i *Iceberg) Type() string {
	return string(types.Iceberg)
}

// validate schema change & evolution and removes null records
func (i *Iceberg) FlattenAndCleanData(rawOldSchema any, records []types.RawRecord) (bool, any, error) {
	if !i.stream.NormalizationEnabled() {
		return false, nil, nil
	}
	extractSchemaFromRecords := func(records []types.RawRecord) (map[string]string, error) {
		newSchema := make(map[string]string)
		for idx, record := range records {
			// TODO: normalized column names (remove from driver side)

			// set pre configured fields
			records[idx].Data[constants.OlakeID] = record.OlakeID
			records[idx].Data[constants.OlakeTimestamp] = time.Now().UTC()
			records[idx].Data[constants.OpType] = record.OperationType
			if record.CdcTimestamp != nil {
				records[idx].Data[constants.CdcTimestamp] = record.CdcTimestamp
			}

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
						"failed to validate schema (detected two different types in batch), expected type: %s, detected type: %s",
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

func (i *Iceberg) EvolveSchema(ctx context.Context, newSchema any) error {
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
			ThreadId:      i.server.serverID,
		},
	}

	resp, err := i.server.sendClientRequest(ctx, &req)
	if err != nil {
		return fmt.Errorf("failed to send records to evolve schema: %s", err)
	}

	logger.Debugf("response received after schema evolution: %s", resp)
	return nil
}

func isValidIcebergType(oldType, newType string) bool {
	// Note: not added decimal precision as we not iceberg decimal type support as of now
	switch oldType {
	case "int":
		return newType == "int" || newType == "long"
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
			field:     colName,
			transform: transform,
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

		// Parse line like: "1: col_name: optional string"
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

func rawDataColumnBuffer(record types.RawRecord, protoSchema []*proto.IcebergPayload_SchemaField) ([]*proto.IcebergPayload_IceRecord_FieldValue, error) {
	dataMap := make(map[string]*proto.IcebergPayload_IceRecord_FieldValue)
	protoColumns := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))

	dataMap[constants.OlakeID] = &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: record.OlakeID}}
	dataMap[constants.OpType] = &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: record.OperationType}}
	dataMap[constants.OlakeTimestamp] = &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: time.Now().UTC().UnixMilli()}}
	if record.CdcTimestamp != nil {
		dataMap[constants.CdcTimestamp] = &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: record.CdcTimestamp.UTC().UnixMilli()}}
	}

	bytesData, err := json.Marshal(record.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data in normalization: %s", err)
	}
	dataMap[constants.StringifiedData] = &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: string(bytesData)}}

	for _, field := range protoSchema {
		value, ok := dataMap[field.Key]
		if !ok {
			protoColumns = append(protoColumns, nil)
			continue
		}
		protoColumns = append(protoColumns, value)
	}
	return protoColumns, nil
}

// returns raw schema in iceberg format
func icebergRawSchema() []*proto.IcebergPayload_SchemaField {
	var icebergFields []*proto.IcebergPayload_SchemaField
	for key, typ := range types.RawSchema {
		icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
			IceType: typ.ToIceberg(),
			Key:     key,
		})
	}
	return icebergFields
}

func init() {
	destination.RegisteredWriters[types.Iceberg] = func() destination.Writer {
		return new(Iceberg)
	}
}
