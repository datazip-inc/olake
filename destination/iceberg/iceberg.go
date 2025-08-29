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
	partitionInfo []PartitionInfo   // ordered slice to preserve partition column order
	server        *serverInstance   // java server instance
	schema        map[string]string // schema for current thread associated with java
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

func (i *Iceberg) Setup(ctx context.Context, stream types.StreamInterface, globalSchema any, options *destination.Options) (any, error) {
	i.options = options
	i.stream = stream
	i.partitionInfo = make([]PartitionInfo, 0)
	i.schema = make(map[string]string)
	// Parse partition regex from stream metadata
	partitionRegex := i.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := i.parsePartitionRegex(partitionRegex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition regex: %s", err)
		}
	}

	server, err := newIcebergClient(i.config, i.partitionInfo, options.ThreadID, false, isUpsertMode(stream, options.Backfill))
	if err != nil {
		return nil, fmt.Errorf("failed to start iceberg server: %s", err)
	}

	// persist server details
	i.server = server

	// check for identifier fields setting
	identifierField := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)
	var schema map[string]string

	if globalSchema == nil {
		var requestPayload proto.IcebergPayload
		iceSchema := utils.Ternary(stream.NormalizationEnabled(), stream.Schema().ToIceberg(), icebergRawSchema()).([]*proto.IcebergPayload_SchemaField)
		requestPayload = proto.IcebergPayload{
			Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
			Metadata: &proto.IcebergPayload_Metadata{
				Schema:          iceSchema,
				DestTableName:   i.stream.Name(),
				ThreadId:        i.server.serverID,
				IdentifierField: &identifierField,
			},
		}

		resp, err := i.server.sendClientRequest(ctx, &requestPayload)
		if err != nil {
			return nil, fmt.Errorf("failed to load or create table: %s", err)
		}
		schema, err = parseSchema(resp)
	} else {
		// set global schema for current thread
		var ok bool
		schema, ok = globalSchema.(map[string]string)
		if !ok {
			return false, fmt.Errorf("failed to convert globalSchema of type[%T] to map[string]string", schema)
		}
	}

	// set schema for current thread
	i.setSchema(schema)
	return schema, nil
}

// note: java server parses time from long value which will in milliseconds
func (i *Iceberg) Write(ctx context.Context, records []types.RawRecord) error {
	protoSchema := make([]*proto.IcebergPayload_SchemaField, 0, len(i.schema))
	for field, dType := range i.schema {
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

				if cmp := typeutils.Compare(iField, jField); cmp != 0 {
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

		protoColumnsValue := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))
		if !i.stream.NormalizationEnabled() {
			protoCols, err := rawDataColumnBuffer(record, protoSchema)
			if err != nil {
				return fmt.Errorf("failed to create raw data column buffer: %s", err)
			}
			protoColumnsValue = protoCols
		} else {
			for _, field := range protoSchema {
				val, ok := record.Data[field.Key]
				if !ok {
					protoColumnsValue = append(protoColumnsValue, nil)
					continue
				}
				switch field.IceType {
				case "boolean":
					boolVal, err := typeutils.ReformatBool(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] as bool value: %s", val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: boolVal}})
				case "int":
					intValue, err := typeutils.ReformatInt32(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as int32 value: %s", val, val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_IntValue{IntValue: intValue}})
				case "long":
					longValue, err := typeutils.ReformatInt64(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as long value: %s", val, val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: longValue}})
				case "float":
					floatValue, err := typeutils.ReformatFloat32(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as float32 value: %s", val, val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_FloatValue{FloatValue: floatValue}})
				case "double":
					doubleValue, err := typeutils.ReformatFloat64(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as float64 value: %s", val, val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: doubleValue}})
				case "timestamptz":
					timeValue, err := typeutils.ReformatDate(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as time value: %s", val, val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: timeValue.UnixMilli()}})
				default:
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: fmt.Sprintf("%v", val)}})
				}
			}
		}

		if len(protoColumnsValue) > 0 {
			protoRecords = append(protoRecords, &proto.IcebergPayload_IceRecord{
				Fields:     protoColumnsValue,
				RecordType: record.OperationType,
			})
		}
	}

	if len(protoRecords) == 0 {
		logger.Debugf("Thread[%s]: no record found in batch", i.options.ThreadID)
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
	reqCtx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := i.server.sendClientRequest(reqCtx, req)
	if err != nil {
		return fmt.Errorf("failed to send batch: %s", err)
	}

	logger.Debugf("Thread[%s]: sent batch to Iceberg server, response: %s", i.options.ThreadID, res)
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
			logger.Errorf("Thread[%s]: error closing Iceberg client: %s", i.options.ThreadID, err)
		}
	}()

	if i.stream == nil {
		// for check connection no commit will happen
		return nil
	}

	// Send commit request for this thread using a special message format
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
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
		return fmt.Errorf("failed to send commit message: %s", err)
	}

	logger.Debugf("Thread[%s]: Sent commit message: %s", i.options.ThreadID, res)
	return nil
}

func (i *Iceberg) Check(ctx context.Context) error {
	i.options = &destination.Options{
		ThreadID: "test_iceberg_destination",
	}
	// Create a temporary setup for checking
	server, err := newIcebergClient(i.config, []PartitionInfo{}, i.options.ThreadID, true, false)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg server: %s", err)
	}

	// to close client properly
	i.server = server
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

	logger.Infof("Thread[%s]: table created or loaded test olake: %s", i.options.ThreadID, res)

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

	logger.Debugf("Thread[%s]: record inserted successfully: %s", i.options.ThreadID, resInsert)
	return nil
}

func (i *Iceberg) Type() string {
	return string(types.Iceberg)
}

// validate schema change & evolution and removes null records
func (i *Iceberg) FlattenAndCleanData(records []types.RawRecord) (bool, any, error) {
	if !i.stream.NormalizationEnabled() {
		return false, i.schema, nil
	}

	dedupRecords := func(records []types.RawRecord) []types.RawRecord {
		// map olakeID -> index of record to keep (index into original slice)
		keepIdx := make(map[string]int, len(records))

		for idx, record := range records {
			if existingIdx, ok := keepIdx[record.OlakeID]; !ok {
				keepIdx[record.OlakeID] = idx
				continue
			} else {
				ex := records[existingIdx]
				if record.CdcTimestamp == nil {
					keepIdx[record.OlakeID] = idx // keep latest reord (in incremental)
					continue
				}

				if ex.CdcTimestamp.Before(*record.CdcTimestamp) {
					keepIdx[record.OlakeID] = idx // keep latest reord (w.r.t cdc timestamp)
				}
			}
		}

		out := make([]types.RawRecord, 0, len(keepIdx))
		for i, r := range records {
			if idx, ok := keepIdx[r.OlakeID]; ok && idx == i {
				out = append(out, r)
			}
		}
		return out
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
				if typeInNewSchema, exists := newSchema[key]; exists {
					if valid, _ := icebergEvolution(typeInNewSchema, detecteIceType); !valid {
						return nil, fmt.Errorf(
							"failed to validate schema (detected two different types in batch), expected type: %s, detected type: %s",
							typeInNewSchema, detecteIceType,
						)
					}
				}
				newSchema[key] = detecteIceType
			}
		}

		return newSchema, nil
	}

	// only dedup if it is upsert mode
	if isUpsertMode(i.stream, i.options.Backfill) {
		records = dedupRecords(records)
	}

	newSchema, err := extractSchemaFromRecords(records)
	if err != nil {
		return false, nil, err
	}

	// check with current thread schema
	evolveSchema, err := compareSchema(i.schema, newSchema)

	return evolveSchema, newSchema, err
}

// compares with global schema and update schema in destination accordingly
func (i *Iceberg) EvolveSchema(ctx context.Context, newRawSchema, globalSchema any) (any, error) {
	if !i.stream.NormalizationEnabled() {
		return i.schema, nil
	}

	logger.Infof("Thread[%s]: schema evolution detected", i.options.ThreadID)
	globalSchemaMap, ok := globalSchema.(map[string]string)
	if !ok {
		return false, fmt.Errorf("failed to convert globalSchema of type[%T] to map[string]string", globalSchema)
	}

	newSchemaMap, ok := newRawSchema.(map[string]string)
	if !ok {
		return false, fmt.Errorf("failed to convert globalSchema of type[%T] to map[string]string", globalSchema)
	}

	if promote, err := compareSchema(globalSchemaMap, newSchemaMap); err != nil {
		return false, fmt.Errorf("failed to compare schema: %s", err)
	} else if !promote {
		return false, nil
	}

	logger.Infof("Thread[%s]: evolving schema in iceberg table")

	var schema []*proto.IcebergPayload_SchemaField
	for field, fieldType := range i.schema {
		schema = append(schema, &proto.IcebergPayload_SchemaField{
			Key:     field,
			IceType: fieldType,
		})
	}

	// check for identifier fields setting
	identifierField := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)

	req := proto.IcebergPayload{
		Type: proto.IcebergPayload_EVOLVE_SCHEMA,
		Metadata: &proto.IcebergPayload_Metadata{
			IdentifierField: &identifierField,
			DestTableName:   i.stream.Name(),
			Schema:          schema,
			ThreadId:        i.server.serverID,
		},
	}

	resp, err := i.server.sendClientRequest(ctx, &req)
	if err != nil {
		return false, fmt.Errorf("failed to send records to evolve schema: %s", err)
	}

	// update schema in current thread after evolution
	i.setSchema(newSchemaMap)

	logger.Debugf("Thread[%s]: response received after schema evolution: %s", i.options.ThreadID, resp)
	return i.schema, nil
}

// return if old type can be evolved to new type as well as if promotion required or not
func icebergEvolution(oldType, newType string) (bool, bool) {
	if oldType == newType {
		return true, false
	}

	switch fmt.Sprintf("%s->%s", oldType, newType) {
	case "int->long", "float->double":
		return true, true // Promotion requires evolution
	case "long->int", "double->float":
		return true, false // Safe narrowing doesn't require evolution
	default:
		return false, true
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

// returns a new copy of schema
func (i *Iceberg) setSchema(schema map[string]string) {
	copySchema := make(map[string]string)
	for key, value := range schema {
		copySchema[key] = value
	}
	i.schema = copySchema
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
	protoColumnsValue := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))

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
			protoColumnsValue = append(protoColumnsValue, nil)
			continue
		}
		protoColumnsValue = append(protoColumnsValue, value)
	}
	return protoColumnsValue, nil
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

func compareSchema(oldSchema, newSchema map[string]string) (bool, error) {
	schemaChange := false
	for fieldName, newType := range newSchema {
		oldType, exists := oldSchema[fieldName]

		if !exists {
			schemaChange = true
			continue
		}

		if validType, promotion := icebergEvolution(oldType, newType); !validType {
			return false, fmt.Errorf(
				"different type detected in schema, old type: %s, new type: %s for field: %s",
				oldType, newType, fieldName,
			)
		} else {
			schemaChange = promotion || schemaChange
		}
	}

	return schemaChange, nil
}

func isUpsertMode(stream types.StreamInterface, backfill bool) bool {
	return utils.Ternary(stream.Self().StreamMetadata.AppendMode, false, !backfill).(bool)
}

func init() {
	destination.RegisteredWriters[types.Iceberg] = func() destination.Writer {
		return new(Iceberg)
	}
}
