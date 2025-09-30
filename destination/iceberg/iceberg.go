package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
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
	schema        map[string]string // schema for current thread associated with java writer (col -> type)
	// Why Schema On Thread Level ?
	// Schema on thread level is identical to writer instance that is available in java server
	// It tells when to complete java writer and when to evolve schema.
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

	server, err := newIcebergClient(i.config, i.partitionInfo, options.ThreadID, false, isUpsertMode(stream, options.Backfill), i.stream.GetDestinationDatabase(&i.config.IcebergDatabase))
	if err != nil {
		return nil, fmt.Errorf("failed to start iceberg server: %s", err)
	}

	// persist server details
	i.server = server

	// check for identifier fields setting
	identifierField := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)
	var schema map[string]string

	if globalSchema == nil {
		logger.Infof("Creating destination table [%s] in Iceberg database [%s] for stream [%s]", i.stream.GetDestinationTable(), i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.Name())

		var requestPayload proto.IcebergPayload
		iceSchema := utils.Ternary(stream.NormalizationEnabled(), stream.Schema().ToIceberg(), icebergRawSchema()).([]*proto.IcebergPayload_SchemaField)
		requestPayload = proto.IcebergPayload{
			Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
			Metadata: &proto.IcebergPayload_Metadata{
				Schema:          iceSchema,
				DestTableName:   i.stream.GetDestinationTable(),
				ThreadId:        i.server.serverID,
				IdentifierField: &identifierField,
			},
		}

		response, err := i.server.sendClientRequest(ctx, &requestPayload)
		if err != nil {
			return nil, fmt.Errorf("failed to load or create table: %s", err)
		}

		schema, err = parseSchema(response)
		if err != nil {
			return nil, fmt.Errorf("failed to parse schema from resp[%s]: %s", response, err)
		}
	} else {
		// set global schema for current thread
		var ok bool
		schema, ok = globalSchema.(map[string]string)
		if !ok {
			return nil, fmt.Errorf("failed to convert globalSchema of type[%T] to map[string]string", globalSchema)
		}
	}

	// set schema for current thread
	i.schema = copySchema(schema)
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
				val, exist := record.Data[field.Key]
				if !exist {
					protoColumnsValue = append(protoColumnsValue, nil)
					continue
				}
				switch field.IceType {
				case "boolean":
					boolValue, err := typeutils.ReformatBool(val)
					if err != nil {
						return fmt.Errorf("failed to reformat rawValue[%v] as bool value: %s", val, err)
					}
					protoColumnsValue = append(protoColumnsValue, &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: boolValue}})
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

	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			DestTableName: i.stream.GetDestinationTable(),
			ThreadId:      i.server.serverID,
			Schema:        protoSchema,
		},
		Records: protoRecords,
	}

	// Send to gRPC server with timeout
	reqCtx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := i.server.sendClientRequest(reqCtx, request)
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
		err := i.server.closeIcebergClient()
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

	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_COMMIT,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      i.server.serverID,
			DestTableName: i.stream.GetDestinationTable(),
		},
	}
	res, err := i.server.sendClientRequest(ctx, request)
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
	server, err := newIcebergClient(i.config, []PartitionInfo{}, i.options.ThreadID, true, false, "test_olake")
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
	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      server.serverID,
			DestTableName: "test_olake",
			Schema:        icebergRawSchema(),
		},
	}

	res, err := server.sendClientRequest(ctx, request)
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
	recrodInsertRequest := &proto.IcebergPayload{
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

	resInsert, err := server.sendClientRequest(ctx, recrodInsertRequest)
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
func (i *Iceberg) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	// dedup records according to cdc timestamp and olakeID
	dedupRecords := func(records []types.RawRecord) []types.RawRecord {
		// only dedup if it is upsert mode
		if !isUpsertMode(i.stream, i.options.Backfill) {
			return records
		}

		// map olakeID -> index of record to keep (index into original slice)
		keepIdx := make(map[string]int, len(records))
		for idx, record := range records {
			existingIdx, ok := keepIdx[record.OlakeID]
			if !ok {
				keepIdx[record.OlakeID] = idx
				continue
			}
			if record.CdcTimestamp == nil {
				keepIdx[record.OlakeID] = idx // keep latest reord (in incremental)
				continue
			}

			ex := records[existingIdx]
			if ex.CdcTimestamp.Before(*record.CdcTimestamp) {
				keepIdx[record.OlakeID] = idx // keep latest reord (w.r.t cdc timestamp)
			}
		}

		out := make([]types.RawRecord, 0, len(keepIdx))
		for rIdx, record := range records {
			if idx, ok := keepIdx[record.OlakeID]; ok && idx == rIdx {
				out = append(out, record)
			}
		}
		return out
	}

	// extractSchemaFromRecords detects difference in current thread schema and the batch that being received
	// Also extracts current batch schema
	extractSchemaFromRecords := func(ctx context.Context, records []types.RawRecord) (bool, map[string]string, error) {
		// detectOrUpdateSchema detects difference in current thread schema and the batch that being received when detectChange is true
		// else updates the schemaMap with the new schema
		detectOrUpdateSchema := func(record types.RawRecord, detectChange bool, threadSchema, finalSchema map[string]string) (bool, error) {
			for key, value := range record.Data {
				detectedType := typeutils.TypeFromValue(value)

				if detectedType == types.Null {
					// remove element from data if it is null
					delete(record.Data, key)
					continue
				}

				detectedIcebergType := detectedType.ToIceberg()
				if _, existInBase := threadSchema[key]; existInBase {
					// Column exists in destination table: restrict to valid promotions only
					valid := validIcebergType(finalSchema[key], detectedIcebergType)
					if !valid {
						return false, fmt.Errorf(
							"failed to validate schema for field[%s] (detected two different types in batch), expected type: %s, detected type: %s",
							key, finalSchema[key], detectedIcebergType,
						)
					}

					if promotionRequired(finalSchema[key], detectedIcebergType) {
						if detectChange {
							return true, nil
						}

						// evolve schema
						finalSchema[key] = detectedIcebergType
					}
				} else {
					// New column: converge to common ancestor across concurrent updates
					if detectChange {
						return true, nil
					}

					// evolve schema
					if existingType, exists := finalSchema[key]; exists {
						finalSchema[key] = getCommonAncestorType(existingType, detectedIcebergType)
					} else {
						finalSchema[key] = detectedIcebergType
					}
				}
			}
			return false, nil
		}

		// parallel flatten data and detect schema difference
		diffThreadSchema := atomic.Bool{}
		err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
			// set pre configured fields
			records[idx].Data[constants.OlakeID] = record.OlakeID
			records[idx].Data[constants.OlakeTimestamp] = time.Now().UTC()
			records[idx].Data[constants.OpType] = record.OperationType
			if record.CdcTimestamp != nil {
				records[idx].Data[constants.CdcTimestamp] = *record.CdcTimestamp
			}

			flattenedRecord, err := typeutils.NewFlattener().Flatten(record.Data)
			if err != nil {
				return fmt.Errorf("failed to flatten record, iceberg writer: %s", err)
			}
			records[idx].Data = flattenedRecord

			// if schema difference is not detected, detect schema difference
			if !diffThreadSchema.Load() {
				changeDetected, err := detectOrUpdateSchema(records[idx], true, i.schema, nil)
				if err != nil {
					return fmt.Errorf("failed to detect schema: %s", err)
				}

				diffThreadSchema.Store(changeDetected)
			}

			return nil
		})

		// if schema difference is detected, update schemaMap with the new schema
		schemaMap := copySchema(i.schema)
		if diffThreadSchema.Load() {
			for _, record := range records {
				_, err := detectOrUpdateSchema(record, false, i.schema, schemaMap)
				if err != nil {
					return false, nil, fmt.Errorf("failed to update schema: %s", err)
				}
			}
		}

		return diffThreadSchema.Load(), schemaMap, err
	}

	records = dedupRecords(records)

	if !i.stream.NormalizationEnabled() {
		return false, records, i.schema, nil
	}

	schemaDifference, recordsSchema, err := extractSchemaFromRecords(ctx, records)
	if err != nil {
		return false, nil, nil, fmt.Errorf("failed to extract schema from records: %s", err)
	}

	return schemaDifference, records, recordsSchema, err
}

// compares with global schema and update schema in destination accordingly
func (i *Iceberg) EvolveSchema(ctx context.Context, globalSchema, recordsRawSchema any) (any, error) {
	if !i.stream.NormalizationEnabled() {
		return i.schema, nil
	}

	// cases as local thread schema has detected changes w.r.t. batch records schema
	//  	i.  iceberg table already have changes (i.e. no difference with global schema), in this case
	//		    only refresh table in iceberg for this thread.
	// 		ii. Schema difference is detected w.r.t. iceberg table (i.e. global schema), in this case
	// 			we need to evolve schema in iceberg table
	// NOTE: All the above cases will also complete current writer (java writer instance) as schema change in thread detected

	globalSchemaMap, ok := globalSchema.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("failed to convert globalSchema of type[%T] to map[string]string", globalSchema)
	}

	recordsSchema, ok := recordsRawSchema.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("failed to convert newSchemaMap of type[%T] to map[string]string", recordsRawSchema)
	}

	// case handled:
	// 1. returns true if promotion is possible or new column is added
	// 2. in case of int(globalType) and string(threadType) it return false
	//    and write method will try to parse the string (write will fail if not parsable)
	differentSchema := func(oldSchema, newSchema map[string]string) bool {
		for fieldName, newType := range newSchema {
			if oldType, exists := oldSchema[fieldName]; !exists {
				return true
			} else if promotionRequired(oldType, newType) {
				return true
			}
		}
		return false
	}

	// check for identifier fields setting
	identifierField := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)
	request := proto.IcebergPayload{
		Type: proto.IcebergPayload_EVOLVE_SCHEMA,
		Metadata: &proto.IcebergPayload_Metadata{
			IdentifierField: &identifierField,
			DestTableName:   i.stream.GetDestinationTable(),
			ThreadId:        i.server.serverID,
		},
	}

	var response string
	var err error
	// check if table schema is different from global schema
	if differentSchema(globalSchemaMap, recordsSchema) {
		logger.Infof("Thread[%s]: evolving schema in iceberg table", i.options.ThreadID)
		for field, fieldType := range recordsSchema {
			request.Metadata.Schema = append(request.Metadata.Schema, &proto.IcebergPayload_SchemaField{
				Key:     field,
				IceType: fieldType,
			})
		}

		response, err = i.server.sendClientRequest(ctx, &request)
		if err != nil {
			return false, fmt.Errorf("failed to evolve schema: %s", err)
		}
	} else {
		logger.Debugf("Thread[%s]: refreshing table schema", i.options.ThreadID)
		request.Type = proto.IcebergPayload_REFRESH_TABLE_SCHEMA
		response, err = i.server.sendClientRequest(ctx, &request)
		if err != nil {
			return false, fmt.Errorf("failed to refresh schema: %s", err)
		}
	}

	// only refresh table schema
	schemaAfterEvolution, err := parseSchema(response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema from resp[%s]: %s", response, err)
	}

	i.schema = copySchema(schemaAfterEvolution)
	return schemaAfterEvolution, nil
}

// return if evolution is valid or not
func validIcebergType(oldType, newType string) bool {
	if oldType == newType || getCommonAncestorType(oldType, newType) == oldType {
		return true
	}

	switch fmt.Sprintf("%s->%s", oldType, newType) {
	case "int->long", "float->double", "long->int", "double->float":
		return true
	default:
		return false
	}
}

// promotion only required from int -> long and float -> double
func promotionRequired(oldType, newType string) bool {
	switch fmt.Sprintf("%s->%s", oldType, newType) {
	case "int->long", "float->double":
		return true
	default:
		return false
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
func copySchema(schema map[string]string) map[string]string {
	copySchema := make(map[string]string)
	for key, value := range schema {
		copySchema[key] = value
	}
	return copySchema
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

func getCommonAncestorType(d1, d2 string) string {
	// check for cases:
	// d1: string d2: int  -> return string
	// d1: float d2: int  -> return float
	// d1: string d2: float  -> return string
	// d1: string d2: timestamp  -> return string

	oldDT := types.IcebergTypeToDatatype(d1)
	newDT := types.IcebergTypeToDatatype(d2)
	return types.GetCommonAncestorType(oldDT, newDT).ToIceberg()
}

func isUpsertMode(stream types.StreamInterface, backfill bool) bool {
	return utils.Ternary(stream.Self().StreamMetadata.AppendMode, false, !backfill).(bool)
}

func init() {
	destination.RegisteredWriters[types.Iceberg] = func() destination.Writer {
		return new(Iceberg)
	}
}
