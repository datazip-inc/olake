package iceberg

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	arrowwriter "github.com/datazip-inc/olake/destination/iceberg/arrow-writer"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	legacywriter "github.com/datazip-inc/olake/destination/iceberg/legacy-writer"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/spf13/viper"
)

type Iceberg struct {
    options       *destination.Options
    config        *Config
    stream        types.StreamInterface
    partitionInfo []internal.PartitionInfo // ordered slice to preserve partition column order
    server        *serverInstance          // Java server instance
    schema        map[string]string        // schema for current thread associated with Java writer (col -> type)
    writer        IcebergWriter            // writer instance

    // Why Schema On Thread Level?
    // Schema on thread level is identical to the writer instance available in the Java server.
    // It defines when to complete the Java writer and when schema evolution is required.
}

func (i *Iceberg) GetConfigRef() destination.Config {
	i.config = &Config{}
	return i.config
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) NewWriter(ctx context.Context) (IcebergWriter, error) {
	if i.config.UseArrowWrites {
		writer, err := arrowwriter.New(ctx, i.partitionInfo, i.schema, i.stream, i.server);
		if err != nil {
			return nil, err
		}

		return writer, nil
	}

	// default: legacy writer
	return legacywriter.New(i.options, i.schema, i.stream, i.server), nil
}

func (i *Iceberg) Setup(ctx context.Context, stream types.StreamInterface, globalSchema any, options *destination.Options) (any, error) {
	i.options = options
	i.stream = stream
	i.partitionInfo = make([]internal.PartitionInfo, 0)
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

		response, err := i.server.SendClientRequest(ctx, &requestPayload)
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

	writer, err := i.NewWriter(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create iceberg writer: %v", err)
	}
	i.writer = writer

	return schema, nil
}

// note: java server parses time from long value which will in milliseconds
func (i *Iceberg) Write(ctx context.Context, records []types.RawRecord) error {
	return i.writer.Write(ctx, records)
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
	ctx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	return i.writer.Close(ctx)
}

func (i *Iceberg) Check(ctx context.Context) error {
	i.options = &destination.Options{
		ThreadID: "test_iceberg_destination",
	}

	destinationDB := "test_olake"
	if prefix := viper.GetString(constants.DestinationDatabasePrefix); prefix != "" {
		destinationDB = fmt.Sprintf("%s_%s", utils.Reformat(prefix), destinationDB)
	}
	// Create a temporary setup for checking
	server, err := newIcebergClient(i.config, []internal.PartitionInfo{}, i.options.ThreadID, true, false, destinationDB)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg server: %s", err)
	}

	// to close client properly
	i.server = server
	defer func() {
		i.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	// try to create table
	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      server.serverID,
			DestTableName: destinationDB,
			Schema:        icebergRawSchema(),
		},
	}

	res, err := server.SendClientRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to create or get table: %s", err)
	}

	logger.Infof("Thread[%s]: table created or loaded test olake: %s", i.options.ThreadID, res)

	// try writing record in dest table
	currentTime := time.Now().UTC()
	protoSchema := icebergRawSchema()
	record := types.CreateRawRecord(destinationDB, map[string]any{"name": "olake"}, "r", &currentTime)
	protoColumns, err := internal.RawDataColumnBuffer(record, protoSchema)
	if err != nil {
		return fmt.Errorf("failed to create raw data column buffer: %s", err)
	}
	recrodInsertRequest := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      server.serverID,
			DestTableName: destinationDB,
			Schema:        protoSchema,
		},
		Records: []*proto.IcebergPayload_IceRecord{{
			Fields:     protoColumns,
			RecordType: "r",
		}},
	}

	resInsert, err := server.SendClientRequest(ctx, recrodInsertRequest)
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
				if _, existInIceberg := threadSchema[key]; existInIceberg {
					// Column exists in iceberg table: restrict to valid promotions only
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
				if changeDetected, err := detectOrUpdateSchema(records[idx], true, i.schema, copySchema(i.schema)); err != nil {
					return fmt.Errorf("failed to detect schema: %s", err)
				} else if changeDetected {
					diffThreadSchema.Store(true)
				}
			}

			return nil
		})
		if err != nil {
			return false, nil, fmt.Errorf("failed to flatten schema concurrently and detect change in records: %s", err)
		}

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

		response, err = i.server.SendClientRequest(ctx, &request)
		if err != nil {
			return false, fmt.Errorf("failed to evolve schema: %s", err)
		}
	} else {
		logger.Debugf("Thread[%s]: refreshing table schema", i.options.ThreadID)
		request.Type = proto.IcebergPayload_REFRESH_TABLE_SCHEMA
		response, err = i.server.SendClientRequest(ctx, &request)
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
		i.partitionInfo = append(i.partitionInfo, internal.PartitionInfo{
			Field:     colName,
			Transform: transform,
		})
	}

	return nil
}

// drop streams required for clear destination
func (i *Iceberg) DropStreams(ctx context.Context, dropStreams []types.StreamInterface) error {
	i.options = &destination.Options{
		ThreadID: "iceberg_destination_drop",
	}
	if len(dropStreams) == 0 {
		logger.Info("No streams selected for clearing Iceberg destination, skipping operation")
		return nil
	}

	// server setup for dropping tables
	server, err := newIcebergClient(i.config, []internal.PartitionInfo{}, i.options.ThreadID, false, false, "")
	if err != nil {
		return fmt.Errorf("failed to setup iceberg server for dropping streams: %s", err)
	}

	// to close client properly
	i.server = server
	defer func() {
		i.Close(ctx)
	}()

	logger.Infof("Starting Clear Iceberg destination for %d selected streams", len(dropStreams))

	// process each stream
	for _, stream := range dropStreams {
		destDB := stream.GetDestinationDatabase(&i.config.IcebergDatabase)
		destTable := stream.GetDestinationTable()
		dropTable := fmt.Sprintf("%s.%s", destDB, destTable)

		logger.Infof("Dropping Iceberg table: %s", dropTable)

		request := proto.IcebergPayload{
			Type: proto.IcebergPayload_DROP_TABLE,
			Metadata: &proto.IcebergPayload_Metadata{
				DestTableName: dropTable,
				ThreadId:      i.server.serverID,
			},
		}
		_, err := i.server.SendClientRequest(ctx, &request)
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %s", dropTable, err)
		}
	}

	logger.Info("Successfully cleared Iceberg destination for selected streams")
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
