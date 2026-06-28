package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
	server        *serverInstance          // shared Java server instance (per-process singleton)
	schema        map[string]string        // schema for current thread associated with Java writer (col -> type)
	writer        Writer                   // writer instance
	olake2PCState *types.MetadataState     // olake_2pc_state for current stream
	// Why Schema On Thread Level?
	// Schema on thread level is identical to the writer instance available in the Java server.
	// It defines when to complete the Java writer and when schema evolution is required.
}

// Pre-computed lookup tables for type validation
var validTypeTransitions = map[string]map[string]bool{
	"int":         {"int": true, "long": true},
	"long":        {"long": true, "int": true},
	"float":       {"float": true, "double": true},
	"double":      {"double": true, "float": true},
	"boolean":     {"boolean": true},
	"string":      {"string": true},
	"timestamptz": {"timestamptz": true},
}

// promotionTransitions tracks when type promotion is required
var promotionTransitions = map[string]map[string]bool{
	"int":   {"long": true},
	"float": {"double": true},
}

func (i *Iceberg) Type() string {
	return string(types.Iceberg)
}

func (i *Iceberg) GetConfigRef() destination.Config {
	i.config = &Config{}
	return i.config
}

// SetConfig points this writer at a shared, already-parsed config instance.
func (i *Iceberg) SetConfig(c destination.Config) {
	i.config = c.(*Config)
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) Setup(ctx context.Context, stream types.StreamInterface, _ any, options *destination.Options) (any, *types.MetadataState, error) {
	i.options = options
	i.stream = stream
	i.partitionInfo = make([]internal.PartitionInfo, 0)
	i.schema = make(map[string]string)

	// parse partition regex
	var err error
	i.partitionInfo, err = parsePartitionRegex(stream.GetPartitionRegex())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse partition regex: %s", err)
	}
	logger.Debugf("Thread[%s]: setting up iceberg writer for table[%s.%s]", i.options.ThreadID, stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())

	// when normalization=false, include partition columns in the Iceberg schema so
	// the Java server can build the partition spec (spec references columns by field ID)
	partitionFields := make([]string, 0, len(i.partitionInfo))
	if !stream.NormalizationEnabled() {
		for _, p := range i.partitionInfo {
			partitionFields = append(partitionFields, p.Field)
		}
	}

	icebergPartFields := make([]*proto.IcebergPayload_PartitionField, 0, len(i.partitionInfo))
	for _, p := range i.partitionInfo {
		icebergPartFields = append(icebergPartFields, &proto.IcebergPayload_PartitionField{
			Field:     p.Field,
			Transform: p.Transform,
		})
	}

	identifierField := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)
	iceSchema := stream.Schema().ToIceberg(!stream.NormalizationEnabled(), i.stream, partitionFields...)
	requestPayload := proto.IcebergPayload{
		Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
		Metadata: &proto.IcebergPayload_Metadata{
			Schema:          iceSchema,
			DestTableName:   stream.GetDestinationTable(),
			ThreadId:        options.ThreadID,
			IdentifierField: &identifierField,
			Namespace:       stream.GetDestinationDatabase(&i.config.IcebergDatabase),
			Upsert:          isUpsertMode(stream, options.Backfill),
			PartitionFields: icebergPartFields,
		},
	}

	response, err := i.server.SendClientRequest(ctx, &requestPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load or create table: %s", err)
	}

	ingestResponse := response.(*proto.RecordIngestResponse)
	schema, err := parseSchema(ingestResponse.GetResult())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse schema from resp[%s]: %s", ingestResponse.GetResult(), err)
	}

	// Capture optional olake_2pc state from table metadata without returning early,
	// so we fall through to create the writer for this thread.
	if olake2PCState := ingestResponse.GetOlake_2PcState(); olake2PCState != "" {
		var metadataState types.MetadataState
		if err := json.Unmarshal([]byte(olake2PCState), &metadataState); err != nil {
			return schema, nil, fmt.Errorf("failed to unmarshal 2pc metadata state: %s", err)
		}
		i.olake2PCState = &metadataState
	}

	// set schema for current thread
	i.schema = copySchema(schema)

	if i.config.UseArrowWrites {
		i.writer, err = arrowwriter.New(ctx, i.options, i.partitionInfo, i.schema, i.stream, i.server, isUpsertMode(i.stream, i.options.Backfill))
	} else {
		i.writer = legacywriter.New(i.options, i.schema, i.stream, i.server)
	}

	return schema, i.olake2PCState, nil
}

// note: java server parses time from long value which will in milliseconds
func (i *Iceberg) Write(ctx context.Context, records []types.RawRecord) error {
	return i.writer.Write(ctx, records)
}

func (i *Iceberg) Close(ctx context.Context, finalMetadataState any) (err error) {
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req := &proto.IcebergPayload{
			Type: proto.IcebergPayload_CLOSE_SESSION,
			Metadata: &proto.IcebergPayload_Metadata{
				ThreadId: i.options.ThreadID,
			},
		}

		if _, cleanupErr := i.server.SendClientRequest(cleanupCtx, req); cleanupErr != nil {
			if err == nil {
				err = cleanupErr
			} else {
				err = fmt.Errorf("%s: cleanup error: %w", err, cleanupErr)
			}
		}
	}()

	select {
	case <-ctx.Done():
		// skip commit in case of context cancellation
		return ctx.Err()
	default:
		return i.writer.Close(ctx, finalMetadataState)
	}
}

func (i *Iceberg) Check(ctx context.Context) error {
	destinationDB := "test_olake"
	if prefix := viper.GetString(constants.DestinationDatabasePrefix); prefix != "" {
		destinationDB = fmt.Sprintf("%s_%s", utils.Reformat(prefix), destinationDB)
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	identifierField := utils.Ternary(i.config.NoIdentifierFields, "", constants.OlakeID).(string)
	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_GET_OR_CREATE_TABLE,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:        i.server.serverID,
			DestTableName:   destinationDB,
			Schema:          types.GetIcebergRawSchema(),
			Namespace:       destinationDB,
			Upsert:          false,
			IdentifierField: &identifierField,
		},
	}

	res, err := i.server.SendClientRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to create or get table: %s", err)
	}

	ingestResponse := res.(*proto.RecordIngestResponse)
	logger.Infof("Thread[%s]: table created or loaded test olake: %s", i.server.serverID, ingestResponse.GetResult())

	// try writing record in dest table
	currentTime := time.Now().UTC()
	protoSchema := types.GetIcebergRawSchema()
	record := types.CreateRawRecord(map[string]any{"name": "olake"}, map[string]any{constants.OlakeID: "olake", constants.OpType: "r", constants.CdcTimestamp: &currentTime})
	protoColumns, err := legacywriter.RawDataColumnBuffer(record, protoSchema)
	if err != nil {
		return fmt.Errorf("failed to create raw data column buffer: %s", err)
	}
	recrodInsertRequest := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			// Session already created by the GET_OR_CREATE_TABLE above; RECORDS
			// carries only the routing thread_id plus the schema for this batch.
			ThreadId: i.server.serverID,
			Schema:   protoSchema,
		},
		Records: []*proto.IcebergPayload_IceRecord{{
			Fields:     protoColumns,
			RecordType: "r",
		}},
	}

	resInsert, err := i.server.SendClientRequest(ctx, recrodInsertRequest)
	if err != nil {
		return fmt.Errorf("failed to insert request: %s", err)
	}

	ingestResponse = resInsert.(*proto.RecordIngestResponse)
	logger.Debugf("Thread[%s]: record inserted successfully: %s", i.server.serverID, ingestResponse.GetResult())
	return nil
}

// validate schema change & evolution and removes null records
func (i *Iceberg) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
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
					valid := isValidTransition(finalSchema[key], detectedIcebergType)
					if !valid {
						return false, fmt.Errorf(
							"failed to validate schema for field[%s] (detected two different types in batch), expected type: %s, detected type: %s",
							key, finalSchema[key], detectedIcebergType,
						)
					}

					if isPromotionRequired(finalSchema[key], detectedIcebergType) {
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
		// One flattener per batch: the internal cache amortizes resolve calls
		// across all records so each column name is resolved only once.
		batchFlattener := typeutils.NewFlattener()
		diffThreadSchema := atomic.Bool{}
		err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
			flattenRecord, err := batchFlattener.Flatten(record.Data)
			if err != nil {
				return fmt.Errorf("failed to flatten record, iceberg writer: %s", err)
			}
			records[idx].Data = flattenRecord
			maps.Copy(records[idx].Data, record.OlakeColumns)
			// if schema difference is not detected, detect schema difference
			if !diffThreadSchema.Load() {
				// when detectChange is true, the function does not modify schema parameter
				if changeDetected, err := detectOrUpdateSchema(records[idx], true, i.schema, i.schema); err != nil {
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

	if !i.stream.NormalizationEnabled() {
		err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
			// JSON-encode original source data before overwriting the map
			dataBytes, err := json.Marshal(record.Data)
			if err != nil {
				return fmt.Errorf("failed to marshal raw data: %s", err)
			}
			records[idx].Data = make(map[string]any, len(record.OlakeColumns)+1+len(i.partitionInfo))
			records[idx].Data[constants.StringifiedData] = string(dataBytes)
			maps.Copy(records[idx].Data, record.OlakeColumns)
			// include partition column values from the original source data so writers
			// can apply the Iceberg partition spec without a separate data buffer.
			// Read with Field (original source key), write with SchemaField (reformatted
			// destination key) so downstream lookups against the Iceberg schema succeed.
			for _, pInfo := range i.partitionInfo {
				if pInfo.Field == constants.OlakeTimestamp {
					continue // _olake_timestamp is already copied via OlakeColumns above
				}
				if v, ok := record.Data[pInfo.Field]; ok {
					records[idx].Data[pInfo.SchemaField] = v
				}
			}
			return nil
		})
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to pre-shape raw records: %s", err)
		}
		return false, records, i.schema, nil
	}

	schemaDifference, recordsSchema, err := extractSchemaFromRecords(ctx, records)
	if err != nil {
		return false, nil, nil, fmt.Errorf("failed to extract schema from records: %s", err)
	}

	if i.options.ApplyFilter {
		filter, isLegacy, filterErr := i.stream.GetFilter()
		if filterErr != nil {
			return false, nil, nil, fmt.Errorf("failed to parse stream filter: %s", filterErr)
		}
		records, err = typeutils.FilterRecords(ctx, records, filter, isLegacy, recordsSchema)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to filter records: %s", err)
		}
	}

	return schemaDifference, records, recordsSchema, nil
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
			} else if isPromotionRequired(oldType, newType) {
				return true
			}
		}
		return false
	}

	// Session-constant context (identifier-field, upsert, partition spec) was
	// captured by the JVM at GET_OR_CREATE_TABLE; EVOLVE_SCHEMA / REFRESH carry
	// only the routing thread_id (schema appended below when evolving).
	request := proto.IcebergPayload{
		Type: proto.IcebergPayload_EVOLVE_SCHEMA,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId: i.options.ThreadID,
		},
	}

	if differentSchema(globalSchemaMap, recordsSchema) {
		logger.Infof("Thread[%s]: evolving schema in iceberg table", i.options.ThreadID)
		for field, fieldType := range recordsSchema {
			request.Metadata.Schema = append(request.Metadata.Schema, &proto.IcebergPayload_SchemaField{
				Key:     field,
				IceType: fieldType,
			})
		}
	} else {
		logger.Debugf("Thread[%s]: refreshing table schema", i.options.ThreadID)
		request.Type = proto.IcebergPayload_REFRESH_TABLE_SCHEMA
	}

	resp, err := i.server.SendClientRequest(ctx, &request)
	if err != nil {
		return false, fmt.Errorf("failed to %s: %s", request.Type.String(), err)
	}

	response := resp.(*proto.RecordIngestResponse).GetResult()

	// only refresh table schema
	schemaAfterEvolution, err := parseSchema(response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema from resp[%s]: %s", response, err)
	}

	i.schema = copySchema(schemaAfterEvolution)
	if err := i.writer.EvolveSchema(ctx, schemaAfterEvolution); err != nil {
		return nil, fmt.Errorf("failed to evolve writer schema: %v", err)
	}

	return schemaAfterEvolution, nil
}

// parsePartitionRegex parses the partition regex and populates the partitionInfo slice
func parsePartitionRegex(pattern string) ([]internal.PartitionInfo, error) {
	// path pattern example: /{col_name, partition_transform}/{col_name, partition_transform}
	// This strictly identifies column name and partition transform entries
	var partitionInfo []internal.PartitionInfo
	if pattern == "" {
		return partitionInfo, nil
	}

	patternRegex := regexp.MustCompile(constants.PartitionRegexIceberg)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no matches found for partition regex: %s", pattern)
	}

	for _, match := range matches {
		if len(match) < 3 {
			continue // We need at least 3 matches: full match, column name, transform
		}

		colName := strings.Replace(strings.TrimSpace(strings.Trim(match[1], `'"`)), "now()", constants.OlakeTimestamp, 1)
		transform := strings.TrimSpace(strings.Trim(match[2], `'"`))

		// Append to ordered slice to preserve partition order.
		// SchemaField is reformatted once here so all consumers use the consistent
		// destination column name without scattering utils.Reformat() calls.
		partitionInfo = append(partitionInfo, internal.PartitionInfo{
			Field:       colName,
			SchemaField: colName,
			Transform:   transform,
		})
	}

	return partitionInfo, nil
}

// isValidTransition checks if type transition is valid using lookup table
func isValidTransition(oldType, newType string) bool {
	if oldType == newType {
		return true
	}
	if transitions, ok := validTypeTransitions[oldType]; ok {
		if transitions[newType] {
			return true
		}
	}

	return getCommonAncestorType(oldType, newType) == oldType
}

// isPromotionRequired checks if promotion is needed using lookup table
func isPromotionRequired(oldType, newType string) bool {
	if transitions, ok := promotionTransitions[oldType]; ok {
		return transitions[newType]
	}

	return false
}

// drop streams required for clear destination
func (i *Iceberg) DropStreams(ctx context.Context, tables []types.StreamInterface) error {
	if len(tables) == 0 {
		logger.Info("No streams selected for clearing Iceberg destination, skipping operation")
		return nil
	}
	logger.Infof("Starting Clear Iceberg destination for %d selected tables", len(tables))

	threadID := "iceberg_destination_drop"
	for _, stream := range tables {
		destDB := stream.GetDestinationDatabase(&i.config.IcebergDatabase)
		destTable := stream.GetDestinationTable()
		dropTable := fmt.Sprintf("%s.%s", destDB, destTable)

		logger.Infof("Dropping Iceberg table: %s", dropTable)

		request := proto.IcebergPayload{
			Type: proto.IcebergPayload_DROP_TABLE,
			Metadata: &proto.IcebergPayload_Metadata{
				DestTableName: dropTable,
				ThreadId:      threadID,
				// Namespace is encoded inside DropTable (db.table) and parsed Java-side.
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
	var server *serverInstance
	var icebergConfig *Config
	destination.RegisteredWriters[types.Iceberg] = func(config any) (destination.Writer, func(ctx context.Context), error) {
		if icebergConfig != nil || server != nil {
			// for already initialized writer, return the same server and config instance
			return &Iceberg{
				config: icebergConfig,
				server: server,
			}, nil, nil
		}

		icebergConfig = &Config{}
		// unmarshal config according to iceberg config struct
		err := utils.Unmarshal(config, icebergConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal iceberg config: %w", err)
		}

		server, err = startServer(icebergConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to start iceberg server: %w", err)
		}

		return &Iceberg{
				config: icebergConfig,
				server: server,
			}, func(ctx context.Context) {
				server.Shutdown(ctx)
			}, nil
	}
}
