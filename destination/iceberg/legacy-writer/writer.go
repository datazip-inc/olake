package legacywriter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type LegacyWriter struct {
	options *destination.Options
	schema  map[string]string
	stream  types.StreamInterface
	server  internal.ServerClient
}

func New(options *destination.Options, schema map[string]string, stream types.StreamInterface, server internal.ServerClient) *LegacyWriter {
	return &LegacyWriter{
		options: options,
		schema:  schema,
		stream:  stream,
		server:  server,
	}
}

func (w *LegacyWriter) Write(ctx context.Context, records []types.RawRecord) error {
	protoSchema := make([]*proto.IcebergPayload_SchemaField, 0, len(w.schema))
	for field, dType := range w.schema {
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
		var err error
		if !w.stream.NormalizationEnabled() {
			protoColumnsValue, err = RawDataColumnBuffer(record, protoSchema)
			if err != nil {
				return fmt.Errorf("failed to create raw data column buffer: %s", err)
			}
		} else {
			for _, field := range protoSchema {
				val, exist := record.Data[field.Key]
				if !exist {
					protoColumnsValue = append(protoColumnsValue, nil)
					continue
				}
				fv, err := toProtoFieldValue(field.IceType, val)
				if err != nil {
					return fmt.Errorf("field[%s]: %s", field.Key, err)
				}

				protoColumnsValue = append(protoColumnsValue, fv)
			}
		}

		if len(protoColumnsValue) > 0 {
			protoRecords = append(protoRecords, &proto.IcebergPayload_IceRecord{
				Fields:     protoColumnsValue,
				RecordType: record.OlakeColumns[constants.OpType].(string),
			})
		}
	}

	if len(protoRecords) == 0 {
		logger.Debugf("Thread[%s]: no record found in batch", w.options.ThreadID)
		return nil
	}

	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
			Schema:        protoSchema,
		},
		Records: protoRecords,
	}

	// Send to gRPC server with timeout
	reqCtx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	// Send the batch to the server
	res, err := w.server.SendClientRequest(reqCtx, request)
	if err != nil {
		return fmt.Errorf("failed to send batch: %s", err)
	}

	ingestResponse := res.(*proto.RecordIngestResponse)
	logger.Debugf("Thread[%s]: sent batch to Iceberg server, response: %s", w.options.ThreadID, ingestResponse.GetResult())

	return nil
}

func (w *LegacyWriter) EvolveSchema(_ context.Context, newSchema map[string]string) error {
	w.schema = newSchema

	return nil
}

func (w *LegacyWriter) Close(ctx context.Context) error {
	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_COMMIT,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:      w.server.ServerID(),
			DestTableName: w.stream.GetDestinationTable(),
		},
	}

	// Send commit request with timeout
	ctx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	res, err := w.server.SendClientRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to send commit message: %s", err)
	}

	ingestResponse := res.(*proto.RecordIngestResponse)
	logger.Debugf("Thread[%s]: Sent commit message: %s", w.options.ThreadID, ingestResponse.GetResult())

	return nil
}

func RawDataColumnBuffer(record types.RawRecord, protoSchema []*proto.IcebergPayload_SchemaField) ([]*proto.IcebergPayload_IceRecord_FieldValue, error) {
	// 1. Start with a copy of OlakeColumns (already prepared upstream)
	dataMap := make(map[string]any, len(record.OlakeColumns)+1)

	for k, v := range record.OlakeColumns {
		dataMap[k] = v
	}

	// 2. Add stringified data as a single column
	bytesData, err := json.Marshal(record.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data in normalization: %s", err)
	}
	dataMap[constants.StringifiedData] = string(bytesData)

	// 3. Build final proto values dynamically using SAME logic as normalized path
	protoColumnsValue := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))

	for _, field := range protoSchema {
		value, ok := dataMap[field.Key]
		if !ok {
			protoColumnsValue = append(protoColumnsValue, nil)
			continue
		}

		fv, err := toProtoFieldValue(field.IceType, value)
		if err != nil {
			return nil, fmt.Errorf("field[%s]: %w", field.Key, err)
		}

		protoColumnsValue = append(protoColumnsValue, fv)
	}

	return protoColumnsValue, nil
}

func toProtoFieldValue(iceType string, val any) (*proto.IcebergPayload_IceRecord_FieldValue, error) {

	switch iceType {

	case "boolean":
		v, err := typeutils.ReformatBool(val)
		if err != nil {
			return nil, fmt.Errorf("bool convert: %w", err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: v},
		}, nil

	case "int":
		v, err := typeutils.ReformatInt32(val)
		if err != nil {
			return nil, fmt.Errorf("int convert: %w", err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_IntValue{IntValue: v},
		}, nil

	case "long":
		v, err := typeutils.ReformatInt64(val)
		if err != nil {
			return nil, fmt.Errorf("long convert: %w", err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: v},
		}, nil

	case "float":
		v, err := typeutils.ReformatFloat32(val)
		if err != nil {
			return nil, fmt.Errorf("float convert: %w", err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_FloatValue{FloatValue: v},
		}, nil

	case "double":
		v, err := typeutils.ReformatFloat64(val)
		if err != nil {
			return nil, fmt.Errorf("double convert: %w", err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: v},
		}, nil

	case "timestamptz":
		t, err := typeutils.ReformatDate(val, true)
		if err != nil {
			return nil, fmt.Errorf("timestamp convert: %w", err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: t.UnixMilli()},
		}, nil

	default:
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{
				StringValue: fmt.Sprintf("%v", val),
			},
		}, nil
	}
}
