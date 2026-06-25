package legacywriter

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

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

// newMetadata builds the per-request Metadata. The session-constant context
// (namespace, upsert, partition spec, identifier-field) was already captured by
// the JVM on the GET_OR_CREATE_TABLE payload during Setup, so RECORDS / COMMIT
// payloads carry only the thread_id the JVM routes on (callers add schema/payload).


func (w *LegacyWriter) Write(ctx context.Context, records []types.RawRecord) error {
	protoSchema := make([]*proto.SchemaField, 0, len(w.schema))
	for field, dType := range w.schema {
		protoSchema = append(protoSchema, &proto.SchemaField{
			Key:     field,
			IceType: dType,
		})
	}

	protoRecords := make([]*proto.SendRecordsRequest_IceRecord, 0, len(records))
	for _, record := range records {
		if record.Data == nil {
			continue
		}

		protoColumnsValue := make([]*proto.SendRecordsRequest_FieldValue, 0, len(protoSchema))
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

		if len(protoColumnsValue) > 0 {
			protoRecords = append(protoRecords, &proto.SendRecordsRequest_IceRecord{
				Fields:     protoColumnsValue,
				RecordType: record.OlakeColumns[constants.OpType].(string),
			})
		}
	}

	if len(protoRecords) == 0 {
		logger.Debugf("Thread[%s]: no record found in batch", w.options.ThreadID)
		return nil
	}

	request := &proto.SendRecordsRequest{
		ThreadId: w.options.ThreadID,
		Schema:   protoSchema,
		Records:  protoRecords,
	}

	reqCtx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	res, err := w.server.SendClientRequest(reqCtx, request)
	if err != nil {
		return fmt.Errorf("failed to send batch: %s", err)
	}

	ingestResponse := res.(*proto.SendRecordsResponse)
	logger.Debugf("Thread[%s]: sent batch to Iceberg server, response: %s", w.options.ThreadID, ingestResponse.GetResult())

	return nil
}

func (w *LegacyWriter) EvolveSchema(_ context.Context, newSchema map[string]string) error {
	w.schema = newSchema

	return nil
}

func (w *LegacyWriter) Close(ctx context.Context, finalMetadataState any) error {
	// Commit payload from CDC/driver only: e.g. {"captured_cdc_pos":"0/123ABC"}
	var payloadStr string
	if finalMetadataState != nil {
		payloadBytes, _ := json.Marshal(finalMetadataState)
		payloadStr = string(payloadBytes)
	}

	request := &proto.CommitRequest{
		ThreadId: w.options.ThreadID,
		Payload:  payloadStr,
	}

	// Send commit request with timeout
	ctx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	res, err := w.server.SendClientRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to send commit message: %s", err)
	}

	ingestResponse := res.(*proto.CommitResponse)
	logger.Debugf("Thread[%s]: Sent commit message: %s", w.options.ThreadID, ingestResponse.GetResult())

	return nil
}

func (w *LegacyWriter) Cleanup() error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &proto.CloseSessionRequest{
		ThreadId: w.options.ThreadID,
	}

	if _, err := w.server.SendClientRequest(cleanupCtx, req); err != nil {
		return fmt.Errorf("failed to close iceberg session: %s", err)
	}
	return nil
}

// RawDataColumnBuffer is used by the connection health check in iceberg.go to build proto
// field values for a synthetic non-normalized test record.
// Normal write-path records are pre-shaped by FlattenAndCleanData and use the standard field loop in Write.
func RawDataColumnBuffer(record types.RawRecord, protoSchema []*proto.SchemaField) ([]*proto.SendRecordsRequest_FieldValue, error) {
	// 1. Start with a copy of OlakeColumns (already prepared upstream)
	dataMap := make(map[string]any, len(record.OlakeColumns)+1)
	maps.Copy(dataMap, record.OlakeColumns)

	// 2. Add stringified data as a single column
	bytesData, err := json.Marshal(record.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data in normalization: %s", err)
	}
	dataMap[constants.StringifiedData] = string(bytesData)

	// 3. Build final proto values dynamically using SAME logic as normalized path
	protoColumnsValue := make([]*proto.SendRecordsRequest_FieldValue, 0, len(protoSchema))

	for _, field := range protoSchema {
		value, ok := dataMap[field.Key]
		if !ok {
			protoColumnsValue = append(protoColumnsValue, nil)
			continue
		}

		fv, err := toProtoFieldValue(field.IceType, value)
		if err != nil {
			return nil, fmt.Errorf("field[%s]: %s", field.Key, err)
		}

		protoColumnsValue = append(protoColumnsValue, fv)
	}

	return protoColumnsValue, nil
}

func toProtoFieldValue(iceType string, val any) (*proto.SendRecordsRequest_FieldValue, error) {
	switch iceType {
	case "boolean":
		v, err := typeutils.ReformatBool(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] as bool value: %s", val, err)
		}
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_BoolValue{BoolValue: v},
		}, nil

	case "int":
		v, err := typeutils.ReformatInt32(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as int32 value: %s", val, val, err)
		}
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_IntValue{IntValue: v},
		}, nil

	case "long":
		v, err := typeutils.ReformatInt64(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as long value: %s", val, val, err)
		}
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_LongValue{LongValue: v},
		}, nil

	case "float":
		v, err := typeutils.ReformatFloat32(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as float32 value: %s", val, val, err)
		}
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_FloatValue{FloatValue: v},
		}, nil

	case "double":
		v, err := typeutils.ReformatFloat64(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as double value: %s", val, val, err)
		}
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_DoubleValue{DoubleValue: v},
		}, nil

	case "timestamptz":
		t, err := typeutils.ReformatDate(val, true)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as timestamp value: %s", val, val, err)
		}
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_LongValue{LongValue: t.UnixMilli()},
		}, nil

	default:
		return &proto.SendRecordsRequest_FieldValue{
			Value: &proto.SendRecordsRequest_FieldValue_StringValue{
				StringValue: fmt.Sprintf("%v", val),
			},
		}, nil
	}
}
