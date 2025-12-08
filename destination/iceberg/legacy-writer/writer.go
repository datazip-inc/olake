package legacywriter

import (
	"context"
	"fmt"
	"time"

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
	server  internal.LegacyServerClient
}

func New(options *destination.Options, schema map[string]string, stream types.StreamInterface, server internal.LegacyServerClient) *LegacyWriter {
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
		if !w.stream.NormalizationEnabled() {
			protoCols, err := internal.RawDataColumnBuffer(record, protoSchema)
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
	reqCtx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := w.server.SendClientRequest(reqCtx, request)
	if err != nil {
		return fmt.Errorf("failed to send batch: %s", err)
	}

	logger.Debugf("Thread[%s]: sent batch to Iceberg server, response: %s", w.options.ThreadID, res)

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
	ctx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	res, err := w.server.SendClientRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to send commit message: %s", err)
	}

	logger.Debugf("Thread[%s]: Sent commit message: %s", w.options.ThreadID, res)

	return nil
}
