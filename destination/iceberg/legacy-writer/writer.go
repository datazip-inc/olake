package legacywriter

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type LegacyWriter struct {
	options     *destination.Options
	schema      map[string]string
	stream      types.StreamInterface
	server      internal.ServerClient
	indexThread *types.StreamIndexThread
	upsertMode  bool
}

func New(options *destination.Options, schema map[string]string, stream types.StreamInterface, server internal.ServerClient, upsertMode bool) *LegacyWriter {
	return &LegacyWriter{
		options:     options,
		schema:      schema,
		stream:      stream,
		server:      server,
		upsertMode:  upsertMode,
		indexThread: types.NewStreamIndexThread(options.TableIndex),
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

	// FlattenAndCleanData pre-shapes record.Data for both normalization modes:
	//   normalization=true:  typed columns + OlakeColumns merged in
	//   normalization=false: StringifiedData + OlakeColumns + partition columns
	// A single loop over protoSchema covers both cases.
	protoRecords := make([]*proto.IcebergPayload_IceRecord, 0, len(records))
	sentRecords := make([]types.RawRecord, 0, len(records))
	for _, record := range records {
		if record.Data == nil {
			continue
		}

		protoColumnsValue := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))
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

		if len(protoColumnsValue) == 0 {
			continue
		}

		opType := record.OlakeColumns[constants.OpType].(string)
		iceRecord := &proto.IcebergPayload_IceRecord{
			Fields:     protoColumnsValue,
			RecordType: opType,
		}

		// check if we need to write pos for the current record
		if w.indexThread != nil && (opType != "r" && opType != "c") {
			olakeID := record.OlakeColumns[constants.OlakeID].(string)
			previous, found, err := w.indexThread.Lookup(olakeID)
			if err != nil {
				return fmt.Errorf("failed to look up row[%s] in index: %s", olakeID, err)
			}
			if found {
				filePath := previous.FilePath
				position := previous.Position
				iceRecord.DeleteFilePath = &filePath
				iceRecord.DeletePosition = &position
			}
		}

		protoRecords = append(protoRecords, iceRecord)
		sentRecords = append(sentRecords, record)
	}

	if len(protoRecords) == 0 {
		logger.Debugf("Thread[%s]: no record found in batch", w.options.ThreadID)
		return nil
	}

	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_RECORDS,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId: w.options.ThreadID,
			Schema:   protoSchema,
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

	if w.indexThread != nil {
		for _, fileMap := range ingestResponse.GetFilePositionMaps() {
			logger.Debugf("Thread[%s]: file position map: %s (%d ranges)", w.options.ThreadID, fileMap.GetFilePath(), len(fileMap.GetRanges()))
			for _, r := range fileMap.GetRanges() {
				logger.Debugf("Thread[%s]:   range startIdx:%d startPos:%d count:%d", w.options.ThreadID, r.GetBatchStartIdx(), r.GetStartPosition(), r.GetCount())
				for i := int32(0); i < r.GetCount(); i++ {
					rec := sentRecords[r.GetBatchStartIdx()+i]
					olakeID := rec.OlakeColumns[constants.OlakeID].(string)
					// Soft-delete keeps the tombstone row on disk, so the index must
					// point at the new location just like any other write (matches arrow).
					w.indexThread.Put(olakeID, types.RowLocation{
						FilePath: fileMap.GetFilePath(),
						Position: r.GetStartPosition() + int64(i),
					})
				}
			}
		}
	}

	return nil
}

// Abort drops the buffered index entries of a thread that never reaches a
// commit. Nothing was written to the index, so releasing the buffer is the
// entire rollback.
func (w *LegacyWriter) Abort() {
	w.indexThread = nil
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

	request := &proto.IcebergPayload{
		Type: proto.IcebergPayload_COMMIT,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId: w.options.ThreadID,
			Payload:  payloadStr,
		},
	}

	if w.indexThread != nil {
		indexBaseSnapshotID, err := w.options.TableIndex.LastCommittedSnapshot()
		if err != nil {
			return fmt.Errorf("failed to get last committed snapshot ID: %s", err)
		}

		request.Metadata.BaseSnapshotId = &indexBaseSnapshotID
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

	// ingestResponse.SnapshotId is 0 if there are external snapshots present or there is no records to commit
	// in that case we will not apply stream index, next sync run will create indexes
	if w.indexThread != nil && ingestResponse.SnapshotId != 0 {
		batch := w.indexThread
		w.indexThread = nil
		if err := w.options.TableIndex.Commit(batch, &ingestResponse.SnapshotId); err != nil {
			return fmt.Errorf("failed to apply stream index: %s", err)
		}
	}

	return nil
}

// RawDataColumnBuffer is used by the connection health check in iceberg.go to build proto
// field values for a synthetic non-normalized test record.
// Normal write-path records are pre-shaped by FlattenAndCleanData and use the standard field loop in Write.
func RawDataColumnBuffer(record types.RawRecord, protoSchema []*proto.IcebergPayload_SchemaField) ([]*proto.IcebergPayload_IceRecord_FieldValue, error) {
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
	protoColumnsValue := make([]*proto.IcebergPayload_IceRecord_FieldValue, 0, len(protoSchema))

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

func toProtoFieldValue(iceType string, val any) (*proto.IcebergPayload_IceRecord_FieldValue, error) {
	switch iceType {
	case "boolean":
		v, err := typeutils.ReformatBool(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] as bool value: %s", val, err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: v},
		}, nil

	case "int":
		v, err := typeutils.ReformatInt32(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as int32 value: %s", val, val, err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_IntValue{IntValue: v},
		}, nil

	case "long":
		v, err := typeutils.ReformatInt64(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as long value: %s", val, val, err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: v},
		}, nil

	case "float":
		v, err := typeutils.ReformatFloat32(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as float32 value: %s", val, val, err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_FloatValue{FloatValue: v},
		}, nil

	case "double":
		v, err := typeutils.ReformatFloat64(val)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as double value: %s", val, val, err)
		}
		return &proto.IcebergPayload_IceRecord_FieldValue{
			Value: &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: v},
		}, nil

	case "timestamptz":
		t, err := typeutils.ReformatDate(val, true)
		if err != nil {
			return nil, fmt.Errorf("failed to reformat rawValue[%v] of type[%T] as timestamp value: %s", val, val, err)
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
