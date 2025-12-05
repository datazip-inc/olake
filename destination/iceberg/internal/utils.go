package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

type ArrowServerClient interface {
	SendArrowRequest(ctx context.Context, reqPayload *proto.ArrowPayload) (*proto.ArrowIngestResponse, error)
	ServerID() string
}

type LegacyServerClient interface {
	SendClientRequest(ctx context.Context, reqPayload *proto.IcebergPayload) (string, error)
	ServerID() string
}

// PartitionInfo represents a Iceberg partition column with its transform, preserving order
type PartitionInfo struct {
	Field     string
	Transform string
}

func RawDataColumnBuffer(record types.RawRecord, protoSchema []*proto.IcebergPayload_SchemaField) ([]*proto.IcebergPayload_IceRecord_FieldValue, error) {
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
