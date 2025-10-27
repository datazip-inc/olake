package arrow

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
)

type Fanout struct {
	PartitionInfo []destination.PartitionInfo
	writers       sync.Map
	mu            sync.Mutex
	ctx           context.Context

	schema        map[string]string
	Normalization bool
}

func NewFanoutWriter(ctx context.Context, p []destination.PartitionInfo, schema map[string]string) *Fanout {
	return &Fanout{
		PartitionInfo: p,
		schema:        schema,
		ctx:           ctx,
	}
}

func (f *Fanout) getOrCreateRollingWriter(partitionKey string) *RollingWriter {
	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, ok := f.writers.Load(partitionKey); ok {
		if writer, ok := existing.(*RollingWriter); ok {
			return writer
		}

		return nil
	}

	writer := NewRollingWriter(context.Background(), partitionKey, "data")
	f.writers.Store(partitionKey, writer)

	return writer
}

func (f *Fanout) createPartitionKey(record types.RawRecord) (string, error) {
	paths := make([]string, 0, len(f.PartitionInfo))

	for _, partition := range f.PartitionInfo {
		field, transform := partition.Field, partition.Transform
		colType := f.schema[field]
		transformedVal, err := transformValue(record.Data[field], transform, colType)
		if err != nil {
			return "", err
		}

		colPath := constructColPath(transformedVal.(string), field, transform)
		paths = append(paths, colPath)
	}

	partitionKey := strings.Join(paths, "/")
	return partitionKey, nil
}

func transformValue(val any, transform string, colType string) (any, error) {
	var tf Transform

	switch transform {
	case "identity":
		tf = IdentityTransform{}
	case "year":
		tf = YearTransform{}
	case "month":
		tf = MonthTransform{}
	case "day":
		tf = DayTransform{}
	case "hour":
		tf = HourTransform{}
	case "void":
		tf = VoidTransform{}
	default:
		if strings.HasPrefix(transform, "bucket") {
			num, err := strconv.Atoi(transform[7 : len(transform)-1])
			if err != nil {
				return nil, err
			}

			tf = BucketTransform{
				NumBuckets: num,
			}
		} else if strings.HasPrefix(transform, "truncate") {
			num, err := strconv.Atoi(transform[9 : len(transform)-1])
			if err != nil {
				return nil, err
			}

			tf = TruncateTransform{
				Width: num,
			}
		} else {
			return nil, fmt.Errorf("unknown partition transformation: %v", transform)
		}
	}

	if tf.canTransform(colType) {
		v, err := tf.apply(val, colType)
		if err != nil {
			return nil, err
		} else {
			return v, nil
		}
	} else {
		return nil, fmt.Errorf("cannot apply transformation %v for column type: %v", transform, colType)
	}
}

func (f *Fanout) partition(records []types.RawRecord) (map[string][]types.RawRecord, []types.RawRecord, error) {
	partitionedData := make(map[string][]types.RawRecord)
	deleteRecords := make([]types.RawRecord, 0)

	for _, rec := range records {
		if rec.OperationType == "d" || rec.OperationType == "u" {
			deleteRecords = append(deleteRecords, rec)
		}

		pKey, err := f.createPartitionKey(rec)
		if err != nil {
			return nil, nil, err
		}
		partitionedData[pKey] = append(partitionedData[pKey], rec)
	}

	return partitionedData, deleteRecords, nil
}

func (f *Fanout) Write(ctx context.Context, records []types.RawRecord, fields []arrow.Field) ([]*FileUploadData, []types.RawRecord, error) {
	partitionedData, deleteRecords, err := f.partition(records)
	if err != nil {
		return nil, nil, err
	}

	uploadDataList := make([]*FileUploadData, 0, len(partitionedData))

	for partitionKey, rawData := range partitionedData {
		rec, err := CreateArrowRecordWithFields(rawData, fields, f.Normalization)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create arrow record for partition %s: %w", partitionKey, err)
		}

		writer := f.getOrCreateRollingWriter(partitionKey)
		uploadData, err := writer.Write(rec)
		if err != nil {
			return uploadDataList, nil, fmt.Errorf("failed to write to partition %s: %w", partitionKey, err)
		}
		if uploadData != nil {
			uploadDataList = append(uploadDataList, uploadData)
		}
	}

	return uploadDataList, deleteRecords, nil
}

func (f *Fanout) Close() ([]*FileUploadData, error) {
	uploadDataList := make([]*FileUploadData, 0)
	var lastErr error

	f.writers.Range(func(key, value interface{}) bool {
		if writer, ok := value.(*RollingWriter); ok {
			if uploadData, err := writer.Close(); err != nil {
				lastErr = err
			} else if uploadData != nil {
				uploadDataList = append(uploadDataList, uploadData)
			}
		}
		return true
	})

	return uploadDataList, lastErr
}
