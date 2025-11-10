package arrow

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/types"
)

type Fanout struct {
	PartitionInfo []PartitionInfo
	writers       sync.Map // stores both data and delete writers, keyed by "data:partition" or "delete:partition"
	mu            sync.Mutex

	schema        map[string]string
	Normalization bool
	FilenameGen   FilenameGenerator
	FieldIdFunc   func(context.Context, string) (int, error) // Function to get field IDs
}

func NewFanoutWriter(p []PartitionInfo, schema map[string]string) *Fanout {
	return &Fanout{
		PartitionInfo: p,
		schema:        schema,
	}
}

func (f *Fanout) getOrCreateRollingWriter(partitionKey string, fileType string) *RollingWriter {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := fileType + ":" + partitionKey

	if existing, ok := f.writers.Load(key); ok {
		if writer, ok := existing.(*RollingWriter); ok {
			return writer
		}
	}

	writer := NewRollingWriter(partitionKey, fileType)
	writer.FilenameGen = f.FilenameGen
	f.writers.Store(key, writer)

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

	if !tf.canTransform(colType) {
		return nil, fmt.Errorf("cannot apply transformation %v for column type: %v", transform, colType)
	}

	v, err := tf.apply(val, colType)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (f *Fanout) partition(records []types.RawRecord) (map[string][]types.RawRecord, map[string][]types.RawRecord, error) {
	partitionedData := make(map[string][]types.RawRecord)
	// Track delete records by partition to maintain partition scope
	deleteRecordsByPartition := make(map[string][]types.RawRecord)

	for _, rec := range records {
		pKey, err := f.createPartitionKey(rec)
		if err != nil {
			return nil, nil, err
		}

		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			deleteRecordsByPartition[pKey] = append(deleteRecordsByPartition[pKey], rec)
		}

		partitionedData[pKey] = append(partitionedData[pKey], rec)
	}

	return partitionedData, deleteRecordsByPartition, nil
}

func (f *Fanout) Write(ctx context.Context, records []types.RawRecord, fields []arrow.Field) ([]*FileUploadData, error) {
	partitionedData, deleteRecordsByPartition, err := f.partition(records)
	if err != nil {
		return nil, err
	}

	uploadDataList := make([]*FileUploadData, 0, len(partitionedData))

	// Write data files
	for partitionKey, rawData := range partitionedData {
		rec, err := CreateArrowRecordWithFields(rawData, fields, f.Normalization)
		if err != nil {
			return nil, fmt.Errorf("failed to create arrow record for partition %s: %w", partitionKey, err)
		}

		writer := f.getOrCreateRollingWriter(partitionKey, "data")
		uploadData, err := writer.Write(rec)
		if err != nil {
			return uploadDataList, fmt.Errorf("failed to write to partition %s: %w", partitionKey, err)
		}
		if uploadData != nil {
			uploadDataList = append(uploadDataList, uploadData)
		}
	}

	// Write delete files for each partition with delete records
	if len(deleteRecordsByPartition) > 0 {
		if f.FieldIdFunc == nil {
			return uploadDataList, fmt.Errorf("FieldIdFunc not set on Fanout writer")
		}

		fieldId, err := f.FieldIdFunc(ctx, "_olake_id")
		if err != nil {
			return uploadDataList, fmt.Errorf("failed to get field ID for _olake_id: %w", err)
		}

		for partitionKey, deleteRecords := range deleteRecordsByPartition {
			deleteWriter := f.getOrCreateRollingWriter(partitionKey, "delete")
			deleteWriter.FieldId = fieldId

			deletes := ExtractDeleteRecords(deleteRecords)

			rec, err := CreateDelArrRecord(deletes, fieldId)
			if err != nil {
				return uploadDataList, fmt.Errorf("failed to create delete record for partition %s: %w", partitionKey, err)
			}

			defer rec.Release()

			uploadData, err := deleteWriter.Write(rec)
			if err != nil {
				return uploadDataList, fmt.Errorf("failed to write delete record for partition %s: %w", partitionKey, err)
			}
			if uploadData != nil {
				uploadDataList = append(uploadDataList, uploadData)
			}
		}
	}

	return uploadDataList, nil
}

func (f *Fanout) Close() ([]*FileUploadData, error) {
	uploadDataList := make([]*FileUploadData, 0)
	var lastErr error

	// Close all writers (both data and delete)
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
