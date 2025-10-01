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

	S3Config *S3Config
}

func NewFanoutWriter(ctx context.Context, p []destination.PartitionInfo, schema map[string]string) *Fanout {
	return &Fanout{
		PartitionInfo: p,
		schema:        schema,
		ctx:           ctx,
	}
}

func (f *Fanout) getOrCreateRollingDataWriter(partitionKey string) *RollingDataWriter {
	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, ok := f.writers.Load(partitionKey); ok {
		if writer, ok := existing.(*RollingDataWriter); ok {
			return writer
		}

		return nil
	}

	// Use background context to avoid cancellation issues during Close()
	// The original context f.ctx may be cancelled before Close() is called
	writer := NewRollingDataWriter(context.Background(), partitionKey)
	writer.S3Config = f.S3Config

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

func (f *Fanout) partition(records []types.RawRecord) (map[string][]types.RawRecord, error) {
	partitionedData := make(map[string][]types.RawRecord)

	for _, rec := range records {
		pKey, err := f.createPartitionKey(rec)
		if err != nil {
			return nil, err
		}
		partitionedData[pKey] = append(partitionedData[pKey], rec)
	}

	return partitionedData, nil
}

func (f *Fanout) Write(ctx context.Context, records []types.RawRecord, fields []arrow.Field) ([]string, error) {
	partitionedData, err := f.partition(records)
	if err != nil {
		return nil, err
	}

	outputFilePaths := make([]string, 0, len(partitionedData))

	for partitionKey, rawData := range partitionedData {
		rec, err := CreateArrowRecordWithFields(rawData, fields, f.Normalization)
		if err != nil {
			return nil, fmt.Errorf("failed to create arrow record for partition %s: %w", partitionKey, err)
		}

		writer := f.getOrCreateRollingDataWriter(partitionKey)
		filePath, err := writer.Write(rec)
		if err != nil {
			return outputFilePaths, fmt.Errorf("failed to write to partition %s: %w", partitionKey, err)
		}
		if filePath != "" {
			outputFilePaths = append(outputFilePaths, filePath)
		}
	}

	return outputFilePaths, nil
}

func (f *Fanout) Close() ([]string, error) {
	outputFilePaths := make([]string, 0)
	var lastErr error

	f.writers.Range(func(key, value interface{}) bool {
		if writer, ok := value.(*RollingDataWriter); ok {
			if filePath, err := writer.Close(); err != nil {
				lastErr = err
			} else if filePath != "" {
				outputFilePaths = append(outputFilePaths, filePath)
			}
		}
		return true
	})

	return outputFilePaths, lastErr
}
