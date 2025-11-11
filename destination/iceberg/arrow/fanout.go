package arrow

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

type UploadFunc func(context.Context, *FileUploadData) error

type Fanout struct {
	PartitionInfo []PartitionInfo
	writers       sync.Map 
	mu            sync.Mutex

	schema        map[string]string
	Normalization bool
	FilenameGen   FilenameGenerator
	FieldIdFunc   func(context.Context, string) (int, error)
	UploadFunc    UploadFunc
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

func (f *Fanout) partition(records []types.RawRecord) (map[string][]types.RawRecord, map[string][]types.RawRecord, error) {
	partitionedData := make(map[string][]types.RawRecord)
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

func (f *Fanout) Write(ctx context.Context, records []types.RawRecord, fields []arrow.Field) error {
	partitionedData, deleteRecordsByPartition, err := f.partition(records)
	if err != nil {
		return err
	}

	// IMPORTANT: Process delete files FIRST, then data files
	// This ensures deletes are applied before inserts for CDC operations
	if len(deleteRecordsByPartition) > 0 {
		if f.FieldIdFunc == nil {
			return fmt.Errorf("FieldIdFunc not set on Fanout writer")
		}

		fieldId, err := f.FieldIdFunc(ctx, constants.OlakeID)
		if err != nil {
			return fmt.Errorf("failed to get field ID for %s: %w", constants.OlakeID, err)
		}

		for partitionKey, deleteRecords := range deleteRecordsByPartition {
			deleteWriter := f.getOrCreateRollingWriter(partitionKey, "delete")
			deleteWriter.FieldId = fieldId

			deletes := ExtractDeleteRecords(deleteRecords)

			rec, err := CreateDelArrRecord(deletes, fieldId)
			if err != nil {
				return fmt.Errorf("failed to create delete record for partition %s: %w", partitionKey, err)
			}

			defer rec.Release()

			uploadData, err := deleteWriter.Write(rec)
			if err != nil {
				return fmt.Errorf("failed to write delete record for partition %s: %w", partitionKey, err)
			}
			if uploadData != nil {
				if err := f.UploadFunc(ctx, uploadData); err != nil {
					return fmt.Errorf("failed to upload delete file for partition %s: %w", partitionKey, err)
				}
			}
		}
	}

	// Now process data files after deletes
	for partitionKey, rawData := range partitionedData {
		rec, err := CreateArrowRecordWithFields(rawData, fields, f.Normalization)
		if err != nil {
			return fmt.Errorf("failed to create arrow record for partition %s: %w", partitionKey, err)
		}

		writer := f.getOrCreateRollingWriter(partitionKey, "data")
		uploadData, err := writer.Write(rec)
		if err != nil {
			return fmt.Errorf("failed to write to partition %s: %w", partitionKey, err)
		}
		if uploadData != nil {
			if err := f.UploadFunc(ctx, uploadData); err != nil {
				return fmt.Errorf("failed to upload data file for partition %s: %w", partitionKey, err)
			}
		}
	}

	return nil
}

func (f *Fanout) Close(ctx context.Context) error {
	var lastErr error

	// Collect all writers first, separated by type
	deleteWriters := make(map[interface{}]*RollingWriter)
	dataWriters := make(map[interface{}]*RollingWriter)

	f.writers.Range(func(key, value interface{}) bool {
		if writer, ok := value.(*RollingWriter); ok {
			if writer.fileType == "delete" {
				deleteWriters[key] = writer
			} else {
				dataWriters[key] = writer
			}
		}
		return true
	})

	// IMPORTANT: Close delete writers first, then data writers
	// This ensures proper CDC operation order (deletes before inserts)
	for _, writer := range deleteWriters {
		uploadData, err := writer.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if uploadData != nil {
			if err := f.UploadFunc(ctx, uploadData); err != nil {
				lastErr = fmt.Errorf("failed to upload delete file: %w", err)
			}
		}
	}

	for _, writer := range dataWriters {
		uploadData, err := writer.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if uploadData != nil {
			if err := f.UploadFunc(ctx, uploadData); err != nil {
				lastErr = fmt.Errorf("failed to upload data file: %w", err)
			}
		}
	}

	return lastErr
}
