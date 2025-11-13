package arrow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

type IcebergOperations interface {
	GenerateFilename(ctx context.Context) (string, error)
	GetFieldId(ctx context.Context, fieldName string) (int, error)
	UploadFile(ctx context.Context, uploadData *FileUploadData) error
}

type Fanout struct {
	PartitionInfo []PartitionInfo
	writers       sync.Map

	schema            map[string]string
	fieldIds          map[string]int
	icebergSchemaJSON string
	Normalization     bool

	ops IcebergOperations
}

func NewFanoutWriter(p []PartitionInfo, schema map[string]string, fieldIds map[string]int, icebergSchemaJSON string, ops IcebergOperations) *Fanout {
	return &Fanout{
		PartitionInfo:     p,
		schema:            schema,
		fieldIds:          fieldIds,
		icebergSchemaJSON: icebergSchemaJSON,
		ops:               ops,
	}
}

func (f *Fanout) getOrCreateRollingWriter(partitionKey string, fileType string) *RollingWriter {
	// differentiating data and delete file writers, "data:pk" : *writer, "delete:pk" : *writer
	key := fileType + ":" + partitionKey

	if existing, ok := f.writers.Load(key); ok {
		return existing.(*RollingWriter)
	}

	writer := NewRollingWriter(partitionKey, fileType, f.ops)
	if fileType == "data" {
		writer.IcebergSchemaJSON = f.icebergSchemaJSON
	}

	w, ok := f.writers.LoadOrStore(key, writer)
	if ok {
		return w.(*RollingWriter)
	}

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
	data := make(map[string][]types.RawRecord)
	deletes := make(map[string][]types.RawRecord)

	for _, rec := range records {
		pKey, err := f.createPartitionKey(rec)
		if err != nil {
			return nil, nil, err
		}

		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			deletes[pKey] = append(deletes[pKey], rec)
		}

		data[pKey] = append(data[pKey], rec)
	}

	return data, deletes, nil
}

func (f *Fanout) Write(ctx context.Context, records []types.RawRecord, fields []arrow.Field) error {
	data, deletes, err := f.partition(records)
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		fieldId, ok := f.fieldIds[constants.OlakeID]

		if !ok {
			var err error
			fieldId, err = f.ops.GetFieldId(ctx, constants.OlakeID)
			if err != nil {
				return fmt.Errorf("failed to get field ID for %s: %w", constants.OlakeID, err)
			}
		}

		for pKey, record := range deletes {
			deleteWriter := f.getOrCreateRollingWriter(pKey, "delete")
			deleteWriter.FieldId = fieldId

			deletes := ExtractDeleteRecords(record)

			rec, err := CreateDelArrowRec(deletes, fieldId)
			if err != nil {
				return fmt.Errorf("failed to create delete record for partition %s: %w", pKey, err)
			}

			uploadData, err := deleteWriter.Write(ctx, rec)
			if err != nil {
				return fmt.Errorf("failed to write delete record for partition %s: %w", pKey, err)
			}

			if uploadData != nil {
				if err := f.ops.UploadFile(ctx, uploadData); err != nil {
					return fmt.Errorf("failed to upload delete file for partition %s: %w", pKey, err)
				}
			}
		}
	}

	for pKey, rawData := range data {
		rec, err := CreateArrowRecord(rawData, fields, f.Normalization)
		if err != nil {
			return fmt.Errorf("failed to create arrow record for partition %s: %w", pKey, err)
		}

		writer := f.getOrCreateRollingWriter(pKey, "data")
		uploadData, err := writer.Write(ctx, rec)
		if err != nil {
			return fmt.Errorf("failed to write to partition %s: %w", pKey, err)
		}

		if err := f.ops.UploadFile(ctx, uploadData); err != nil {
			return fmt.Errorf("failed to upload data file for partition %s: %w", pKey, err)
		}
	}

	return nil
}

func (f *Fanout) Close(ctx context.Context) error {
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

	var errs []error

	for _, writer := range deleteWriters {
		uploadData, err := writer.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to close delete writer: %w", err))
			continue
		}

		if err := f.ops.UploadFile(ctx, uploadData); err != nil {
			errs = append(errs, fmt.Errorf("failed to upload delete file: %w", err))
		}
	}

	for _, writer := range dataWriters {
		uploadData, err := writer.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to close data writer: %w", err))
			continue
		}

		if err := f.ops.UploadFile(ctx, uploadData); err != nil {
			errs = append(errs, fmt.Errorf("failed to upload data file: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("fanout close errors: %v", errors.Join(errs...))
	}

	return nil
}
