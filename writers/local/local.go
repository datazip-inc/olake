package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/piyushsingariya/relec/memory"

	goparquet "github.com/fraugster/parquet-go"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

// Local destination writes Parquet files
// local_path/database/table/1...999.parquet
type Local struct {
	options             *protocol.Options
	fileName            string
	destinationFilePath string
	closed              bool
	config              *Config
	file                source.ParquetFile
	writer              *goparquet.FileWriter
	stream              protocol.Stream
	records             atomic.Int64
	pqSchemaMutex       sync.Mutex // To prevent concurrent underlying map access from fraugster library
}

func (l *Local) GetConfigRef() any {
	l.config = &Config{}
	return l.config
}

func (p *Local) Spec() any {
	return Config{}
}

func (l *Local) Setup(stream protocol.Stream, options *protocol.Options) error {
	l.options = options
	l.fileName = utils.TimestampedFileName(constants.ParquetFileExt)
	l.destinationFilePath = filepath.Join(l.config.BaseFilePath, stream.Namespace(), stream.Name(), l.fileName)

	// Start a new local writer
	err := os.MkdirAll(filepath.Dir(l.destinationFilePath), os.ModePerm)
	if err != nil {
		return err
	}

	pqFile, err := local.NewLocalFileWriter(l.destinationFilePath)
	if err != nil {
		return err
	}

	// parquetschema.ParseSchemaDefinition()
	writer := goparquet.NewFileWriter(pqFile, goparquet.WithSchemaDefinition(stream.Schema().ToParquet()),
		// goparquet.WithMaxRowGroupSize(10),
		// goparquet.WithMaxPageSize(100),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)

	l.file = pqFile
	l.writer = writer
	l.stream = stream
	return nil
}

func (l *Local) Check() error {
	err := os.MkdirAll(l.config.BaseFilePath, os.ModePerm)
	if err != nil {
		return err
	}

	// Create a temporary file in the specified directory
	tempFile, err := os.CreateTemp(l.config.BaseFilePath, "temporary-*.txt")
	if err != nil {
		return err
	}

	// Print the file name
	logger.Infof("Temporary file created:", tempFile.Name())

	// Write some content to the file (optional)
	if _, err := tempFile.Write([]byte("Hello, this is a temporary file!")); err != nil {
		return err
	}

	// Close the file
	if err := tempFile.Close(); err != nil {
		return err
	}

	// Delete the temporary file
	return os.Remove(tempFile.Name())
}

func (l *Local) Write(ctx context.Context, channel <-chan types.Record) error {
iteration:
	for !l.closed {
		select {
		case <-ctx.Done():
			break iteration
		default:
			record, ok := <-channel
			if !ok {
				// channel has been closed by other process; possibly the producer(i.e. reader)
				break iteration
			}

			// check memory and dump row group
			var err error
			memory.LockWithTrigger(ctx, func() {
				err = l.writer.FlushRowGroupWithContext(ctx)
			})
			if err != nil {
				return err
			}

			l.pqSchemaMutex.Lock()
			if err := l.writer.AddData(record); err != nil {
				l.pqSchemaMutex.Unlock()
				return fmt.Errorf("parquet write error: %s", err)
			}
			l.pqSchemaMutex.Unlock()

			l.records.Add(1)
		}
	}

	return nil
}

func (l *Local) ReInitiationOnTypeChange() bool {
	return false
}

func (l *Local) ReInitiationOnNewColumns() bool {
	return false
}

func (l *Local) EvolveSchema(mutation map[string]*types.Property) error {
	l.pqSchemaMutex.Lock()
	defer l.pqSchemaMutex.Unlock()

	l.writer.SetSchemaDefinition(l.stream.Schema().ToParquet())
	return nil
	// return fmt.Errorf("EvolveSchema not implemented in %s", l.Type())
}

func (l *Local) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true

	defer func() {
		if l.records.Load() == 0 {
			logger.Debugf("Wrote zero records in file[%s]; Deleting it!", l.destinationFilePath)
			err := os.Remove(l.destinationFilePath)
			if err != nil {
				logger.Warnf("failed to delete file[%s] with zero records", l.destinationFilePath)
			}
			logger.Debugf("Deleted file[%s].", l.destinationFilePath)
		}
	}()

	// send error if only records were stored and error occured; This to handle error "short write"
	if err := l.writer.Close(); err != nil && l.records.Load() > 0 {
		logger.Errorf("failed to close writer with identifier %s: %s", l.options.Identifier, err) // TODO: remove
		return fmt.Errorf("failed to stop local writer after adding %d records: %s", l.records.Load(), err)
	}

	if l.records.Load() > 0 {
		logger.Infof("Finished writing file [%s] with %d records", l.destinationFilePath, l.records.Load())
	}
	return nil
}

func (l *Local) Type() string {
	return string(types.Local)
}

func (l *Local) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func init() {
	protocol.RegisteredWriters[types.Local] = func() protocol.Writer {
		return new(Local)
	}
}
