package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// Local destination writes Parquet files
// local_path/database/table/1...999.parquet
type Local struct {
	closed bool
	config *Config
	file   source.ParquetFile
	writer *writer.ParquetWriter
}

func (l *Local) GetConfigRef() any {
	l.config = &Config{}
	return l.config
}

func (p *Local) Spec() any {
	return Config{}
}

func (l *Local) Setup(stream protocol.Stream) error {
	destinationFilePath := filepath.Join(l.config.BaseFilePath, stream.Namespace(), stream.Name(), utils.TimestampedFileName())
	// Start a new local writer
	pqFile, err := local.NewLocalFileWriter(destinationFilePath)
	if err != nil {
		return err
	}

	// TODO: Generate Correct parquet write with Schema from Stream

	// Create writer
	pw, err := writer.NewParquetWriter(pqFile, nil, 4)
	if err != nil {
		return err
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	l.file = pqFile
	l.writer = pw
	return nil
}

func (l *Local) Check() error {
	// Create a temporary file in the specified directory
	tempFile, err := os.CreateTemp(filepath.Join(l.config.BaseFilePath, "temp"), "temporary-*.txt")
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
	for {
		select {
		case <-ctx.Done():
			break iteration
		default:
			record, ok := <-channel
			if !ok { // channel has been closed by other process; possibly the producer(i.e. reader)
				break iteration
			}

			// TODO: Manipulate the record for parquet
			// Write the record to the Parquet file
			if err := l.writer.Write(record); err != nil {
				return fmt.Errorf("parquet write error: %s", err)
			}
		}
	}

	return nil
}

func (l *Local) ReInitiationRequiredOnSchemaEvolution() bool {
	return true
}

func (l *Local) Close() error {
	if l.closed {
		return nil
	}

	return utils.ErrExecSequential(l.writer.WriteStop, l.file.Close)
}

func (l *Local) Type() string {
	return string(types.Local)
}

func init() {
	protocol.RegisteredWriters[types.Local] = func() protocol.Writer {
		return &Local{}
	}
}
