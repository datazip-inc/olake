package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

// Local destination writes Parquet files
// local_path/database/table/1...999.parquet
type Local struct {
	closed bool
	config *Config
	file   source.ParquetFile
	writer *goparquet.FileWriter
	stream protocol.Stream
}

func (l *Local) GetConfigRef() any {
	l.config = &Config{}
	return l.config
}

func (p *Local) Spec() any {
	return Config{}
}

func (l *Local) Setup(stream protocol.Stream) error {
	destinationFilePath := filepath.Join(l.config.BaseFilePath, stream.Namespace(), stream.Name(), utils.TimestampedFileName(constants.ParquetFileExt))
	// Start a new local writer
	os.MkdirAll(filepath.Dir(destinationFilePath), os.ModePerm)

	pqFile, err := local.NewLocalFileWriter(destinationFilePath)
	if err != nil {
		return err
	}

	// parquetschema.ParseSchemaDefinition()
	writer := goparquet.NewFileWriter(pqFile, goparquet.WithSchemaDefinition(stream.Schema().ToParquet()),
		goparquet.WithMaxRowGroupSize(128*1024*1024), // 128MB
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)

	// Create writer
	// pw, err := writer.NewParquetWriter(pqFile, stream.Schema().ToParquet(), 4)
	// if err != nil {
	// 	return err
	// }

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
	for {
		select {
		case <-ctx.Done():
			break iteration
		default:
			record, ok := <-channel
			if !ok {
				// channel has been closed by other process; possibly the producer(i.e. reader)
				break iteration
			}

			def := l.stream.Schema().ToParquet()
			var columnDef *parquetschema.ColumnDefinition
			i, found := utils.ArrayContains(def.RootColumn.Children, func(one *parquetschema.ColumnDefinition) bool {
				return one.SchemaElement.Name == "id"
			})
			if found {
				columnDef = def.RootColumn.Children[i]
			}

			if err := l.writer.AddData(record); err != nil {
				return fmt.Errorf("parquet write error: %s", err)
			}

			if columnDef == nil {
				fmt.Println("here")
			}
		}
	}

	return nil
}

func (l *Local) ReInitiationOnTypeChange() bool {
	return true
}

func (l *Local) ReInitiationOnNewColumns() bool {
	return false
}

func (l *Local) EvolveSchema(mutation map[string]*types.Property) error {
	for column, property := range mutation {
		pqSchemaElem := property.DataType().ToParquet()
		pqSchemaElem.Name = column

		l.writer.SetSchemaDefinition(l.stream.Schema().ToParquet())
		// l.writer.SchemaHandler.SchemaElements = append(l.writer.SchemaHandler.SchemaElements, pqSchemaElem)
	}

	return nil
}

func (l *Local) Close() error {
	if l.closed {
		return nil
	}

	return utils.ErrExecSequential(
		utils.ErrExecFormat("failed to stop local writer: %s", func() error {
			return l.writer.Close()
		}),
		utils.ErrExecFormat("failed to close parquet file: %s", l.file.Close),
	)
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
		return &Local{}
	}
}
