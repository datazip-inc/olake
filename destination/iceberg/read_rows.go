package iceberg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"google.golang.org/grpc"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

// openFileWriter is a writer that holds data files open on the Go side (the arrow writer).
type openFileWriter interface {
	// EnsureReadable closes any file in paths the writer still has open, without committing it, so its rows
	// can be read (an open file has no footer).
	EnsureReadable(ctx context.Context, paths []string) error
}

var _ destination.RowReader = (*Iceberg)(nil)

// ReadRows implements destination.RowReader: it finds each row in this thread's view of the stream's row index
// and reads the wanted columns back through the Java table index service.
func (i *Iceberg) ReadRows(ctx context.Context, rows []destination.RowRead, emit func(int, map[string]any) error) error {
	request := &proto.ReadRowsRequest{ThreadId: i.options.ThreadID}
	wanted := make(map[types.RowLocation]int, len(rows)) // a row Java sends back, by file and position, to its index in rows
	var paths []string                                   // a file is named once per run of rows; repeats are harmless
	for idx, row := range rows {
		location, found, err := i.indexThread.Lookup(row.OlakeID)
		if err != nil {
			return fmt.Errorf("failed to look up row[%s] in index: %w", row.OlakeID, err)
		}
		if !found {
			if err := emit(idx, nil); err != nil {
				return err
			}
			continue
		}
		wanted[location] = idx
		if last := len(request.Files) - 1; last < 0 || request.Files[last].GetFilePath() != location.FilePath {
			request.Files = append(request.Files, &proto.ReadRowsRequest_FileRows{FilePath: location.FilePath})
			paths = append(paths, location.FilePath)
		}
		file := request.Files[len(request.Files)-1]
		file.Rows = append(file.Rows, &proto.ReadRowsRequest_Row{Position: location.Position, Columns: row.Columns})
	}
	if len(request.Files) == 0 {
		return nil
	}

	// Rows written earlier in this sync may sit in a file the arrow writer still holds open; the legacy
	// writer's open files are closed by Java inside ReadRows.
	if withOpenFiles, ok := i.writer.(openFileWriter); ok {
		if err := withOpenFiles.EnsureReadable(ctx, paths); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx) // canceled on return, which stops Java's reads if this side fails first
	defer cancel()
	// The limit applies per streamed message; the 4 MB default is too small for one large value. Java keeps each
	// message to 64 rows or 16 MB, plus one value.
	stream, err := i.server.tableIndexClient.ReadRows(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return fmt.Errorf("failed to read unavailable column values: %w", err)
	}
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil // every row asked has arrived, or as many as Java's read limit allowed
		}
		if err != nil {
			return fmt.Errorf("failed to read unavailable column values: %w", err)
		}
		for _, row := range batch.GetRows() {
			values := make(map[string]any, len(row.GetValues()))
			for column, field := range row.GetValues() {
				values[column] = fieldValue(field)
			}
			if err := emit(wanted[types.RowLocation{FilePath: row.GetFilePath(), Position: row.GetPosition()}], values); err != nil {
				return err
			}
		}
	}
}

// fieldValue converts one stored value for the record; unset means NULL. The columns read are text (Postgres
// text, json, arrays and the like, _op_type, the stored row), sent as bytes when not UTF-8, or numeric as double.
func fieldValue(value *proto.IcebergPayload_IceRecord_FieldValue) any {
	switch stored := value.GetValue().(type) {
	case *proto.IcebergPayload_IceRecord_FieldValue_StringValue:
		return stored.StringValue
	case *proto.IcebergPayload_IceRecord_FieldValue_BytesValue:
		return string(stored.BytesValue)
	case *proto.IcebergPayload_IceRecord_FieldValue_DoubleValue:
		return stored.DoubleValue
	default:
		return nil
	}
}
