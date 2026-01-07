//go:build duckdb_arrow
// +build duckdb_arrow

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

func main() {
	ctx := context.Background()

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		log.Fatalf("create connector: %v", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(ctx)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	arrowIface, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		log.Fatalf("new arrow interface: %v", err)
	}

	record, cleanup := buildDemoRecord()
	defer cleanup()

	reader, err := array.NewRecordReader(record.Schema(), []arrow.RecordBatch{record})
	if err != nil {
		log.Fatalf("create record reader: %v", err)
	}
	defer reader.Release()

	release, err := arrowIface.RegisterView(reader, "input")
	if err != nil {
		log.Fatalf("register view: %v", err)
	}
	defer release()

	query := `
SELECT
  id,
  city,
  id * 10 AS id_times_ten
FROM input
WHERE id % 2 = 1
`

	fmt.Println("Query:", query)
	resultReader, err := arrowIface.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("query arrow: %v", err)
	}
	defer resultReader.Release()

	for resultReader.Next() {
		batch := resultReader.RecordBatch()
		printRecord(batch)
		batch.Release()
	}
}

func buildDemoRecord() (arrow.RecordBatch, func()) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "city", Type: arrow.BinaryTypes.String},
	}, nil)

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"nyc", "la", "sf", "mia"}, nil)

	record := builder.NewRecord()

	return record, func() {
		record.Release()
	}
}

func printRecord(record arrow.RecordBatch) {
	fmt.Printf("batch: rows=%d cols=%d\n", record.NumRows(), record.NumCols())
	for i := int64(0); i < record.NumCols(); i++ {
		col := record.Column(int(i))
		fmt.Printf("  %s -> %s\n", record.ColumnName(int(i)), col)
	}
}
