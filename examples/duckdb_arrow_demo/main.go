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
	fmt.Println("Record before transformation:")
	printRecord(record)
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
  country,
  population,
  area,
  area * 1.05 AS adjusted_area,
  population + 100000 AS projected_population,
  population / area AS density,
  city || ' (' || country || ')' AS verbose_name
FROM input
WHERE population > 2000000 OR area > 450
ORDER BY id DESC
`

	fmt.Println("Query:", query)
	resultReader, err := arrowIface.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("query arrow: %v", err)
	}
	defer resultReader.Release()

	if resultReader.Next() {
		fmt.Println("Record after transformation:")
		batch := resultReader.RecordBatch()
		printRecord(batch)
		batch.Release()
	}
}

func buildDemoRecord() (arrow.RecordBatch, func()) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "city", Type: arrow.BinaryTypes.String},
		{Name: "country", Type: arrow.BinaryTypes.String},
		{Name: "population", Type: arrow.PrimitiveTypes.Int64},
		{Name: "area", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"New York", "Los Angeles", "San Francisco", "Chicago", "Seattle", "Denver"}, nil)
	builder.Field(2).(*array.StringBuilder).AppendValues([]string{"USA", "USA", "USA", "USA", "USA", "USA"}, nil)
	builder.Field(3).(*array.Int64Builder).AppendValues([]int64{8420000, 3980000, 883000, 2716000, 744000, 715000}, nil)
	builder.Field(4).(*array.Float64Builder).AppendValues([]float64{783.8, 503.0, 231.9, 589.6, 217.0, 401.2}, nil)

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
