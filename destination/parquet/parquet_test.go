package parquet

import (
	"context"
	"fmt"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	pqgo "github.com/parquet-go/parquet-go"
)

// ---- scaffolding -----------------------------------------------------------

func newStream(normalized bool, partitionRegex string, cols map[string]types.DataType) *types.ConfiguredStream {
	st := types.NewStream("users", "public", nil)
	for name, dt := range cols {
		st.UpsertField(name, dt, true, false)
	}
	return &types.ConfiguredStream{
		StreamMetadata: types.StreamMetadata{Normalization: &normalized, PartitionRegex: partitionRegex},
		Stream:         st,
	}
}

// newParquet builds a local-mode Parquet writer rooted at dir and runs Setup. When
// maxBytes > 0 it overrides the resolved roll threshold (in bytes) for deterministic
// rolling; when rollCheckInterval > 0 it overrides how often the on-disk size is checked
// (each check flushes a row group, so keep it well above 1 to avoid per-row row groups).
func newParquet(t *testing.T, dir string, stream *types.ConfiguredStream, maxBytes int64, rollCheckInterval int) *Parquet {
	t.Helper()
	p := &Parquet{}
	p.GetConfigRef() // initializes p.config
	p.config.Path = dir
	if _, _, err := p.Setup(context.Background(), stream, nil, &destination.Options{ThreadID: "test"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if maxBytes > 0 {
		p.maxFileBytes = maxBytes
	}
	if rollCheckInterval > 0 {
		p.checkIntervalForRoll = rollCheckInterval
	}
	return p
}

// writeBatch drives the writer the way WriterThread does: flatten, evolve, then write.
func writeBatch(t *testing.T, p *Parquet, records []types.RawRecord) {
	t.Helper()
	ctx := context.Background()
	evolution, buf, _, err := p.FlattenAndCleanData(ctx, records)
	if err != nil {
		t.Fatalf("FlattenAndCleanData: %v", err)
	}
	if evolution {
		if _, err := p.EvolveSchema(ctx, nil, nil); err != nil {
			t.Fatalf("EvolveSchema: %v", err)
		}
	}
	if err := p.Write(ctx, buf); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func rec(id int64, extra map[string]any) types.RawRecord {
	data := map[string]any{"id": id, "name": fmt.Sprintf("name-%d-padding-payload-to-grow-rows", id)}
	maps.Copy(data, extra)
	return types.RawRecord{
		Data: data,
		OlakeColumns: map[string]any{
			constants.OlakeID:        fmt.Sprintf("oid-%d", id),
			constants.OlakeTimestamp: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
			constants.OpType:         "i",
		},
	}
}

func recs(n int, extra func(i int) map[string]any) []types.RawRecord {
	out := make([]types.RawRecord, n)
	for i := range out {
		var e map[string]any
		if extra != nil {
			e = extra(i)
		}
		out[i] = rec(int64(i), e)
	}
	return out
}

// ---- parquet read helpers --------------------------------------------------

func parquetFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == "."+constants.ParquetFileExt {
			out = append(out, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return out
}

func parquetNumRows(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	pf, err := pqgo.OpenFile(f, info.Size())
	if err != nil {
		t.Fatalf("open parquet %s: %v", path, err)
	}
	return pf.NumRows()
}

func totalRows(t *testing.T, files []string) int64 {
	t.Helper()
	var total int64
	for _, f := range files {
		total += parquetNumRows(t, f)
	}
	return total
}

// ---- tests -----------------------------------------------------------------

// Rolling on a tiny threshold must produce several files whose rows sum to the input,
// for both the denormalized and normalized write paths.
func TestRollsIntoMultipleFiles(t *testing.T) {
	const batches, perBatch = 3, 2000
	for _, tc := range []struct {
		name       string
		normalized bool
	}{
		{"denormalized", false},
		{"normalized", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stream := newStream(tc.normalized, "", map[string]types.DataType{"id": types.Int64, "name": types.String})
			p := newParquet(t, dir, stream, 64*1024, 0) // 64KB rolling

			for range batches {
				writeBatch(t, p, recs(perBatch, nil))
			}
			if err := p.Close(context.Background(), nil); err != nil {
				t.Fatalf("Close: %v", err)
			}

			files := parquetFiles(t, dir)
			if len(files) < 2 {
				t.Fatalf("expected rolling into multiple files, got %d: %v", len(files), files)
			}

			if got, want := totalRows(t, files), int64(batches*perBatch); got != want {
				t.Fatalf("row count mismatch: got %d want %d across %d files", got, want, len(files))
			}
		})
	}
}

// Under the default (large) threshold a partition stays in a single file.
func TestNoRollSingleFile(t *testing.T) {
	dir := t.TempDir()
	stream := newStream(false, "", map[string]types.DataType{"id": types.Int64, "name": types.String})
	p := newParquet(t, dir, stream, 0, 0) // keep the resolved default (512MB)

	writeBatch(t, p, recs(500, nil))
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := parquetFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected exactly one file, got %d: %v", len(files), files)
	}
	if got := totalRows(t, files); got != 500 {
		t.Fatalf("row count mismatch: got %d want 500", got)
	}
}

// Each partition rolls independently; files land under per-partition directories and
// the total row count is preserved.
func TestRollsPerPartition(t *testing.T) {
	dir := t.TempDir()
	stream := newStream(false, "{region, default_region, }", map[string]types.DataType{
		"id": types.Int64, "name": types.String, "region": types.String,
	})
	p := newParquet(t, dir, stream, 16*1024, 0) // 16KB rolling

	region := func(i int) map[string]any {
		if i%2 == 0 {
			return map[string]any{"region": "us"}
		}
		return map[string]any{"region": "eu"}
	}
	const batches, perBatch = 2, 3000
	for range batches {
		writeBatch(t, p, recs(perBatch, region))
	}
	if err := p.Close(context.Background(), nil); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := parquetFiles(t, dir)
	dirs := map[string]bool{}
	for _, f := range files {
		dirs[filepath.Base(filepath.Dir(f))] = true
	}
	if !dirs["us"] || !dirs["eu"] {
		t.Fatalf("expected us+eu partition dirs, got %v (files: %v)", dirs, files)
	}
	if got, want := totalRows(t, files), int64(batches*perBatch); got != want {
		t.Fatalf("row count mismatch: got %d want %d", got, want)
	}
}

// decimalRecords holds one record per text, its price column read as a decimal of dataType; an empty text is null.
func decimalRecords(t *testing.T, dataType types.DataType, texts ...string) []types.RawRecord {
	t.Helper()
	records := make([]types.RawRecord, len(texts))
	for i, text := range texts {
		var price any
		if text != "" {
			value, err := typeutils.ReformatDecimal(dataType, text)
			if err != nil {
				t.Fatalf("ReformatDecimal(%s): %v", text, err)
			}
			price = value
		}
		records[i] = rec(int64(i), map[string]any{"price": price})
	}
	return records
}

// integers is the text of each integer from from up to to.
func integers(from, to int) []string {
	texts := make([]string, 0, to-from)
	for i := from; i < to; i++ {
		texts = append(texts, strconv.Itoa(i))
	}
	return texts
}

// twosComplement reads big-endian two's complement bytes as an integer.
func twosComplement(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).SetBytes(append([]byte{1}, make([]byte, len(b))...)))
	}
	return n
}

// boundOf is the unscaled integer a statistics or column index bound holds: little-endian for INT32 and
// INT64, big-endian for fixed bytes.
func boundOf(bound []byte, kind pqgo.Kind) *big.Int {
	if kind != pqgo.FixedLenByteArray {
		bound = slices.Clone(bound)
		slices.Reverse(bound)
	}
	return twosComplement(bound)
}

// valueText is the unscaled integer a reader such as spark decodes from a decimal value, "" for null.
func valueText(value pqgo.Value) string {
	switch {
	case value.IsNull():
		return ""
	case value.Kind() == pqgo.FixedLenByteArray:
		return twosComplement(value.ByteArray()).String()
	default:
		return value.String()
	}
}

func openParquet(t *testing.T, path string) *pqgo.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := pqgo.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	return pf
}

// TestWrite reads back the parquet column type, the values a reader such as spark decodes, and the bounds
// readers skip data by: each file's column statistics and the per page bounds of its column index.
func TestWrite(t *testing.T) {
	testCases := []struct {
		name     string
		dataType types.DataType
		texts    []string
		perFile  int
		node     pqgo.Node
		want     []string
		bounds   []string
		pages    int
	}{
		{
			name:     "decimal up to 9 digits is an int32",
			dataType: types.DecimalOf(9, 2),
			texts:    []string{"-1234567.89"},
			node:     pqgo.Decimal(2, 9, pqgo.Int32Type),
			want:     []string{"-123456789"},
			bounds:   []string{"-123456789 -123456789"},
		},
		{
			name:     "decimal up to 18 digits is an int64",
			dataType: types.DecimalOf(18, 4),
			texts:    []string{"12345678901234.5678"},
			node:     pqgo.Decimal(4, 18, pqgo.Int64Type),
			want:     []string{"123456789012345678"},
			bounds:   []string{"123456789012345678 123456789012345678"},
		},
		{
			name:     "an int64 decimal bounds a negative below a positive",
			dataType: types.DecimalOf(18, 4),
			texts:    []string{"2.0000", "-1.0000"},
			node:     pqgo.Decimal(4, 18, pqgo.Int64Type),
			want:     []string{"20000", "-10000"},
			bounds:   []string{"-10000 20000"},
		},
		{
			name:     "decimal past 18 digits is the fewest fixed bytes that hold it",
			dataType: types.DecimalOf(20, 2),
			texts:    []string{"-123456789012345678.90"},
			node:     pqgo.Decimal(2, 20, pqgo.FixedLenByteArrayType(9)),
			want:     []string{"-12345678901234567890"},
			bounds:   []string{"-12345678901234567890 -12345678901234567890"},
		},
		{
			name:     "the widest decimal is 16 fixed bytes",
			dataType: types.DecimalOf(38, 10),
			texts:    []string{"-1234567890123456789012345678.0123456789"},
			node:     pqgo.Decimal(10, 38, pqgo.FixedLenByteArrayType(16)),
			want:     []string{"-12345678901234567890123456780123456789"},
			bounds:   []string{"-12345678901234567890123456780123456789 -12345678901234567890123456780123456789"},
		},
		{
			// parquet-go v0.29.0 orders the bytes unsigned, which bounds this as 1.50 to -3.00
			name:     "fixed bytes bound a negative below a positive",
			dataType: types.DecimalOf(20, 2),
			texts:    []string{"5.00", "-3.00", "1.50"},
			node:     pqgo.Decimal(2, 20, pqgo.FixedLenByteArrayType(9)),
			want:     []string{"500", "-300", "150"},
			bounds:   []string{"-300 500"},
		},
		{
			name:     "fixed bytes bound both ends of the widest range",
			dataType: types.DecimalOf(38, 0),
			texts:    []string{"99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", "0", "-1", "1"},
			node:     pqgo.Decimal(0, 38, pqgo.FixedLenByteArrayType(16)),
			want:     []string{"99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", "0", "-1", "1"},
			bounds:   []string{"-99999999999999999999999999999999999999 99999999999999999999999999999999999999"},
		},
		{
			name:     "fixed bytes bound the smallest fractions by sign",
			dataType: types.DecimalOf(25, 5),
			texts:    []string{"-0.00001", "0.00001", "-99999999999999999999.99999"},
			node:     pqgo.Decimal(5, 25, pqgo.FixedLenByteArrayType(11)),
			want:     []string{"-1", "1", "-9999999999999999999999999"},
			bounds:   []string{"-9999999999999999999999999 1"},
		},
		{
			name:     "each file bounds its own decimals",
			dataType: types.DecimalOf(20, 0),
			texts:    []string{"7", "-7", "100", "-100"},
			perFile:  2,
			node:     pqgo.Decimal(0, 20, pqgo.FixedLenByteArrayType(9)),
			want:     []string{"7", "-7", "100", "-100"},
			bounds:   []string{"-7 7", "-100 100"},
		},
		{
			// parquet-go v0.29.0 leaves a null page out of a fixed bytes column index, misaligning its lists
			name:     "a file of null decimals has no bounds and a column index entry for its page",
			dataType: types.DecimalOf(20, 0),
			texts:    []string{"5", "-3", "", ""},
			perFile:  2,
			node:     pqgo.Decimal(0, 20, pqgo.FixedLenByteArrayType(9)),
			want:     []string{"5", "-3", "", ""},
			bounds:   []string{"-3 5", ""},
		},
		{
			// the column statistics merge the bounds of pages holding only negatives, only positives, and both
			name:     "pages merge their fixed bytes bounds by sign",
			dataType: types.DecimalOf(20, 0),
			texts:    integers(-50000, 50000),
			node:     pqgo.Decimal(0, 20, pqgo.FixedLenByteArrayType(9)),
			want:     integers(-50000, 50000),
			bounds:   []string{"-50000 49999"},
			pages:    2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			var maxBytes int64
			if tc.perFile > 0 {
				maxBytes = 1 // roll the file at every check, so each holds perFile records
			}
			p := newParquet(t, dir, newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String, "price": tc.dataType}), maxBytes, tc.perFile)
			writeBatch(t, p, decimalRecords(t, tc.dataType, tc.texts...))
			if err := p.Close(context.Background(), nil); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got := make([]string, len(tc.texts))
			var bounds []string
			for _, path := range parquetFiles(t, dir) {
				pf := openParquet(t, path)
				price, found := pf.Schema().Lookup("price")
				if !found {
					t.Fatal("price column missing from the file")
				}
				id, _ := pf.Schema().Lookup("id")
				if !pqgo.EqualNodes(pqgo.Optional(tc.node), price.Node) {
					t.Errorf("node %v, want %v", price.Node, tc.node)
				}
				kind := price.Node.Type().Kind()
				for i, rowGroup := range pf.RowGroups() {
					rows, buffer := rowGroup.Rows(), make([]pqgo.Row, 256)
					for {
						n, err := rows.ReadRows(buffer)
						for _, row := range buffer[:n] {
							got[row[id.ColumnIndex].Int64()] = valueText(row[price.ColumnIndex])
						}
						if err != nil {
							break
						}
					}
					rows.Close()

					fileBounds := ""
					if stats := pf.Metadata().RowGroups[i].Columns[price.ColumnIndex].MetaData.Statistics; stats.MinValue != nil {
						fileBounds = boundOf(stats.MinValue, kind).String() + " " + boundOf(stats.MaxValue, kind).String()
					}
					bounds = append(bounds, fileBounds)

					index := pf.ColumnIndexes()[i*len(pf.Metadata().RowGroups[i].Columns)+price.ColumnIndex]
					if len(index.MinValues) != len(index.NullPages) || len(index.MaxValues) != len(index.NullPages) {
						t.Fatalf("column index of %d pages holds %d mins and %d maxes", len(index.NullPages), len(index.MinValues), len(index.MaxValues))
					}
					if len(index.NullPages) < tc.pages {
						t.Errorf("column of %d pages, want at least %d", len(index.NullPages), tc.pages)
					}
					var lowest, highest *big.Int
					for page, null := range index.NullPages {
						if null {
							continue
						}
						low, high := boundOf(index.MinValues[page], kind), boundOf(index.MaxValues[page], kind)
						if low.Cmp(high) > 0 {
							t.Errorf("page %d bounded from %s down to %s", page, low, high)
						}
						if lowest == nil || low.Cmp(lowest) < 0 {
							lowest = low
						}
						if highest == nil || high.Cmp(highest) > 0 {
							highest = high
						}
					}
					if lowest != nil && lowest.String()+" "+highest.String() != fileBounds {
						t.Errorf("pages bound %s %s, statistics %q", lowest, highest, fileBounds)
					}
				}
			}

			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("record %d read back %q, want %q", i, got[i], tc.want[i])
				}
			}
			slices.Sort(bounds)
			if want := slices.Sorted(slices.Values(tc.bounds)); !slices.Equal(bounds, want) {
				t.Errorf("file bounds %q, want %q", bounds, want)
			}
		})
	}
}

// TestFlattenAndCleanData checks when a decimal value evolves its column, after a first batch that
// adds the olake columns.
func TestFlattenAndCleanData(t *testing.T) {
	column := types.DecimalOf(10, 2)
	testCases := []struct {
		name      string
		valueType types.DataType
		text      string
		evolution bool
		want      types.DataType
	}{
		{
			name:      "a decimal of the column's type changes no schema",
			valueType: column,
			text:      "2.50",
			evolution: false,
			want:      column,
		},
		{
			name:      "a wider decimal widens the column",
			valueType: types.DecimalOf(12, 2),
			text:      "1234567890.12",
			evolution: true,
			want:      types.DecimalOf(12, 2),
		},
		{
			name:      "decimals of two scales meet at the decimal holding both",
			valueType: types.DecimalOf(10, 4),
			text:      "1.2345",
			evolution: true,
			want:      types.DecimalOf(12, 4),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := newParquet(t, t.TempDir(), newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String, "price": column}), 0, 0)
			if _, _, _, err := p.FlattenAndCleanData(context.Background(), decimalRecords(t, column, "1.50")); err != nil {
				t.Fatalf("first batch: %v", err)
			}
			evolution, _, _, err := p.FlattenAndCleanData(context.Background(), decimalRecords(t, tc.valueType, tc.text))
			if err != nil {
				t.Fatalf("FlattenAndCleanData: %v", err)
			}
			if evolution != tc.evolution || p.schema["price"].Type() != tc.want {
				t.Fatalf("evolution=%v type=%s, want evolution=%v type=%s", evolution, p.schema["price"].Type(), tc.evolution, tc.want)
			}
		})
	}
}

// TestWriteRejectsWrongFixedBinaryWidth writes a 16-byte value into a fixed_binary(8) column, which
// Write must reject with an error rather than let parquet-go panic.
func TestWriteRejectsWrongFixedBinaryWidth(t *testing.T) {
	stream := newStream(true, "", map[string]types.DataType{"id": types.Int64, "name": types.String, "code": types.FixedBinaryOf(8)})
	p := newParquet(t, t.TempDir(), stream, 0, 0)
	_, buf, _, err := p.FlattenAndCleanData(context.Background(), []types.RawRecord{rec(1, map[string]any{"code": make([]byte, 16)})})
	if err != nil {
		t.Fatalf("FlattenAndCleanData: %v", err)
	}
	want := "field[code]: fixed_binary(8) holds exactly 8 bytes, got 16"
	if err := p.Write(context.Background(), buf); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Write error = %v, want it to contain %q", err, want)
	}
}

// TestParquetGetPartitionedFilePath checks the directory each partition value renders to. Bytes
// render as escaped base64, the arrow writer's identity form, so a '/' in the encoding stays inside
// one directory.
func TestParquetGetPartitionedFilePath(t *testing.T) {
	testCases := []struct {
		name    string
		pattern string
		values  map[string]any
		want    string
	}{
		{
			name:    "bytes render as escaped base64",
			pattern: "/{pk, default, }",
			values:  map[string]any{"pk": []byte("k0000001")},
			want:    "azAwMDAwMDE%3D",
		},
		{
			name:    "a slash in the base64 stays in one directory",
			pattern: "/{pk, default, }",
			values:  map[string]any{"pk": []byte{0xff, 0xfe, 0xfb}},
			want:    "%2F%2F77",
		},
		{
			name:    "text renders as itself",
			pattern: "/{pk, default, }",
			values:  map[string]any{"pk": "k0000001"},
			want:    "k0000001",
		},
		{
			name:    "a missing value takes the default",
			pattern: "/{pk, default, }",
			values:  map[string]any{},
			want:    "default",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := newStream(false, tc.pattern, map[string]types.DataType{"pk": types.Binary})
			p := newParquet(t, t.TempDir(), stream, 0, 0)
			got := p.getPartitionedFilePath(tc.values, time.Unix(0, 0).UTC())
			if want := filepath.Join(p.basePath, tc.want); got != want {
				t.Fatalf("getPartitionedFilePath() = %q, want %q", got, want)
			}
		})
	}
}
