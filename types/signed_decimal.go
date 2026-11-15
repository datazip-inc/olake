package types

import (
	"bytes"
	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// TODO: delete signedDecimal and bump parquet-go once a release carries parquet-go#595, which orders
// byte array decimals as signed itself; v0.32.0, the latest on 2026-11-12, predates it.

// signedDecimal is a FIXED_LEN_BYTE_ARRAY decimal ordered as signed, the DECIMAL sort order. parquet-go
// orders the bytes unsigned, so a negative value would bound a page above every positive one.
type signedDecimal struct{ parquet.Type }

// Compare merges the bounds of a column chunk's pages into its statistics.
func (t signedDecimal) Compare(a, b parquet.Value) int {
	return compareSigned(a.ByteArray(), b.ByteArray())
}

func (t signedDecimal) NewColumnBuffer(columnIndex, numValues int) parquet.ColumnBuffer {
	return signedDecimalBuffer{t.Type.NewColumnBuffer(columnIndex, numValues)}
}

func (t signedDecimal) NewColumnIndexer(int) parquet.ColumnIndexer {
	return &signedDecimalIndexer{}
}

// compareSigned compares two big-endian two's complement values of the same width.
func compareSigned(a, b []byte) int {
	if negativeA, negativeB := len(a) > 0 && a[0]&0x80 != 0, len(b) > 0 && b[0]&0x80 != 0; negativeA != negativeB {
		if negativeA {
			return -1
		}
		return 1
	}
	return bytes.Compare(a, b)
}

// signedDecimalBuffer hands the writer pages bounded in the signed order.
type signedDecimalBuffer struct{ parquet.ColumnBuffer }

func (b signedDecimalBuffer) Clone() parquet.ColumnBuffer {
	return signedDecimalBuffer{b.ColumnBuffer.Clone()}
}

func (b signedDecimalBuffer) Page() parquet.Page {
	return signedDecimalPage{b.ColumnBuffer.Page()}
}

// signedDecimalPage bounds its values in the signed order; the writer builds both the column chunk statistics
// and the column index from these bounds.
type signedDecimalPage struct{ parquet.Page }

func (p signedDecimalPage) Slice(i, j int64) parquet.Page {
	return signedDecimalPage{p.Page.Slice(i, j)}
}

func (p signedDecimalPage) Bounds() (minValue, maxValue parquet.Value, ok bool) {
	values := p.Values()
	buffer := make([]parquet.Value, 64)
	for {
		n, err := values.ReadValues(buffer)
		for _, value := range buffer[:n] {
			switch {
			case !ok:
				minValue, maxValue, ok = value, value, true
			case compareSigned(value.ByteArray(), minValue.ByteArray()) < 0:
				minValue = value
			case compareSigned(value.ByteArray(), maxValue.ByteArray()) > 0:
				maxValue = value
			}
		}
		if err != nil || n == 0 {
			return minValue, maxValue, ok
		}
	}
}

// signedDecimalIndexer keeps each page's whole bounds, since cutting a two's complement value short changes
// its order. It leaves the boundary order unordered, which holds for any pages.
// shortcut: readers scan every page bound instead of binary searching them, until parquet-go#595 replaces this.
type signedDecimalIndexer struct{ index format.ColumnIndex }

func (i *signedDecimalIndexer) Reset() {
	i.index = format.ColumnIndex{}
}

func (i *signedDecimalIndexer) IndexPage(numValues, numNulls int64, minValue, maxValue parquet.Value) {
	i.index.NullPages = append(i.index.NullPages, numValues == numNulls)
	i.index.NullCounts = append(i.index.NullCounts, numNulls)
	i.index.MinValues = append(i.index.MinValues, slices.Clone(minValue.ByteArray()))
	i.index.MaxValues = append(i.index.MaxValues, slices.Clone(maxValue.ByteArray()))
}

func (i *signedDecimalIndexer) ColumnIndex() format.ColumnIndex {
	return i.index
}
