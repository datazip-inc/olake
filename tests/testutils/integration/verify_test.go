package integration

import (
	"fmt"
	"maps"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

func TestBinaryExpectations(t *testing.T) {
	typeMapping := map[string]string{"binary(16)": "fixed[16]", "blob": "binary", "varchar": "string"}
	declared := map[string]string{"data_fixed_binary": "binary(16)", "data_blob": "blob", "name": "varchar"}
	want := map[string]string{"data_fixed_binary": "fixed[16]", "data_blob": "binary"}
	if got := binaryExpectations(declared, typeMapping); !maps.Equal(got, want) {
		t.Fatalf("binaryExpectations = %v, want %v", got, want)
	}
	for expected, spark := range map[string]string{"fixed[16]": "binary", "binary": "binary", "bigint": "bigint"} {
		if got := sparkTypeOf(expected); got != spark {
			t.Errorf("sparkTypeOf(%q) = %q, want %q", expected, got, spark)
		}
	}
	for expected, kind := range map[string]string{"fixed[16]": "FIXED_LEN_BYTE_ARRAY(16)", "binary": "BYTE_ARRAY"} {
		if got := parquetKindOf(expected); got != kind {
			t.Errorf("parquetKindOf(%q) = %q, want %q", expected, got, kind)
		}
	}
	for _, bad := range []string{"fixed[]", "fixed[0]", "fixed[x]", "fixed[16"} {
		if _, ok := fixedBinaryWidth(bad); ok {
			t.Errorf("fixedBinaryWidth(%q) accepted a malformed width", bad)
		}
	}
}

type testMockRow map[string]any

func (m testMockRow) At(_ int) any          { return nil }
func (m testMockRow) Value(name string) any { return m[name] }
func (m testMockRow) Values() []any         { return nil }
func (m testMockRow) Len() int              { return len(m) }
func (m testMockRow) FieldNames() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestExpectedRowCountContract(t *testing.T) {
	// 1. Defaults (Relational / Mongo / Kafka)
	th := &TestHandler{}
	assertExpectedRowCount := func(opSymbol, operation string, want int) {
		t.Helper()
		got, err := th.ExpectedRowCount(opSymbol, operation)
		if err != nil {
			t.Errorf("ExpectedRowCount(%q, %q) returned unexpected error: %v", opSymbol, operation, err)
		} else if got != want {
			t.Errorf("ExpectedRowCount(%q, %q) = %d, want %d", opSymbol, operation, got, want)
		}
	}

	assertExpectedRowCount("r", "", 5)
	assertExpectedRowCount("c", "insert", 1)
	assertExpectedRowCount("c", "add", 1)
	assertExpectedRowCount("u", "update", 1)
	assertExpectedRowCount("d", "delete", 0)

	// 2. Custom contract (S3: 6/3/3)
	s3Handler := &TestHandler{
		RowCounts: RowCountContract{
			FullLoad: 6,
			Insert:   3,
			Update:   3,
		},
	}
	assertS3ExpectedRowCount := func(opSymbol, operation string, want int) {
		t.Helper()
		got, err := s3Handler.ExpectedRowCount(opSymbol, operation)
		if err != nil {
			t.Errorf("s3 ExpectedRowCount(%q, %q) returned unexpected error: %v", opSymbol, operation, err)
		} else if got != want {
			t.Errorf("s3 ExpectedRowCount(%q, %q) = %d, want %d", opSymbol, operation, got, want)
		}
	}

	assertS3ExpectedRowCount("r", "", 6)
	assertS3ExpectedRowCount("u", "insert", 3)
	assertS3ExpectedRowCount("u", "update", 3)
}

func TestVerifySyncedRows_Success(t *testing.T) {
	schema := map[string]interface{}{
		"col_text": "sample text",
		"col_int":  int32(123),
	}
	mockRows := make([]types.Row, 5)
	for i := 0; i < 5; i++ {
		mockRows[i] = testMockRow{
			"_olake_id": fmt.Sprintf("id-%d", i),
			"_op_type":  "r",
			"col_text":  "sample text",
			"col_int":   int32(123),
		}
	}

	// Should pass with exact count 5
	verifySyncedRows(t, mockRows, 5, schema, nil, "r", "Iceberg", "postgres", false)

	// String ID conversion test
	if got := stringID([]byte("abc")); got != "abc" {
		t.Errorf("stringID([]byte(\"abc\")) = %q, want \"abc\"", got)
	}
	if got := stringID("hello"); got != "hello" {
		t.Errorf("stringID(\"hello\") = %q, want \"hello\"", got)
	}
}

func TestVerifySyncedRows_DeleteOp(t *testing.T) {
	// 0 rows is acceptable for delete
	verifySyncedRows(t, nil, 0, nil, nil, "d", "Iceberg", "postgres", true)

	// 1 row with non-empty _olake_id is also acceptable for delete
	deleteRow := []types.Row{
		testMockRow{
			"_olake_id": "deleted-row-1",
			"_op_type":  "d",
		},
	}
	verifySyncedRows(t, deleteRow, 0, nil, nil, "d", "Iceberg", "postgres", true)
}

func TestExpectedRowCountContract_UnknownOperationError(t *testing.T) {
	th := &TestHandler{}
	_, err := th.ExpectedRowCount("c", "unknown_operation")
	if err == nil {
		t.Errorf("ExpectedRowCount with unknown operation should have returned error, but got nil")
	}
}

func TestVerifySyncedRows_SkipRowCount(t *testing.T) {
	mockRows := []types.Row{
		testMockRow{
			"_olake_id": "id-1",
			"_op_type":  "r",
		},
	}
	// Passing SkipRowCount (-1) or any negative count (-2) asserts NotEmpty without asserting exact len
	verifySyncedRows(t, mockRows, SkipRowCount, nil, nil, "r", "Iceberg", "postgres", false)
	verifySyncedRows(t, mockRows, -2, nil, nil, "r", "Iceberg", "postgres", false)
}

func TestVerifySyncedRows_ExactZeroRows(t *testing.T) {
	// 0 rows expected and 0 rows provided: should pass
	verifySyncedRows(t, nil, 0, nil, nil, "c", "Iceberg", "postgres", false)
}
