package integration

import (
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

func TestBinaryExpectations(t *testing.T) {
	typeMapping := map[string]string{"binary(16)": "fixed[16]", "blob": "binary", "varchar": "string"}
	declared := map[string]string{"data_fixed_binary": "binary(16)", "data_blob": "blob", "name": "varchar"}
	want := map[string]string{"data_fixed_binary": "fixed[16]", "data_blob": "binary"}
	require.Equal(t, want, binaryExpectations(declared, typeMapping))
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

func TestResolveCount(t *testing.T) {
	for _, tc := range []struct {
		name string
		exp  ExpectedRows
		want *int
	}{
		{"explicit", ExpectedRows{Count: ptr(5)}, ptr(5)},
		{"explicit zero", ExpectedRows{Count: ptr(0)}, ptr(0)},
		{"from rows", ExpectedRows{Rows: map[string]map[string]interface{}{"1": {"id": 1}, "2": {"id": 2}}}, ptr(2)},
		{"empty", ExpectedRows{}, nil},
		{"skip overrides explicit", ExpectedRows{Count: ptr(5), SkipCountCheck: true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.exp.ResolveCount()
			if tc.want == nil {
				require.True(t, got == nil, "expected nil, got %v", got)
			} else {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestDefaultRowCount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler *TestHandler
		op      string
		opName  string
		want    *int
	}{
		{"relational full load", &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mysql"}}, "r", "", ptr(5)},
		{"relational cdc insert", &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mysql"}}, "c", "insert", ptr(1)},
		{"relational delete", &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mysql"}}, "d", "delete", nil},
		{"relational dedup", &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mysql"}, DedupKeys: []string{"id"}}, "r", "", ptr(1)},
		{"relational filter", &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mysql"}, ExpectedData: map[string]interface{}{"id_bigint": int64(111111111111111)}}, "r", "", ptr(1)},
		{"mssql filter", &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mssql"}, ExpectedData: map[string]interface{}{"col_decimal": testutils.Decimal("239835.89")}}, "r", "", ptr(1)},
		{"s3 full load", &TestHandler{TestConfig: &testutils.TestConfig{Driver: string(constants.S3)}}, "r", "", ptr(6)},
		{"s3 update evolved", &TestHandler{TestConfig: &testutils.TestConfig{Driver: string(constants.S3)}}, "u", "update", ptr(6)},
		{"s3 update no evolve", &TestHandler{TestConfig: &testutils.TestConfig{Driver: string(constants.S3), SkipSchemaEvolution: true}}, "u", "update", ptr(3)},
		{"kafka avro", &TestHandler{TestConfig: &testutils.TestConfig{Driver: string(constants.Kafka), DataFormat: "avro"}}, "r", "", ptr(1)},
		{"kafka json full", &TestHandler{TestConfig: &testutils.TestConfig{Driver: string(constants.Kafka)}}, "r", "", ptr(5)},
		{"kafka json cdc", &TestHandler{TestConfig: &testutils.TestConfig{Driver: string(constants.Kafka)}}, "c", "update", ptr(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.handler.defaultRowCount(tc.op, tc.opName))
		})
	}
}

func TestExpectedRowsFor(t *testing.T) {
	th := &TestHandler{TestConfig: &testutils.TestConfig{Driver: "mysql"}}
	schema := map[string]interface{}{"val": "test"}

	rows := th.ExpectedRowsFor("r", "", schema, false)
	require.Equal(t, ptr(5), rows.ResolveCount())
	require.Empty(t, rows.Rows)
	require.Equal(t, schema, rows.FallbackTemplate)

	override := th.ExpectedRowsFor("r", "", schema, false, ptr(2))
	require.Equal(t, ptr(2), override.ResolveCount())
	require.Empty(t, override.Rows)
	require.Equal(t, schema, override.FallbackTemplate)

	skipped := th.ExpectedRowsFor("r", "", schema, false, nil)
	require.True(t, skipped.ResolveCount() == nil)
	require.Empty(t, skipped.Rows)
	require.Equal(t, schema, skipped.FallbackTemplate)
}

// mockRow implements types.Row for unit testing verifySyncedRows.
type mockRow struct {
	values map[string]any
}

func (m mockRow) At(int) any            { return nil }
func (m mockRow) Value(name string) any { return m.values[name] }
func (m mockRow) Values() []any {
	vals := make([]any, 0, len(m.values))
	for _, v := range m.values {
		vals = append(vals, v)
	}
	return vals
}
func (m mockRow) Len() int { return len(m.values) }
func (m mockRow) FieldNames() []string {
	names := make([]string, 0, len(m.values))
	for k := range m.values {
		names = append(names, k)
	}
	return names
}

func TestVerifySyncedRows_MultiRowByKey(t *testing.T) {
	rows := []types.Row{
		mockRow{values: map[string]any{"_olake_id": "row-1", "id": int32(1), "name": "alice"}},
		mockRow{values: map[string]any{"_olake_id": "row-2", "id": int32(2), "name": "bob"}},
	}

	expected := ExpectedRows{
		Rows: map[string]map[string]interface{}{
			"1": {"name": "alice"},
			"2": {"name": "bob"},
		},
		KeyColumn: "id",
	}

	verifySyncedRows(t, rows, expected, "r", "Iceberg", "mysql")
}

func TestVerifySyncedRows_FallbackTemplate(t *testing.T) {
	rows := []types.Row{
		mockRow{values: map[string]any{"_olake_id": "row-1", "id": int32(1), "name": "test"}},
		mockRow{values: map[string]any{"_olake_id": "row-2", "id": int32(2), "name": "test"}},
	}

	expected := ExpectedRows{
		FallbackTemplate: map[string]interface{}{"name": "test"},
		Count:            ptr(2),
	}

	verifySyncedRows(t, rows, expected, "r", "Iceberg", "mysql")
}

func TestVerifySyncedRows_RowsOverridesFallback(t *testing.T) {
	rows := []types.Row{
		mockRow{values: map[string]any{"_olake_id": "row-1", "id": int32(1), "name": "alice"}},
	}

	expected := ExpectedRows{
		Rows: map[string]map[string]interface{}{
			"1": {"name": "alice"},
		},
		FallbackTemplate: map[string]interface{}{"name": "not-alice"},
		KeyColumn:        "id",
	}

	verifySyncedRows(t, rows, expected, "r", "Iceberg", "mysql")
}

func TestVerifySyncedRows_Delete(t *testing.T) {
	// 0 rows expected and 0 rows returned -> passes
	verifySyncedRows(t, []types.Row{}, ExpectedRows{Count: ptr(0)}, "d", "Iceberg", "mysql")

	// Tombstone row returned with explicit Count: ptr(1) -> passes
	tombstone := []types.Row{
		mockRow{values: map[string]any{"_olake_id": "tombstone-1"}},
	}
	verifySyncedRows(t, tombstone, ExpectedRows{Count: ptr(1)}, "d", "Iceberg", "mysql")

	// Default Count (nil) defaults to maxAllowed = 1 -> passes
	verifySyncedRows(t, tombstone, ExpectedRows{}, "d", "Iceberg", "mysql")
}
