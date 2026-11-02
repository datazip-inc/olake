package require

import (
	"testing"

	trequire "github.com/stretchr/testify/require"
)

func TestMapDiff(t *testing.T) {
	ref := map[string]string{"id": "bigint", "data_blob": "string", "gone": "int"}
	upg := map[string]string{"id": "bigint", "data_blob": "binary", "added": "int"}
	want := "3 columns differ:\n" +
		"  column     reference  upgrade\n" +
		"  data_blob  string     binary\n" +
		"  gone       int        (absent)\n" +
		"  added      (absent)   int"
	if got := MapDiff("column", "reference", "upgrade", ref, upg); got != want {
		t.Fatalf("MapDiff rendered:\n%s\nwant:\n%s", got, want)
	}
	if got := MapDiff("column", "reference", "upgrade", ref, ref); got != "" {
		t.Fatalf("equal maps rendered %q, want nothing", got)
	}
	want = "1 op type differs:\n" +
		"  op type  reference  upgrade\n" +
		"  u        6          5"
	if got := MapDiff("op type", "reference", "upgrade", map[string]int64{"c": 1, "u": 6}, map[string]int64{"c": 1, "u": 5}); got != want {
		t.Fatalf("MapDiff rendered:\n%s\nwant:\n%s", got, want)
	}
	if got := MapDiff("key", "expected", "actual", "not a map", map[string]int{}); got != "" {
		t.Fatalf("non-map operand rendered %q, want nothing", got)
	}
	if got := MapDiff("key", "expected", "actual", map[int]string{1: "a"}, map[string]string{"1": "a"}); got != "" {
		t.Fatalf("mismatched key types rendered %q, want nothing", got)
	}
}

// A decoded catalog entry nests maps inside interfaces several levels deep; the difference is
// reported by its path, and the sibling that agrees is not.
func TestMapDiffDescendsIntoNestedMaps(t *testing.T) {
	entry := func(aType string) any {
		return map[string]any{
			"stream_name": "t",
			"type_schema": map[string]any{"properties": map[string]any{
				"a": map[string]any{"type": []any{aType}},
				"b": map[string]any{"type": []any{"int"}},
			}},
			"cursor": nil,
		}
	}
	got := MapDiff("field", "expected", "discovered", entry("string"), entry("binary"))
	want := "1 field differs:\n" +
		"  field                          expected  discovered\n" +
		"  type_schema.properties.a.type  [string]  [binary]"
	if got != want {
		t.Fatalf("MapDiff rendered:\n%s\nwant:\n%s", got, want)
	}
	if got := MapDiff("field", "expected", "discovered", entry("string"), entry("string")); got != "" {
		t.Fatalf("equal documents rendered %q, want nothing", got)
	}
}

func TestEqualReportsMapDiff(t *testing.T) {
	expected := map[string]string{"id": "bigint", "data_blob": "string"}
	actual := map[string]string{"id": "bigint", "data_blob": "binary"}
	summary := func() string { return MapDiff("key", "expected", "actual", expected, actual) }

	c := capture(func(c *failT) { trequire.Equal(c, expected, actual) }, summary)
	if !c.failed {
		t.Fatal("testify did not fail two unequal maps")
	}
	want := "1 key differs:\n" +
		"  key        expected  actual\n" +
		"  data_blob  string    binary"
	if last := c.messages[len(c.messages)-1]; last != want {
		t.Fatalf("Equal's report ends with:\n%s\nwant:\n%s", last, want)
	}

	c = capture(func(c *failT) { trequire.Equal(c, expected, expected) }, summary)
	if c.failed || len(c.messages) != 0 {
		t.Fatalf("equal maps reported %v", c.messages)
	}
}
