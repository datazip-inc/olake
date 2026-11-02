package integration

import (
	"maps"
	"testing"
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
