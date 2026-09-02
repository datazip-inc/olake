package testutils

import "testing"

// The cases that must still compare equal are the ones the helper exists for: the catalog is
// marshaled from maps and sync.Map ranges, so key order and array order are both arbitrary.
// The cases that must differ are the ones a rune-sorting comparison used to accept.
func TestNormalizedEqual(t *testing.T) {
	const catalog = `{"stream":{"name":"users","namespace":"public","type_schema":{"properties":{"col_int":{"type":["integer","null"]},"col_text":{"type":["string","null"]}}}}}`

	for _, tc := range []struct {
		name  string
		other string
		equal bool
	}{
		{"identical", catalog, true},
		{"whitespace and indentation", "{\n  \"stream\": {\n    \"name\": \"users\",\n    \"namespace\": \"public\",\n    \"type_schema\": {\"properties\": {\"col_int\": {\"type\": [\"integer\", \"null\"]}, \"col_text\": {\"type\": [\"string\", \"null\"]}}}\n  }\n}", true},
		{"object key order", `{"stream":{"namespace":"public","type_schema":{"properties":{"col_text":{"type":["string","null"]},"col_int":{"type":["integer","null"]}}},"name":"users"}}`, true},
		{"array order", `{"stream":{"name":"users","namespace":"public","type_schema":{"properties":{"col_int":{"type":["null","integer"]},"col_text":{"type":["null","string"]}}}}}`, true},

		{"column names reversed", `{"stream":{"name":"users","namespace":"public","type_schema":{"properties":{"tni_loc":{"type":["integer","null"]},"txet_loc":{"type":["string","null"]}}}}}`, false},
		{"two columns swap types", `{"stream":{"name":"users","namespace":"public","type_schema":{"properties":{"col_int":{"type":["string","null"]},"col_text":{"type":["integer","null"]}}}}}`, false},
		{"name and namespace swapped", `{"stream":{"name":"public","namespace":"users","type_schema":{"properties":{"col_int":{"type":["integer","null"]},"col_text":{"type":["string","null"]}}}}}`, false},
		{"a column dropped", `{"stream":{"name":"users","namespace":"public","type_schema":{"properties":{"col_int":{"type":["integer","null"]}}}}}`, false},
		{"not json", `col_int`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizedEqual(catalog, tc.other); got != tc.equal {
				t.Fatalf("NormalizedEqual = %v, want %v", got, tc.equal)
			}
		})
	}
}
