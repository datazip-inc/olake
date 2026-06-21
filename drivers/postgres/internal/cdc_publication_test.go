package driver

import (
	"errors"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

type mockStream struct {
	name      string
	namespace string
}

func (m *mockStream) Name() string      { return m.name }
func (m *mockStream) Namespace() string { return m.namespace }
func (m *mockStream) ID() string {
	if m.namespace != "" {
		return m.namespace + "." + m.name
	}
	return m.name
}

// Remaining interface stubs — not exercised by these tests.
func (m *mockStream) Self() *types.ConfiguredStream { return nil }
func (m *mockStream) Schema() *types.TypeSchema     { return nil }
func (m *mockStream) GetStream() *types.Stream      { return nil }
func (m *mockStream) GetSyncMode() types.SyncMode   { return "" }
func (m *mockStream) GetFilter() (types.FilterConfig, bool, error) {
	return types.FilterConfig{}, false, nil
}
func (m *mockStream) SupportedSyncModes() *types.Set[types.SyncMode] { return nil }
func (m *mockStream) Cursor() (string, string)                       { return "", "" }
func (m *mockStream) Validate(_ *types.Stream) error                 { return nil }
func (m *mockStream) NormalizationEnabled() bool                     { return false }
func (m *mockStream) GetDestinationDatabase(_ *string) string        { return "" }
func (m *mockStream) GetDestinationTable() string                    { return "" }
func (m *mockStream) RetainSelectedColumns() func(map[string]interface{}) map[string]interface{} {
	return func(r map[string]interface{}) map[string]interface{} { return r }
}
func (m *mockStream) IsSelectedColumn() func(string) bool {
	return func(_ string) bool { return true }
}

func ms(schema, table string) types.StreamInterface {
	return &mockStream{name: table, namespace: schema}
}

// Tests for checkStreamsInPublication

func TestCheckStreamsInPublication(t *testing.T) {
	tests := []struct {
		name           string
		publication    string
		pubTables      []pubTable
		streams        []types.StreamInterface
		wantErr        bool
		errContains    []string
		isNonRetryable bool
	}{
		{
			name:        "happy path — all streams present, matched via ID()",
			publication: "my_pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "orders"},
				{Schema: "public", Table: "users"},
			},
			streams: []types.StreamInterface{
				ms("public", "orders"),
				ms("public", "users"),
			},
			wantErr: false,
		},
		{
			name:        "one stream missing from publication",
			publication: "my_pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "orders"},
			},
			streams: []types.StreamInterface{
				ms("public", "orders"),
				ms("public", "payments"), // not in publication
			},
			wantErr:        true,
			errContains:    []string{"public.payments", "ALTER PUBLICATION", "my_pub"},
			isNonRetryable: true,
		},
		{
			name:        "all streams missing from publication",
			publication: "pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "other_table"},
			},
			streams: []types.StreamInterface{
				ms("public", "orders"),
				ms("analytics", "events"),
			},
			wantErr:        true,
			errContains:    []string{"public.orders", "analytics.events"},
			isNonRetryable: true,
		},
		{
			name:           "publication has no tables at all",
			publication:    "empty_pub",
			pubTables:      []pubTable{},
			streams:        []types.StreamInterface{ms("public", "orders")},
			wantErr:        true,
			errContains:    []string{"empty_pub", "no tables"},
			isNonRetryable: true,
		},
		{
			name:        "case-insensitive match — DB side uppercase, stream.ID() lowercase",
			publication: "pub",
			pubTables: []pubTable{
				{Schema: "PUBLIC", Table: "ORDERS"},
			},
			streams: []types.StreamInterface{
				ms("public", "orders"),
			},
			wantErr: false,
		},
		{
			name:        "case-insensitive match — stream.ID() uppercase, DB side lowercase",
			publication: "pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "orders"},
			},
			streams: []types.StreamInterface{
				ms("PUBLIC", "ORDERS"),
			},
			wantErr: false,
		},
		{
			name:        "multiple schemas — correct matching via ID()",
			publication: "cross_schema_pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "orders"},
				{Schema: "analytics", Table: "events"},
			},
			streams: []types.StreamInterface{
				ms("public", "orders"),
				ms("analytics", "events"),
			},
			wantErr: false,
		},
		{
			name:        "same table name different schema — must not cross-match",
			publication: "pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "orders"}, // only public.orders is covered
			},
			streams: []types.StreamInterface{
				ms("analytics", "orders"), // analytics.orders is NOT covered
			},
			wantErr:        true,
			errContains:    []string{"analytics.orders"},
			isNonRetryable: true,
		},
		{
			name:        "no streams selected — no error",
			publication: "pub",
			pubTables: []pubTable{
				{Schema: "public", Table: "orders"},
			},
			streams: nil,
			wantErr: false,
		},
		{
			name:        "single stream present — no error",
			publication: "pub",
			pubTables:   []pubTable{{Schema: "public", Table: "orders"}},
			streams:     []types.StreamInterface{ms("public", "orders")},
			wantErr:     false,
		},
		{
			name:        "error message includes ALTER PUBLICATION hint with all missing tables",
			publication: "mypub",
			pubTables:   []pubTable{{Schema: "public", Table: "other"}},
			streams: []types.StreamInterface{
				ms("public", "t1"),
				ms("public", "t2"),
			},
			wantErr:     true,
			errContains: []string{"ALTER PUBLICATION mypub ADD TABLE", "public.t1", "public.t2"},
		},
		{
			name:        "missing table names in error use stream.ID() format",
			publication: "pub",
			pubTables:   []pubTable{{Schema: "public", Table: "other"}},
			streams: []types.StreamInterface{
				ms("sales", "orders"),
			},
			wantErr:     true,
			errContains: []string{"sales.orders"}, // exact ID() format, not reconstructed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkStreamsInPublication(tt.publication, tt.pubTables, tt.streams)

			if (err != nil) != tt.wantErr {
				t.Fatalf("wantErr=%v but got err=%v", tt.wantErr, err)
			}

			if err == nil {
				return
			}

			for _, substr := range tt.errContains {
				if !strings.Contains(err.Error(), substr) {
					t.Errorf("error must contain %q\ngot: %v", substr, err)
				}
			}

			if tt.isNonRetryable && !errors.Is(err, constants.ErrNonRetryable) {
				t.Errorf("expected ErrNonRetryable to be wrapped in error, got: %v", err)
			}
		})
	}
}

func TestStreamID_FormatAssumption(t *testing.T) {
	s := ms("public", "orders")
	want := "public.orders"
	if s.ID() != want {
		t.Fatalf("mockStream.ID() = %q, want %q — checkStreamsInPublication assumes namespace.name format", s.ID(), want)
	}
}
