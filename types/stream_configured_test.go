package types

import (
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestIDAndSelfAndNameAndNamespaceAndSchema(t *testing.T) {
    st := NewStream("users", "public", nil)
    cs := st.Wrap(42)

    assert.Equal(t, 42, cs.ID())
    assert.Equal(t, cs, cs.Self())
    assert.Equal(t, "users", cs.Name())
    assert.Equal(t, "public", cs.Namespace())
    assert.Equal(t, "", cs.Schema()) // adjust if Schema() has default
}

func TestGetStream(t *testing.T) {
    st := NewStream("orders", "sales", nil)
    cs := st.Wrap(1)
    assert.Equal(t, st, cs.GetStream())
}

func TestSupportedSyncModesAndGetSyncMode(t *testing.T) {
    st := NewStream("s", "ns", nil)
    st.SupportedSyncModes.Insert(FULLREFRESH, INCREMENTAL, CDC, STRICTCDC)
    st.SyncMode = INCREMENTAL
    cs := st.Wrap(0)

    assert.True(t, cs.SupportedSyncModes().Contains(INCREMENTAL))
    assert.Equal(t, INCREMENTAL, cs.GetSyncMode())
}

func TestGetFilter(t *testing.T) {
    testCases := []struct {
        name       string
        filter     string
        wantErr    bool
        wantConds  int
        wantLogic  string
    }{
        {"Empty filter", "", false, 0, ""},
        {"Operator <", "age<40", false, 1, ""},
        {"Operator <=", "age<=40", false, 1, ""},
        {"Operator !=", "age!=40", false, 1, ""},
        {"Operator =", "age=40", false, 1, ""},
        {"Quoted string", "name='Alice'", false, 1, ""},
        {"OR logic", "age>18 or score<50", false, 2, "or"},
        {"Invalid format", "age>>", true, 0, ""},
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            s := &ConfiguredStream{
                StreamMetadata: StreamMetadata{Filter: tc.filter},
                Stream:         NewStream("users", "public", nil),
            }
            f, err := s.GetFilter()
            if tc.wantErr {
                assert.Error(t, err)
                return
            }
            assert.NoError(t, err)
            assert.Equal(t, tc.wantConds, len(f.Conditions))
            if tc.wantLogic != "" {
                assert.Equal(t, tc.wantLogic, f.LogicalOperator)
            }
            if tc.wantConds == 2 {
                assert.NotEmpty(t, f.Conditions[1].Column)
            }
        })
    }
}

func TestCursor(t *testing.T) {
    testCases := []struct {
        name        string
        cursorField string
        wantPrimary string
        wantSecondary string
    }{
        {"Empty field", "", "", ""},
        {"Multiple colons", "a:b:c", "a", "b"}, // only first two considered
        {"Whitespace handling", " id : created_at ", "id", "created_at"},
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            st := NewStream("orders", "public", nil)
            st.CursorField = tc.cursorField
            cs := st.Wrap(0)
            p, s := cs.Cursor()
            assert.Equal(t, tc.wantPrimary, p)
            assert.Equal(t, tc.wantSecondary, s)
        })
    }
}

func TestGetDestinationDatabase(t *testing.T) {
    testCases := []struct {
        name                string
        destinationDatabase string
        icebergOverride     *string
        namespace           string
        wantDB              string
    }{
        {"Destination precedence", "target_db", nil, "ns", "target_db"},
        {"Fallback to iceberg", "", strPtr("iceberg_db"), "ns", "iceberg_db"},
        {"Namespace fallback", "", nil, "ns", "ns"},
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            st := NewStream("prod", tc.namespace, nil)
            st.DestinationDatabase = tc.destinationDatabase
            cs := st.Wrap(0)
            got := cs.GetDestinationDatabase(tc.icebergOverride)
            assert.Equal(t, tc.wantDB, got)
        })
    }
}

func TestGetDestinationTable(t *testing.T) {
    testCases := []struct {
        name             string
        streamName       string
        destinationTable string
        wantTable        string
    }{
        {"Default to stream name", "customers", "", "customers"},
        {"Provided destination table", "customers", "cust_tbl", "cust_tbl"},
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            st := NewStream(tc.streamName, "public", nil)
            st.DestinationTable = tc.destinationTable
            cs := st.Wrap(0)
            got := cs.GetDestinationTable()
            assert.Equal(t, tc.wantTable, got)
        })
    }
}

func TestValidate(t *testing.T) {
    testCases := []struct {
        name        string
        setupSource func() *Stream
        setupCfg    func() *Stream
        wantErr     bool
    }{
        {
            "Valid FULLREFRESH",
            func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(FULLREFRESH)
                return s
            },
            func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = FULLREFRESH
                return c
            },
            false,
        },
        {
            "Valid INCREMENTAL with cursor",
            func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(INCREMENTAL)
                s.AvailableCursorFields.Insert("id", "created_at")
                return s
            },
            func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = INCREMENTAL
                c.CursorField = "id:created_at"
                return c
            },
            false,
        },
        {
            "CDC skips cursor validation",
            func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(CDC)
                return s
            },
            func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = CDC
                c.CursorField = "invalid"
                return c
            },
            false,
        },
        {
            "Matching primary keys",
            func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(FULLREFRESH)
                s.SourceDefinedPrimaryKey.Insert("id", "name")
                return s
            },
            func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = FULLREFRESH
                c.SourceDefinedPrimaryKey.Insert("id", "name")
                return c
            },
            false,
        },
        {
            "Invalid filter in metadata",
            func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(FULLREFRESH)
                return s
            },
            func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = FULLREFRESH
                c.StreamMetadata.Filter = "age>>"
                return c
            },
            true,
        },
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            source := tc.setupSource()
            cfg := tc.setupCfg()
            cs := cfg.Wrap(0)
            err := cs.Validate(source)
            if tc.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}

func TestNormalizationEnabled(t *testing.T) {
    st := NewStream("s", "ns", nil)
    cs := st.Wrap(0)
    cs.StreamMetadata.Normalization = true
    assert.True(t, cs.NormalizationEnabled())

    cs.StreamMetadata.Normalization = false
    assert.False(t, cs.NormalizationEnabled())
}

// helper
func strPtr(s string) *string { return &s }
