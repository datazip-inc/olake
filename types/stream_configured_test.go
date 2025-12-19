package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFilter(t *testing.T) {
    testCases := []struct {
        name       string
        filter     string
        wantErr    bool
        wantConds  int
        wantColumn string
        wantOp     string
        wantValue  string
        wantLogic  string
    }{
        {
            name:      "Empty filter",
            filter:    "",
            wantErr:   false,
            wantConds: 0,
        },
        {
            name:       "Single condition",
            filter:     "age>30",
            wantErr:    false,
            wantConds:  1,
            wantColumn: "age",
            wantOp:     ">",
            wantValue:  "30",
        },
        {
            name:      "Two conditions with AND",
            filter:    "age>=18 and score<100",
            wantErr:   false,
            wantConds: 2,
            wantLogic: "and",
        },
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

            if tc.wantConds == 1 {
                c := f.Conditions[0]
                assert.Equal(t, tc.wantColumn, c.Column)
                assert.Equal(t, tc.wantOp, c.Operator)
                assert.Equal(t, tc.wantValue, c.Value)
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
        {
            name:          "Two cursor fields",
            cursorField:   "created_at:updated_at",
            wantPrimary:   "created_at",
            wantSecondary: "updated_at",
        },
        {
            name:          "Single cursor field",
            cursorField:   "id",
            wantPrimary:   "id",
            wantSecondary: "",
        },
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
    iceberg := "iceberg_db"

    testCases := []struct {
        name                 string
        destinationDatabase  string
        icebergOverride      *string
        wantDB               string
    }{
        {
            name:                "Destination database precedence",
            destinationDatabase: "target_db",
            icebergOverride:     nil,
            wantDB:              "target_db",
        },
        {
            name:                "Fallback to iceberg override",
            destinationDatabase: "",
            icebergOverride:     &iceberg,
            wantDB:              "iceberg_db",
        },
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            st := NewStream("prod", "sales", nil)
            st.DestinationDatabase = tc.destinationDatabase
            cs := st.Wrap(0)

            got := cs.GetDestinationDatabase(tc.icebergOverride)
            assert.Equal(t, tc.wantDB, got)
        })
    }
}

func TestGetDestinationTable(t *testing.T) {
    testCases := []struct {
        name              string
        streamName        string
        destinationTable  string
        wantTable         string
    }{
        {
            name:             "Default to stream name when empty",
            streamName:       "customers",
            destinationTable: "",
            wantTable:        "customers",
        },
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
        name              string
        setupSource       func() *Stream
        setupCfg          func() *Stream
        wantErr           bool
    }{
        {
            name: "Invalid sync mode not supported by source",
            setupSource: func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(FULLREFRESH)
                return s
            },
            setupCfg: func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = INCREMENTAL
                return c
            },
            wantErr: true,
        },
        {
            name: "Invalid cursor field not available in source",
            setupSource: func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(INCREMENTAL)
                s.AvailableCursorFields.Insert("id")
                return s
            },
            setupCfg: func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = INCREMENTAL
                c.CursorField = "created_at"
                return c
            },
            wantErr: true,
        },
        {
            name: "Primary key difference (cfg has superset of source)",
            setupSource: func() *Stream {
                s := NewStream("s", "ns", nil)
                s.SupportedSyncModes.Insert(FULLREFRESH)
                s.SourceDefinedPrimaryKey.Insert("id")
                return s
            },
            setupCfg: func() *Stream {
                c := NewStream("s", "ns", nil)
                c.SyncMode = FULLREFRESH
                c.SourceDefinedPrimaryKey.Insert("id", "name")
                return c
            },
            wantErr: true,
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
}
