package types

import (
	"testing"
)

func TestGetFilter_Empty(t *testing.T) {
    s := &ConfiguredStream{
        StreamMetadata: StreamMetadata{Filter: ""},
        Stream:         NewStream("users", "public", nil),
    }

    f, err := s.GetFilter()
    if err != nil {
        t.Fatalf("expected no error for empty filter, got %v", err)
    }
    if len(f.Conditions) != 0 {
        t.Fatalf("expected 0 conditions for empty filter, got %d", len(f.Conditions))
    }
}

func TestGetFilter_SingleCondition(t *testing.T) {
    s := &ConfiguredStream{
        StreamMetadata: StreamMetadata{Filter: "age>30"},
        Stream:         NewStream("users", "public", nil),
    }

    f, err := s.GetFilter()
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if len(f.Conditions) != 1 {
        t.Fatalf("expected 1 condition, got %d", len(f.Conditions))
    }
    c := f.Conditions[0]
    if c.Column != "age" || c.Operator != ">" || c.Value != "30" {
        t.Fatalf("unexpected condition values: %+v", c)
    }
}

func TestGetFilter_TwoConditionsAnd(t *testing.T) {
    s := &ConfiguredStream{
        StreamMetadata: StreamMetadata{Filter: "age>=18 and score<100"},
        Stream:         NewStream("users", "public", nil),
    }

    f, err := s.GetFilter()
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if f.LogicalOperator != "and" {
        t.Fatalf("expected logical operator 'and', got '%s'", f.LogicalOperator)
    }
    if len(f.Conditions) != 2 {
        t.Fatalf("expected 2 conditions, got %d", len(f.Conditions))
    }
}

func TestCursor(t *testing.T) {
    st := NewStream("orders", "public", nil)
    st.CursorField = "created_at:updated_at"
    cs := st.Wrap(0)
    p, s2 := cs.Cursor()
    if p != "created_at" || s2 != "updated_at" {
        t.Fatalf("unexpected cursors: %s, %s", p, s2)
    }

    st2 := NewStream("items", "public", nil)
    st2.CursorField = "id"
    cs2 := st2.Wrap(0)
    p2, s3 := cs2.Cursor()
    if p2 != "id" || s3 != "" {
        t.Fatalf("unexpected cursors for single field: %s, %s", p2, s3)
    }
}

func TestGetDestinationDatabase_Precedence(t *testing.T) {
    st := NewStream("prod", "sales", nil)
    st.DestinationDatabase = "target_db"
    cs := st.Wrap(0)

    db := cs.GetDestinationDatabase(nil)
    if db != "target_db" {
        t.Fatalf("expected destination database to be target_db, got %s", db)
    }

    st2 := NewStream("prod", "sales", nil)
    cs2 := st2.Wrap(0)
    iceberg := "iceberg_db"
    db2 := cs2.GetDestinationDatabase(&iceberg)
    if db2 != "iceberg_db" {
        t.Fatalf("expected iceberg_db, got %s", db2)
    }
}

func TestGetDestinationTable_Default(t *testing.T) {
    st := NewStream("customers", "public", nil)
    st.DestinationTable = ""
    cs := st.Wrap(0)
    tbl := cs.GetDestinationTable()
    if tbl != "customers" {
        t.Fatalf("expected default destination table to be stream name, got %s", tbl)
    }
}

func TestValidate_InvalidSyncMode(t *testing.T) {
    source := NewStream("s", "ns", nil)
    source.SupportedSyncModes.Insert(FULLREFRESH)

    cfg := NewStream("s", "ns", nil)
    cfg.SyncMode = INCREMENTAL
    cs := cfg.Wrap(0)

    err := cs.Validate(source)
    if err == nil {
        t.Fatalf("expected error for invalid sync mode, got nil")
    }
}

func TestValidate_InvalidCursorField(t *testing.T) {
    source := NewStream("s", "ns", nil)
    source.SupportedSyncModes.Insert(INCREMENTAL)
    source.AvailableCursorFields.Insert("id")

    cfg := NewStream("s", "ns", nil)
    cfg.SyncMode = INCREMENTAL
    cfg.CursorField = "created_at"
    cs := cfg.Wrap(0)

    err := cs.Validate(source)
    if err == nil {
        t.Fatalf("expected error for invalid cursor field, got nil")
    }
}

func TestValidate_PrimaryKeyDifference(t *testing.T) {
    source := NewStream("s", "ns", nil)
    source.SupportedSyncModes.Insert(FULLREFRESH)
    source.SourceDefinedPrimaryKey.Insert("id")

    cfg := NewStream("s", "ns", nil)
    cfg.SyncMode = FULLREFRESH
    cfg.SourceDefinedPrimaryKey.Insert("id", "name")
    cs := cfg.Wrap(0)

    // source has proper subset of cfg primary keys -> should error
    err := cs.Validate(source)
    if err == nil {
        t.Fatalf("expected error for primary key difference, got nil")
    }
}

func TestNormalizationEnabled(t *testing.T) {
    st := NewStream("s", "ns", nil)
    cs := st.Wrap(0)
    cs.StreamMetadata.Normalization = true
    if !cs.NormalizationEnabled() {
        t.Fatalf("expected normalization to be enabled")
    }
}
