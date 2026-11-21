package driver

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestMSSQLState creates an initialized state and directs state persistence to a
// temporary file so SetCursor can safely persist test state.
func newTestMSSQLState(t *testing.T) *types.State {
	t.Helper()

	previousPath := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() {
		viper.Set(constants.StatePath, previousPath)
	})

	return &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.StreamType,
	}
}

func testLSN(b byte) []byte {
	res := make([]byte, 10)
	res[9] = b
	return res
}

// newMSSQLCDCRows creates a typed *sqlmock.Rows fixture with standard SQL Server CDC metadata columns:
// __$start_lsn: BINARY(10)
// __$seqval: BINARY(10)
// __$operation: INT
// __$update_mask: VARBINARY
// Any additional captured data columns (e.g. id INT, name VARCHAR) can be passed via extraCols.
func newMSSQLCDCRows(extraCols ...*sqlmock.Column) *sqlmock.Rows {
	cols := []*sqlmock.Column{
		sqlmock.NewColumn("__$start_lsn").OfType("BINARY", []byte{}).WithLength(10),
		sqlmock.NewColumn("__$seqval").OfType("BINARY", []byte{}).WithLength(10),
		sqlmock.NewColumn("__$operation").OfType("INT", int64(0)),
		sqlmock.NewColumn("__$update_mask").OfType("VARBINARY", []byte{}),
	}
	cols = append(cols, extraCols...)
	return sqlmock.NewRowsWithColumnDefinition(cols...)
}

func TestExtractMetadataHex(t *testing.T) {
	validBytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
	validString := string(validBytes)
	expectedHex := "00010203040506070809"

	t.Run("valid 10-byte byte slice", func(t *testing.T) {
		record := map[string]interface{}{"__$start_lsn": validBytes}
		res, err := extractMetadataHex(record, "__$start_lsn")
		require.NoError(t, err)
		assert.Equal(t, expectedHex, res)
	})

	t.Run("valid 10-byte string", func(t *testing.T) {
		record := map[string]interface{}{"__$start_lsn": validString}
		res, err := extractMetadataHex(record, "__$start_lsn")
		require.NoError(t, err)
		assert.Equal(t, expectedHex, res)
	})

	t.Run("canonical lowercase hex", func(t *testing.T) {
		bytesWithUpper := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44}
		record := map[string]interface{}{"__$start_lsn": bytesWithUpper}
		res, err := extractMetadataHex(record, "__$start_lsn")
		require.NoError(t, err)
		assert.Equal(t, "aabbccddeeff11223344", res)
	})

	t.Run("missing column", func(t *testing.T) {
		record := map[string]interface{}{}
		_, err := extractMetadataHex(record, "__$start_lsn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing")
	})

	t.Run("nil column", func(t *testing.T) {
		record := map[string]interface{}{"__$start_lsn": nil}
		_, err := extractMetadataHex(record, "__$start_lsn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("unsupported type", func(t *testing.T) {
		record := map[string]interface{}{"__$start_lsn": 12345}
		_, err := extractMetadataHex(record, "__$start_lsn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")
	})

	t.Run("short byte length (8 bytes)", func(t *testing.T) {
		record := map[string]interface{}{"__$start_lsn": []byte{1, 2, 3, 4, 5, 6, 7, 8}}
		_, err := extractMetadataHex(record, "__$start_lsn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte length 8")
	})

	t.Run("long byte length (12 bytes)", func(t *testing.T) {
		record := map[string]interface{}{"__$start_lsn": string([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})}
		_, err := extractMetadataHex(record, "__$start_lsn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte length 12")
	})
}

func TestExtractOperationCode(t *testing.T) {
	t.Run("valid int32 codes 1-4", func(t *testing.T) {
		for code := int32(1); code <= 4; code++ {
			record := map[string]interface{}{"__$operation": code}
			res, err := extractOperationCode(record)
			require.NoError(t, err)
			assert.Equal(t, code, res)
		}
	})

	t.Run("valid alternate integer representations", func(t *testing.T) {
		res, err := extractOperationCode(map[string]interface{}{"__$operation": int64(2)})
		require.NoError(t, err)
		assert.Equal(t, int32(2), res)

		res, err = extractOperationCode(map[string]interface{}{"__$operation": int(3)})
		require.NoError(t, err)
		assert.Equal(t, int32(3), res)

		res, err = extractOperationCode(map[string]interface{}{"__$operation": int16(1)})
		require.NoError(t, err)
		assert.Equal(t, int32(1), res)

		res, err = extractOperationCode(map[string]interface{}{"__$operation": int8(4)})
		require.NoError(t, err)
		assert.Equal(t, int32(4), res)
	})

	t.Run("int64 overflow rejection", func(t *testing.T) {
		record := map[string]interface{}{"__$operation": int64(math.MaxInt64)}
		_, err := extractOperationCode(record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows int32")

		record = map[string]interface{}{"__$operation": int64(math.MinInt64)}
		_, err = extractOperationCode(record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows int32")
	})

	t.Run("missing operation column", func(t *testing.T) {
		record := map[string]interface{}{}
		_, err := extractOperationCode(record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing")
	})

	t.Run("nil operation column", func(t *testing.T) {
		record := map[string]interface{}{"__$operation": nil}
		_, err := extractOperationCode(record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("unsupported type float or string", func(t *testing.T) {
		record := map[string]interface{}{"__$operation": 3.0}
		_, err := extractOperationCode(record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")

		record = map[string]interface{}{"__$operation": "3"}
		_, err = extractOperationCode(record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")
	})

	t.Run("out-of-range operation codes", func(t *testing.T) {
		for _, code := range []int32{0, 5, -1, 100} {
			record := map[string]interface{}{"__$operation": code}
			_, err := extractOperationCode(record)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported CDC operation code")
		}
	})
}

func TestHasPrimaryKeyChanged(t *testing.T) {
	t.Run("single PK unchanged", func(t *testing.T) {
		oldRec := map[string]interface{}{"id": int32(1), "val": "old"}
		newRec := map[string]interface{}{"id": int32(1), "val": "new"}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"id"})
		require.NoError(t, err)
		assert.False(t, changed)
	})

	t.Run("single PK changed", func(t *testing.T) {
		oldRec := map[string]interface{}{"id": int32(1), "val": "a"}
		newRec := map[string]interface{}{"id": int32(2), "val": "a"}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"id"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("composite PK unchanged", func(t *testing.T) {
		oldRec := map[string]interface{}{"tenant_id": "t1", "user_id": int32(10)}
		newRec := map[string]interface{}{"tenant_id": "t1", "user_id": int32(10)}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"tenant_id", "user_id"})
		require.NoError(t, err)
		assert.False(t, changed)
	})

	t.Run("composite PK partially changed", func(t *testing.T) {
		oldRec := map[string]interface{}{"tenant_id": "t1", "user_id": int32(10)}
		newRec := map[string]interface{}{"tenant_id": "t2", "user_id": int32(10)}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"tenant_id", "user_id"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("composite PK fully changed", func(t *testing.T) {
		oldRec := map[string]interface{}{"tenant_id": "t1", "user_id": int32(10)}
		newRec := map[string]interface{}{"tenant_id": "t2", "user_id": int32(20)}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"tenant_id", "user_id"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("delimiter framing collision safety", func(t *testing.T) {
		oldRec := map[string]interface{}{"a": "x|y", "b": "z"}
		newRec := map[string]interface{}{"a": "x", "b": "y|z"}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"a", "b"})
		require.NoError(t, err)
		assert.True(t, changed, "component-by-component comparison must detect changed components despite pipe delimiters")
	})

	t.Run("empty PK list rejected", func(t *testing.T) {
		oldRec := map[string]interface{}{"id": 1}
		newRec := map[string]interface{}{"id": 1}
		_, err := hasPrimaryKeyChanged(oldRec, newRec, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "primary key columns list cannot be empty")
	})

	t.Run("missing PK column in before-image", func(t *testing.T) {
		oldRec := map[string]interface{}{"name": "foo"}
		newRec := map[string]interface{}{"id": 1, "name": "foo"}
		_, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"id"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing in before-image")
	})

	t.Run("missing PK column in after-image", func(t *testing.T) {
		oldRec := map[string]interface{}{"id": 1, "name": "foo"}
		newRec := map[string]interface{}{"name": "foo"}
		_, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"id"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing in after-image")
	})

	t.Run("nil PK value in before-image", func(t *testing.T) {
		oldRec := map[string]interface{}{"id": nil}
		newRec := map[string]interface{}{"id": 1}
		_, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"id"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is nil in before-image")
	})

	t.Run("nil PK value in after-image", func(t *testing.T) {
		oldRec := map[string]interface{}{"id": 1}
		newRec := map[string]interface{}{"id": nil}
		_, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"id"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is nil in after-image")
	})

	t.Run("empty string PK value", func(t *testing.T) {
		oldRec := map[string]interface{}{"code": ""}
		newRec := map[string]interface{}{"code": ""}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"code"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"code": "A"}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"code"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Float32 coverage: equal vs different representations
	t.Run("float32 PK values", func(t *testing.T) {
		oldRec := map[string]interface{}{"val": float32(3.14159)}
		newRec := map[string]interface{}{"val": float32(3.14159)}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"val"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"val": float32(2.71828)}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"val"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Float64 coverage: equal vs different representations
	t.Run("float64 PK values", func(t *testing.T) {
		oldRec := map[string]interface{}{"val": float64(12345.6789)}
		newRec := map[string]interface{}{"val": float64(12345.6789)}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"val"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"val": float64(98765.4321)}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"val"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Exact decimal coverage: same declared type (realistic SQL Server CDC column comparison)
	t.Run("exact decimal PK values - same declared type", func(t *testing.T) {
		dec1, err := typeutils.ReformatDecimal(types.DecimalOf(10, 2), "123.45")
		require.NoError(t, err)
		dec2, err := typeutils.ReformatDecimal(types.DecimalOf(10, 2), "123.45")
		require.NoError(t, err)

		// Equal decimal values with identical precision and scale (10, 2)
		oldRec := map[string]interface{}{"dec": dec1}
		newRec := map[string]interface{}{"dec": dec2}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"dec"})
		require.NoError(t, err)
		assert.False(t, changed)

		// Different numeric decimal values with identical precision and scale (10, 2)
		dec3, err := typeutils.ReformatDecimal(types.DecimalOf(10, 2), "999.99")
		require.NoError(t, err)
		newRecDiff := map[string]interface{}{"dec": dec3}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"dec"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Exact decimal token serialization behavior across different scales:
	// In SQL Server CDC, before-image and after-image rows for a single column always share
	// the declared precision and scale. This test isolates the underlying key-token serialization
	// behavior of typeutils.Decimal when different scales are presented, ensuring that string tokens
	// reflect scale distinctions rather than assuming an unrealistic intra-column scale change.
	t.Run("exact decimal PK values - token serialization behavior across scales", func(t *testing.T) {
		decScale1, err := typeutils.ReformatDecimal(types.DecimalOf(10, 1), "12.3")
		require.NoError(t, err)
		decScale2, err := typeutils.ReformatDecimal(types.DecimalOf(10, 2), "12.30")
		require.NoError(t, err)

		oldRecScale := map[string]interface{}{"dec": decScale1}
		newRecScale := map[string]interface{}{"dec": decScale2}
		changed, err := hasPrimaryKeyChanged(oldRecScale, newRecScale, []string{"dec"})
		require.NoError(t, err)
		assert.True(t, changed, "scale differences in exact decimals must produce distinct key tokens")
	})

	// Decimal floating-point fallback coverage (distinct from exact decimal)
	t.Run("decimal float64 fallback PK values", func(t *testing.T) {
		oldRec := map[string]interface{}{"dec_fallback": float64(12.34)}
		newRec := map[string]interface{}{"dec_fallback": float64(12.34)}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"dec_fallback"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"dec_fallback": float64(56.78)}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"dec_fallback"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("UUID string PK values", func(t *testing.T) {
		u1 := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
		u2 := "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"
		oldRec := map[string]interface{}{"uuid": u1}
		newRec := map[string]interface{}{"uuid": u1}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"uuid"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"uuid": u2}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"uuid"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Time representations: time.Time
	t.Run("time.Time PK values", func(t *testing.T) {
		now := time.Now().UTC()
		later := now.Add(1 * time.Hour)
		oldRec := map[string]interface{}{"ts": now}
		newRec := map[string]interface{}{"ts": now}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"ts"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"ts": later}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"ts"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Time representations: MSSQL time string representation (produced by ReformatTimeValue)
	t.Run("MSSQL time string PK values", func(t *testing.T) {
		oldRec := map[string]interface{}{"t_col": "14:30:00"}
		newRec := map[string]interface{}{"t_col": "14:30:00"}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"t_col"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"t_col": "15:45:30.1234567"}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"t_col"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	// Hierarchyid representations: path strings and hex strings
	t.Run("hierarchyid string PK values", func(t *testing.T) {
		// Path format representation
		oldRecPath := map[string]interface{}{"h_path": "/1/2/3/"}
		newRecPath := map[string]interface{}{"h_path": "/1/2/3/"}
		changed, err := hasPrimaryKeyChanged(oldRecPath, newRecPath, []string{"h_path"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecPathDiff := map[string]interface{}{"h_path": "/1/2/4/"}
		changed, err = hasPrimaryKeyChanged(oldRecPath, newRecPathDiff, []string{"h_path"})
		require.NoError(t, err)
		assert.True(t, changed)

		// Hex format representation (produced by connector for binary hierarchyid)
		oldRecHex := map[string]interface{}{"h_hex": "58"}
		newRecHex := map[string]interface{}{"h_hex": "58"}
		changed, err = hasPrimaryKeyChanged(oldRecHex, newRecHex, []string{"h_hex"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecHexDiff := map[string]interface{}{"h_hex": "5ac0"}
		changed, err = hasPrimaryKeyChanged(oldRecHex, newRecHexDiff, []string{"h_hex"})
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("binary string PK values", func(t *testing.T) {
		bin1 := "\x00\x01\x02\x03"
		bin2 := "\x00\x01\x02\x04"
		oldRec := map[string]interface{}{"bin": bin1}
		newRec := map[string]interface{}{"bin": bin1}
		changed, err := hasPrimaryKeyChanged(oldRec, newRec, []string{"bin"})
		require.NoError(t, err)
		assert.False(t, changed)

		newRecDiff := map[string]interface{}{"bin": bin2}
		changed, err = hasPrimaryKeyChanged(oldRec, newRecDiff, []string{"bin"})
		require.NoError(t, err)
		assert.True(t, changed)
	})
}

func TestMSSQLFetchTableChangesInLSNRange_Operation3Paired_UnchangedPK(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, err := hex.DecodeString(fromLSNHex)
	require.NoError(t, err)
	advancedLSNBytes, err := hex.DecodeString(advancedLSNHex)
	require.NoError(t, err)
	toLSNBytes, err := hex.DecodeString(toLSNHex)
	require.NoError(t, err)

	// 1. Mock advanceLSN query
	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	// 2. Mock CDC changes query
	capture := captureInstance{
		instanceName: "dbo_test_table",
	}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("name").OfType("VARCHAR", ""),
	).
		AddRow(lsnBytes, seqvalBytes, int32(1), updateMask, int32(1), "deleted_row").
		AddRow(lsnBytes, seqvalBytes, int32(2), updateMask, int32(2), "inserted_row").
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(3), "before_update_row").
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(3), "after_update_row")

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	var emittedChanges []abstract.CDCChange
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		emittedChanges = append(emittedChanges, change)
		return nil
	}

	err = m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.NoError(t, err)

	// Verify operation 3 was paired with operation 4: unchanged PK emits exactly one update
	require.Len(t, emittedChanges, 3, "expected 3 events: delete, insert, update")

	assert.Equal(t, "delete", emittedChanges[0].Kind)
	assert.Equal(t, "insert", emittedChanges[1].Kind)
	assert.Equal(t, "update", emittedChanges[2].Kind)

	expectedLSNHex := fmt.Sprintf("%x", lsnBytes)
	expectedSeqvalHex := fmt.Sprintf("%x", seqvalBytes)

	// Verify metadata columns populated in extraColumns and stripped from Data
	for _, ch := range emittedChanges {
		assert.Equal(t, expectedLSNHex, ch.ExtraColumns[CDCStartLSN])
		assert.Equal(t, expectedSeqvalHex, ch.ExtraColumns[CDCSeqVal])
		assert.NotContains(t, ch.Data, "__$operation")
		assert.NotContains(t, ch.Data, "__$start_lsn")
		assert.NotContains(t, ch.Data, "__$seqval")
		assert.NotContains(t, ch.Data, "__$update_mask")
	}
	assert.Equal(t, int32(3), emittedChanges[2].Data["id"])
	assert.Equal(t, "after_update_row", emittedChanges[2].Data["name"])
}

func TestMSSQLFetchTableChangesInLSNRange_ChangedPK(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("name").OfType("VARCHAR", ""),
	).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(10), "old_name").
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(20), "new_name")

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	var emittedChanges []abstract.CDCChange
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		emittedChanges = append(emittedChanges, change)
		return nil
	}

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.NoError(t, err)

	// PK changed: must emit DELETE old image, then INSERT new image
	require.Len(t, emittedChanges, 2)
	assert.Equal(t, "delete", emittedChanges[0].Kind)
	assert.Equal(t, int32(10), emittedChanges[0].Data["id"])
	assert.Equal(t, "old_name", emittedChanges[0].Data["name"])

	assert.Equal(t, "insert", emittedChanges[1].Kind)
	assert.Equal(t, int32(20), emittedChanges[1].Data["id"])
	assert.Equal(t, "new_name", emittedChanges[1].Data["name"])
}

func TestMSSQLFetchTableChangesInLSNRange_CompositePK_Changed(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(5)
	seqvalBytes := testLSN(6)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(
		sqlmock.NewColumn("tenant_id").OfType("VARCHAR", ""),
		sqlmock.NewColumn("user_id").OfType("INT", int64(0)),
		sqlmock.NewColumn("role").OfType("VARCHAR", ""),
	).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, "tenant_A", int32(1), "admin").
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, "tenant_B", int32(1), "admin")

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("tenant_id", "user_id"),
	}).Wrap(0)

	var emittedChanges []abstract.CDCChange
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		emittedChanges = append(emittedChanges, change)
		return nil
	}

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.NoError(t, err)

	require.Len(t, emittedChanges, 2)
	assert.Equal(t, "delete", emittedChanges[0].Kind)
	assert.Equal(t, "tenant_A", emittedChanges[0].Data["tenant_id"])
	assert.Equal(t, int32(1), emittedChanges[0].Data["user_id"])
	assert.Equal(t, "insert", emittedChanges[1].Kind)
	assert.Equal(t, "tenant_B", emittedChanges[1].Data["tenant_id"])
	assert.Equal(t, int32(1), emittedChanges[1].Data["user_id"])
}

func TestMSSQLFetchTableChangesInLSNRange_MultipleIndependentPairs(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000030000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes1 := testLSN(1)
	seqvalBytes1 := testLSN(1)

	lsnBytes2 := testLSN(2)
	seqvalBytes2 := testLSN(1)

	updateMask := []byte{0xFF}

	// Pair A: Op 3 + Op 4 with UNCHANGED PK (id=10)
	// Pair B: Op 3 + Op 4 with CHANGED PK (id=20 -> id=21)
	cdcRows := newMSSQLCDCRows(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("name").OfType("VARCHAR", ""),
	).
		AddRow(lsnBytes1, seqvalBytes1, int32(3), updateMask, int32(10), "old_A").
		AddRow(lsnBytes1, seqvalBytes1, int32(4), updateMask, int32(10), "new_A").
		AddRow(lsnBytes2, seqvalBytes2, int32(3), updateMask, int32(20), "old_B").
		AddRow(lsnBytes2, seqvalBytes2, int32(4), updateMask, int32(21), "new_B")

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	var emittedChanges []abstract.CDCChange
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		emittedChanges = append(emittedChanges, change)
		return nil
	}

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.NoError(t, err)

	// Pair A emits 1 "update". Pair B emits 1 "delete" then 1 "insert". Total: 3 events.
	require.Len(t, emittedChanges, 3)

	// Event 0: Pair A update
	assert.Equal(t, "update", emittedChanges[0].Kind)
	assert.Equal(t, int32(10), emittedChanges[0].Data["id"])
	assert.Equal(t, "new_A", emittedChanges[0].Data["name"])
	assert.Equal(t, fmt.Sprintf("%x", lsnBytes1), emittedChanges[0].ExtraColumns[CDCStartLSN])
	assert.Equal(t, fmt.Sprintf("%x", seqvalBytes1), emittedChanges[0].ExtraColumns[CDCSeqVal])

	// Event 1: Pair B delete (old image)
	assert.Equal(t, "delete", emittedChanges[1].Kind)
	assert.Equal(t, int32(20), emittedChanges[1].Data["id"])
	assert.Equal(t, "old_B", emittedChanges[1].Data["name"])
	assert.Equal(t, fmt.Sprintf("%x", lsnBytes2), emittedChanges[1].ExtraColumns[CDCStartLSN])
	assert.Equal(t, fmt.Sprintf("%x", seqvalBytes2), emittedChanges[1].ExtraColumns[CDCSeqVal])

	// Event 2: Pair B insert (new image)
	assert.Equal(t, "insert", emittedChanges[2].Kind)
	assert.Equal(t, int32(21), emittedChanges[2].Data["id"])
	assert.Equal(t, "new_B", emittedChanges[2].Data["name"])
	assert.Equal(t, fmt.Sprintf("%x", lsnBytes2), emittedChanges[2].ExtraColumns[CDCStartLSN])
	assert.Equal(t, fmt.Sprintf("%x", seqvalBytes2), emittedChanges[2].ExtraColumns[CDCSeqVal])
}

func TestMSSQLFetchTableChangesInLSNRange_OrphanAfterImage(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(1))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "orphan update after-image (operation 4)")
}

func TestMSSQLFetchTableChangesInLSNRange_ConsecutiveOperation3(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(2))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected update after-image (operation 4) following before-image")
}

func TestMSSQLFetchTableChangesInLSNRange_Op3InterruptedByOp1(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(1), updateMask, int32(2))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected update after-image (operation 4) following before-image")
}

func TestMSSQLFetchTableChangesInLSNRange_Op3InterruptedByOp2(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(2), updateMask, int32(2))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected update after-image (operation 4) following before-image")
}

func TestMSSQLFetchTableChangesInLSNRange_MismatchedLSN(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes1 := testLSN(1)
	lsnBytes2 := testLSN(2)
	seqvalBytes := testLSN(1)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes1, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes2, seqvalBytes, int32(4), updateMask, int32(1))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update pair metadata mismatch")
}

func TestMSSQLFetchTableChangesInLSNRange_MismatchedSeqval(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes1 := testLSN(1)
	seqvalBytes2 := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes1, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes2, int32(4), updateMask, int32(1))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update pair metadata mismatch")
}

func TestMSSQLFetchTableChangesInLSNRange_IncompletePairAtEOF(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(1)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream ended with incomplete update pair")
}

func TestMSSQLFetchTableChangesInLSNRange_DatabaseReadErrorWithPendingOp3(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	columns := []string{"__$start_lsn", "__$seqval", "__$operation", "__$update_mask", "id"}
	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(1)
	updateMask := []byte{0xFF}

	// Injected database read error on row index 1:
	// Row index 0 (operation 3) is successfully returned, scanned, and buffered into pendingBefore.
	// Row index 1 is required by sqlmock so that Next() reaches row index 1 where RowError(1, injectedErr)
	// triggers, rather than hitting io.EOF at row index 0.
	// This proves that rows.Err() is checked first before EOF evaluation and that the error is not
	// misclassified as an incomplete update pair.
	injectedErr := errors.New("connection reset by peer")
	cdcRows := sqlmock.NewRows(columns).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(1)).
		RowError(1, injectedErr)

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	callbackCount := 0
	var emittedEvents []abstract.CDCChange
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		callbackCount++
		emittedEvents = append(emittedEvents, change)
		return nil
	}

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.Error(t, err)

	// Must return the actual database error from rows.Err(), NOT incomplete update pair!
	assert.ErrorIs(t, err, injectedErr)
	assert.NotContains(t, err.Error(), "incomplete update pair")

	// Verify no callbacks were emitted while operation 3 was pending
	assert.Equal(t, 0, callbackCount, "no CDC callbacks must be emitted when extraction fails on pending before-image")
	assert.Empty(t, emittedEvents)
}

func TestMSSQLFetchTableChangesInLSNRange_DeleteCallbackFailure(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(1)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(2))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	deleteCalled := false
	insertCalled := false
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		if change.Kind == "delete" {
			deleteCalled = true
			return errors.New("delete sink write failed")
		}
		if change.Kind == "insert" {
			insertCalled = true
		}
		return nil
	}

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete sink write failed")
	assert.True(t, deleteCalled)
	assert.False(t, insertCalled, "insert callback must never be invoked if delete callback fails")
}

func TestMSSQLFetchTableChangesInLSNRange_InsertCallbackFailure(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(1)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(2)).
		AddRow(lsnBytes, seqvalBytes, int32(2), updateMask, int32(3))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)

	deleteCalled := false
	insertCalled := false
	laterRowCalled := false
	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		if change.Kind == "delete" {
			deleteCalled = true
			return nil
		}
		if change.Kind == "insert" && change.Data["id"] == int32(2) {
			insertCalled = true
			return errors.New("insert sink write failed")
		}
		if change.Data["id"] == int32(3) {
			laterRowCalled = true
		}
		return nil
	}

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, processFn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert sink write failed")
	assert.True(t, deleteCalled)
	assert.True(t, insertCalled)
	assert.False(t, laterRowCalled, "processing must halt immediately upon callback failure")
}

func TestMSSQLFetchTableChangesInLSNRange_EmptyPKList_Fails(t *testing.T) {
	m, mock := newMockMSSQL(t)

	fromLSNHex := "00000000000000010000"
	advancedLSNHex := "00000000000000010001"
	toLSNHex := "00000000000000020000"

	fromLSNBytes, _ := hex.DecodeString(fromLSNHex)
	advancedLSNBytes, _ := hex.DecodeString(advancedLSNHex)
	toLSNBytes, _ := hex.DecodeString(toLSNHex)

	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(fromLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	capture := captureInstance{instanceName: "dbo_test_table"}
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)

	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(1)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
		AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(1)).
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(1))

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, toLSNBytes).
		WillReturnRows(cdcRows)

	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet[string](),
	}).Wrap(0)

	err := m.fetchTableChangesInLSNRange(context.Background(), stream, capture, fromLSNHex, toLSNHex, func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key columns list cannot be empty")
}

func TestMSSQLStreamChanges_FailureDoesNotCacheLSN(t *testing.T) {
	m, mock := newMockMSSQL(t)
	stream := (&types.Stream{
		Name:                    "test_table",
		Namespace:               "dbo",
		SourceDefinedPrimaryKey: types.NewSet("id"),
	}).Wrap(0)
	m.streams = []types.StreamInterface{stream}
	m.state = newTestMSSQLState(t)
	initialLSNHex := "00000000000000010000"
	targetLSNHex := "00000000000000020000"
	m.state.SetCursor(stream.Self(), cdcCursorKey, initialLSNHex)

	targetLSNBytes, err := hex.DecodeString(targetLSNHex)
	require.NoError(t, err)

	// 1. Mock max LSN query
	mock.ExpectQuery(jdbc.MSSQLCDCMaxLSNQuery()).
		WillReturnRows(sqlmock.NewRows([]string{"max_lsn"}).AddRow(targetLSNBytes))

	// 2. Mock capture instances discovery query
	captureLSNBytes, err := hex.DecodeString(initialLSNHex)
	require.NoError(t, err)
	mock.ExpectQuery(jdbc.MSSQLCDCDiscoverQuery([]string{stream.ID()})).
		WillReturnRows(sqlmock.NewRows([]string{"schema", "table", "instance_name", "start_lsn"}).
			AddRow("dbo", "test_table", "dbo_test_table", captureLSNBytes))

	// 3. Mock advance LSN query
	initialLSNBytes, err := hex.DecodeString(initialLSNHex)
	require.NoError(t, err)
	advancedLSNHex := "00000000000000010001"
	advancedLSNBytes, err := hex.DecodeString(advancedLSNHex)
	require.NoError(t, err)
	mock.ExpectQuery(jdbc.MSSQLCDCAdvanceLSNQuery()).
		WithArgs(initialLSNBytes).
		WillReturnRows(sqlmock.NewRows([]string{"next_lsn"}).AddRow(advancedLSNBytes))

	// 4. Mock CDC changes query with an orphan operation 4 (causes extraction error)
	query := jdbc.MSSQLCDCGetChangesQuery("dbo_test_table")
	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	cdcRows := newMSSQLCDCRows(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("name").OfType("VARCHAR", ""),
	).
		AddRow(lsnBytes, seqvalBytes, int32(4), updateMask, int32(1), "orphan_after")

	mock.ExpectQuery(query).
		WithArgs(advancedLSNBytes, targetLSNBytes).
		WillReturnRows(cdcRows)

	processFn := func(ctx context.Context, change abstract.CDCChange) error {
		return nil
	}

	_, err = m.StreamChanges(context.Background(), 0, nil, processFn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "orphan update after-image")

	// Assert that lsnMap does NOT contain the target LSN
	_, exists := m.lsnMap.Load(stream.ID())
	assert.False(t, exists, "failed extraction must not store candidate target LSN in lsnMap")
}

func TestMSSQLPostCDC_CanceledContext(t *testing.T) {
	m, _ := newMockMSSQL(t)
	stream := (&types.Stream{Name: "test_table", Namespace: "dbo"}).Wrap(0)
	m.streams = []types.StreamInterface{stream}
	m.state = newTestMSSQLState(t)
	m.state.SetCursor(stream.Self(), cdcCursorKey, "initial_lsn")

	// Store a candidate LSN in lsnMap
	m.lsnMap.Store(stream.ID(), "new_lsn")

	// Create canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := m.PostCDC(ctx, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))

	// Verify state cursor was NOT updated
	cursor := m.state.GetCursor(stream.Self(), cdcCursorKey)
	assert.Equal(t, "initial_lsn", cursor)
}

func TestMSSQLPostCDC_Success(t *testing.T) {
	m, _ := newMockMSSQL(t)
	stream := (&types.Stream{Name: "test_table", Namespace: "dbo"}).Wrap(0)
	m.streams = []types.StreamInterface{stream}
	m.state = newTestMSSQLState(t)
	m.state.SetCursor(stream.Self(), cdcCursorKey, "initial_lsn")

	// Store a candidate LSN in lsnMap
	m.lsnMap.Store(stream.ID(), "new_lsn")

	err := m.PostCDC(context.Background(), 0)
	require.NoError(t, err)

	// Verify state cursor WAS updated
	cursor := m.state.GetCursor(stream.Self(), cdcCursorKey)
	assert.Equal(t, "new_lsn", cursor)
}

func TestMapScan_MSSQLCDCConversionPath(t *testing.T) {
	lsnBytes := testLSN(1)
	seqvalBytes := testLSN(2)
	updateMask := []byte{0xFF}

	t.Run("MapScan with explicit column definitions simulating production types", func(t *testing.T) {
		m, mock := newMockMSSQL(t)

		// Define columns with explicit database types simulating production SQL Server CDC:
		// __$start_lsn and __$seqval are BINARY(10) -> maps to types.String (Go string via reformatString)
		// __$operation is INT -> maps to types.Int32 (Go int32 via ReformatInt32)
		// id is INT -> maps to types.Int32 (Go int32 via ReformatInt32)
		mockRows := newMSSQLCDCRows(sqlmock.NewColumn("id").OfType("INT", int64(0))).
			AddRow(lsnBytes, seqvalBytes, int64(3), updateMask, int64(100))

		mock.ExpectQuery("SELECT CDC").WillReturnRows(mockRows)

		rows, err := m.client.QueryContext(context.Background(), "SELECT CDC")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())

		record := make(map[string]interface{})
		rowBytes, err := jdbc.MapScan(rows, record, m.dataTypeConverter, mssqlCDCColumnSizer, mssqlColumnTypeName)
		require.NoError(t, err)

		// 1. Verify resulting Go types directly after MapScan with production column types:
		// LSN and seqval convert from binary(10) -> types.String -> Go string
		assert.IsType(t, "", record["__$start_lsn"])
		assert.Equal(t, string(lsnBytes), record["__$start_lsn"])

		assert.IsType(t, "", record["__$seqval"])
		assert.Equal(t, string(seqvalBytes), record["__$seqval"])

		// Operation converts from int -> types.Int32 -> Go int32
		assert.IsType(t, int32(0), record["__$operation"])
		assert.Equal(t, int32(3), record["__$operation"])

		// Data column sizes and converts
		assert.IsType(t, int32(0), record["id"])
		assert.Equal(t, int32(100), record["id"])
		assert.GreaterOrEqual(t, rowBytes, int64(0))

		// 2. Verify extraction helpers process the production-converted record values:
		startHex, err := extractMetadataHex(record, "__$start_lsn")
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%x", lsnBytes), startHex)

		seqHex, err := extractMetadataHex(record, "__$seqval")
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%x", seqvalBytes), seqHex)

		opCode, err := extractOperationCode(record)
		require.NoError(t, err)
		assert.Equal(t, int32(3), opCode)
	})

	t.Run("MapScan with default NewRows without column definitions", func(t *testing.T) {
		m, mock := newMockMSSQL(t)

		columns := []string{"__$start_lsn", "__$seqval", "__$operation", "__$update_mask", "id"}
		mockRows := sqlmock.NewRows(columns).
			AddRow(lsnBytes, seqvalBytes, int32(3), updateMask, int32(100))

		mock.ExpectQuery("SELECT CDC").WillReturnRows(mockRows)

		rows, err := m.client.QueryContext(context.Background(), "SELECT CDC")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())

		record := make(map[string]interface{})
		rowBytes, err := jdbc.MapScan(rows, record, m.dataTypeConverter, mssqlCDCColumnSizer, mssqlColumnTypeName)
		require.NoError(t, err)

		// Plain NewRows returns empty DatabaseTypeName(), so driver.DefaultParameterConverter
		// converts integers to int64, and the untyped converter preserves raw driver values:
		assert.IsType(t, []byte{}, record["__$start_lsn"])
		assert.IsType(t, []byte{}, record["__$seqval"])
		assert.IsType(t, int64(0), record["__$operation"])
		assert.Equal(t, int64(3), record["__$operation"])
		assert.IsType(t, int64(0), record["id"])
		assert.Equal(t, int64(100), record["id"])

		// Extraction helpers also gracefully handle raw driver values:
		startHex, err := extractMetadataHex(record, "__$start_lsn")
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%x", lsnBytes), startHex)

		seqHex, err := extractMetadataHex(record, "__$seqval")
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%x", seqvalBytes), seqHex)

		opCode, err := extractOperationCode(record)
		require.NoError(t, err)
		assert.Equal(t, int32(3), opCode)
		assert.GreaterOrEqual(t, rowBytes, int64(0))
	})

	t.Run("direct dataTypeConverter execution with production type names", func(t *testing.T) {
		m, _ := newMockMSSQL(t)

		// Direct binary(10) conversion
		convLSN, err := m.dataTypeConverter(lsnBytes, "binary(10)")
		require.NoError(t, err)
		assert.IsType(t, "", convLSN)
		assert.Equal(t, string(lsnBytes), convLSN)

		startHexFromStr, err := extractMetadataHex(map[string]any{"__$start_lsn": convLSN}, "__$start_lsn")
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%x", lsnBytes), startHexFromStr)

		// Direct int conversion from int64
		convOp, err := m.dataTypeConverter(int64(4), "int")
		require.NoError(t, err)
		assert.IsType(t, int32(0), convOp)
		assert.Equal(t, int32(4), convOp)

		opCodeFromConv, err := extractOperationCode(map[string]any{"__$operation": convOp})
		require.NoError(t, err)
		assert.Equal(t, int32(4), opCodeFromConv)
	})
}
