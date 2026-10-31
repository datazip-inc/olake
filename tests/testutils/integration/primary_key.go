package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// testCDCPrimaryKeyUpdate checks both identities, including the retained delete tombstone.
func (th *TestHandler) testCDCPrimaryKeyUpdate(ctx context.Context, t *testing.T, testTable string) error {
	th.resetTable(ctx, t)
	defer th.resetCatalog(t)
	if err := testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, "", "", []string{testTable}, []string{th.ColumnToExclude}); err != nil {
		return fmt.Errorf("failed to clear partition and filter config: %w", err)
	}
	if err := updateStreamConfig(th.TestConfig, th.Namespace, testTable, "cdc", ""); err != nil {
		return err
	}
	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "eq"); err != nil {
		return err
	}

	testutils.DropIcebergTable(t, testTable, th.DestinationDB)
	if !testutils.KeepTestData() {
		defer testutils.DropIcebergTable(t, testTable, th.DestinationDB)
	}
	if err := th.runIcebergSync(ctx, t, "primary key initial sync", false); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.DestinationDB, testTable)
	readRows := func() map[string]string {
		refreshTable(ctx, t, spark, fullTableName)
		df, err := spark.Sql(ctx, fmt.Sprintf("SELECT _olake_id, _op_type FROM %s", fullTableName))
		require.NoError(t, err)
		rows, err := df.Collect(ctx)
		require.NoError(t, err)
		result := make(map[string]string, len(rows))
		for _, row := range rows {
			id := fmt.Sprint(row.Value("_olake_id"))
			_, duplicate := result[id]
			require.Falsef(t, duplicate, "duplicate destination identity %s", id)
			result[id] = fmt.Sprint(row.Value("_op_type"))
		}
		return result
	}
	want := map[string]string{"1": "r", "2": "r", "3": "r", "4": "r", "5": "r"}
	// The seed's normally filtered row is present in this unfiltered scenario too.
	if th.Driver == string(constants.Postgres) {
		want["6"] = "r"
	} else {
		want["998"] = "r"
	}
	require.Equal(t, want, readRows())

	th.ExecuteQuery(ctx, t, th.TestConfig, "update_primary_key")
	if err := th.runIcebergSync(ctx, t, "primary key update sync", true); err != nil {
		return err
	}
	want["1"] = "d"
	want["101"] = "u"
	require.Equal(t, want, readRows())

	// Keep source state and destination metadata so a new process exercises normal resume.
	if err := th.runIcebergSync(ctx, t, "primary key repeat sync", true); err != nil {
		return err
	}
	require.Equal(t, want, readRows())
	return nil
}
