package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

type Upsert struct {
	Name      string
	Operation string
	UseState  bool
	OpSymbol  string
	Expected  map[string]interface{}
}

// RunUpsertIceberg seeds a Kafka upsert stream's topic with seedOp and runs steps as Arrow Iceberg sync+verify rounds.
func (th *TestHandler) RunUpsertIceberg(t *testing.T, seedOp string, steps []Upsert) {
	ctx := t.Context()
	testTable := th.GetTableName()

	if err := th.IsolateDestinationDB(); err != nil {
		t.Fatalf("failed to isolate the destination database in streams.json: %s", err)
	}
	if err := setStreamUpsert(th.TestConfig, th.Namespace, testTable, th.DedupKeys); err != nil {
		t.Fatalf("failed to set stream upsert: %s", err)
	}
	if err := testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, th.PartitionRegex, th.FilterConfig, []string{testTable}, []string{th.ColumnToExclude}); err != nil {
		t.Fatalf("failed to update streams.json: %s", err)
	}

	if err := th.IcebergWriter(ctx, t, testTable, true, func(ctx context.Context, t *testing.T, testTable string) error {
		testutils.DropIcebergTable(t, testTable, th.DestinationDB)
		th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
		th.ExecuteQuery(ctx, t, th.TestConfig, "create")
		th.ExecuteQuery(ctx, t, th.TestConfig, seedOp)
		if err := os.RemoveAll(th.tableIndexHostDir()); err != nil {
			return fmt.Errorf("failed to clear table index: %w", err)
		}

		for _, tc := range steps {
			if err := th.runSyncAndVerify(ctx, t, testTable, tc.UseState, "iceberg", tc.Operation, tc.OpSymbol, tc.Expected, true); err != nil {
				return fmt.Errorf("%s: %w", tc.Name, err)
			}
		}
		testutils.DropIcebergTable(t, testTable, th.DestinationDB)
		return nil
	}); err != nil {
		t.Fatalf("upsert iceberg: %v", err)
	}
}

// RunUpsertIcebergExpectFail seeds the topic with seedOp and asserts the first Arrow Iceberg sync fails with wantErr.
func (th *TestHandler) RunUpsertIcebergExpectFail(t *testing.T, seedOp, wantErr string) {
	ctx := t.Context()
	testTable := th.GetTableName()

	if err := th.IsolateDestinationDB(); err != nil {
		t.Fatalf("failed to isolate the destination database in streams.json: %s", err)
	}
	if err := setStreamUpsert(th.TestConfig, th.Namespace, testTable, th.DedupKeys); err != nil {
		t.Fatalf("failed to set stream upsert: %s", err)
	}
	if err := testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, th.PartitionRegex, th.FilterConfig, []string{testTable}, []string{th.ColumnToExclude}); err != nil {
		t.Fatalf("failed to update streams.json: %s", err)
	}

	if err := th.IcebergWriter(ctx, t, testTable, true, func(ctx context.Context, t *testing.T, testTable string) error {
		testutils.DropIcebergTable(t, testTable, th.DestinationDB)
		th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
		th.ExecuteQuery(ctx, t, th.TestConfig, "create")
		th.ExecuteQuery(ctx, t, th.TestConfig, seedOp)
		if err := os.RemoveAll(th.tableIndexHostDir()); err != nil {
			return fmt.Errorf("failed to clear table index: %w", err)
		}
		return th.runSyncAndVerify(ctx, t, testTable, false, "iceberg", "", "c", th.ExpectedData, true)
	}); err == nil {
		t.Fatalf("sync must fail (%s)", wantErr)
	} else if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("expected %q, got: %v", wantErr, err)
	}
}

func setStreamUpsert(cfg *testutils.TestConfig, namespace, table string, dedupKeys []string) error {
	if len(dedupKeys) == 0 {
		return fmt.Errorf("upsert mode requires at least one dedup key")
	}
	keys := make([]interface{}, 0, len(dedupKeys))
	for _, k := range dedupKeys {
		keys = append(keys, k)
	}
	return testutils.EditJSONFile(cfg.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
		selected, _ := doc["selected_streams"].(map[string]interface{})
		namespaceStreams, ok := selected[namespace].([]interface{})
		if !ok {
			return fmt.Errorf("namespace %q missing in selected streams", namespace)
		}
		matched := false
		tableName := testutils.NormalizeStreamName(cfg.Driver, table)
		for _, raw := range namespaceStreams {
			s, ok := raw.(map[string]interface{})
			if !ok || fmt.Sprint(s["stream_name"]) != tableName {
				continue
			}
			s["append_mode"] = false
			s["dedup_keys"] = keys
			matched = true
		}
		if !matched {
			return fmt.Errorf("stream %q not found in namespace %q", table, namespace)
		}
		return nil
	})
}
