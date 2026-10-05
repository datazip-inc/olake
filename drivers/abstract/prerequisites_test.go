package abstract

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reportingDriver is a stubDriver that reports prerequisite results from its Setup.
type reportingDriver struct {
	stubDriver
	prerequisites types.Prerequisites
}

func (r reportingDriver) Prerequisites() types.Prerequisites { return r.prerequisites }

func staticCheck(name string, required bool, current string, ok bool, err error) Prerequisite {
	return Prerequisite{
		Name:        name,
		Required:    required,
		Recommended: "recommended",
		Check: func(context.Context) (string, bool, error) {
			return current, ok, err
		},
	}
}

func TestRunPrerequisites(t *testing.T) {
	results := RunPrerequisites(context.Background(), []Prerequisite{
		staticCheck("passed_required", true, "ON", true, nil),
		staticCheck("failed_optional", false, "1 day", false, nil),
		staticCheck("failed_required", true, "OFF", false, nil),
		// a read error does not stop the remaining checks and reads as unavailable
		staticCheck("unreadable_required", true, "ignored", true, errors.New("permission denied")),
		staticCheck("passed_optional", false, "7 days", true, nil),
	})

	names := make([]string, len(results))
	for i, r := range results {
		names[i] = r.Name
	}
	// required failed → optional failed → passed, stable within each group
	assert.Equal(t, []string{
		"failed_required", "unreadable_required", "failed_optional", "passed_required", "passed_optional",
	}, names)

	unreadable := results[1]
	assert.False(t, unreadable.Passed)
	assert.Equal(t, "unavailable", unreadable.CurrentValue)
	assert.Equal(t, "recommended", unreadable.RecommendedValue)

	assert.Equal(t, []string{"failed_required", "unreadable_required"}, results.FailedRequired())
}

func TestRequireCDCPrerequisites(t *testing.T) {
	// a driver calls this from Setup when the config selects CDC, so test connection fails
	err := RequireCDCPrerequisites("mysql", types.Prerequisites{
		{Name: "binlog_format", Required: true, Passed: false},
		{Name: "binlog_retention", Required: false, Passed: false},
		{Name: "log_bin", Required: true, Passed: true},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "binlog_format")
	assert.NotContains(t, err.Error(), "binlog_retention")

	got := errs.From(errs.Classify(err))
	assert.Equal(t, errs.CDCPreconditionFailed, got.Category)
	assert.Equal(t, "mysql.cdc_prerequisites_failed", got.Code)

	// only optional checks failed: CDC may still run
	assert.NoError(t, RequireCDCPrerequisites("mysql", types.Prerequisites{
		{Name: "binlog_retention", Required: false, Passed: false},
	}))
	assert.NoError(t, RequireCDCPrerequisites("mongodb", nil))
}

func TestPrerequisitesWithoutReporter(t *testing.T) {
	driver := NewAbstractDriver(context.Background(), stubDriver{typ: "oracle"})
	assert.Nil(t, driver.Prerequisites())
}

func TestValidateCDCPrerequisites(t *testing.T) {
	failedRequired := types.Prerequisites{{Name: "binlog_format", Required: true, Passed: false}}
	failedOptional := types.Prerequisites{{Name: "binlog_retention", Required: false, Passed: false}}

	testCases := []struct {
		name          string
		prerequisites types.Prerequisites
		cdcStreams    int
		expectedCode  string
	}{
		// backfill and incremental syncs are never blocked by CDC checks
		{name: "no cdc streams", prerequisites: failedRequired, cdcStreams: 0},
		{
			name:          "failed required check blocks cdc",
			prerequisites: failedRequired,
			cdcStreams:    1,
			expectedCode:  "mysql.cdc_prerequisites_failed",
		},
		{name: "failed optional check does not block", prerequisites: failedOptional, cdcStreams: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(context.Background(), reportingDriver{
				stubDriver:    stubDriver{typ: "mysql"},
				prerequisites: tc.prerequisites,
			})
			err := driver.ValidateCDCPrerequisites(make([]types.StreamInterface, tc.cdcStreams))
			if tc.expectedCode == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)

			got := errs.From(errs.Classify(err))
			assert.Equal(t, errs.CDCPreconditionFailed, got.Category)
			assert.Equal(t, tc.expectedCode, got.Code)
		})
	}
}

// cdcStubDriver discovers one stream that supports both CDC and incremental.
type cdcStubDriver struct {
	reportingDriver
}

// RetryOnBackoff runs nothing when attempts is 0, which is stubDriver's default
func (c cdcStubDriver) MaxRetries() int { return 1 }

func (c cdcStubDriver) GetStreamNames(context.Context) ([]types.StreamID, error) {
	return []types.StreamID{{Namespace: "public", Name: "orders"}}, nil
}

func (c cdcStubDriver) ProduceSchema(context.Context, types.StreamID) (*types.Stream, error) {
	stream := types.NewStream("orders", "public", nil)
	stream.WithCursorField("updated_at")
	stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL, types.CDC)
	return stream, nil
}

func TestDiscoverDefaultSyncModeRespectsPrerequisites(t *testing.T) {
	testCases := []struct {
		name          string
		prerequisites types.Prerequisites
		expected      types.SyncMode
	}{
		{
			name:          "cdc is the default when the checks pass",
			prerequisites: types.Prerequisites{{Name: "binlog_format", Required: true, Passed: true}},
			expected:      types.CDC,
		},
		// the server cannot do CDC, so defaulting to it would hand the user a mode that fails
		{
			name:          "a failed required check falls back to incremental",
			prerequisites: types.Prerequisites{{Name: "binlog_format", Required: true, Passed: false}},
			expected:      types.INCREMENTAL,
		},
		{
			name:          "a failed optional check still defaults to cdc",
			prerequisites: types.Prerequisites{{Name: "binlog_retention", Required: false, Passed: false}},
			expected:      types.CDC,
		},
		// drivers that report no prerequisites keep the intent-only behaviour
		{name: "no prerequisites reported", expected: types.CDC},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(context.Background(), cdcStubDriver{reportingDriver{
				stubDriver:    stubDriver{typ: "mysql", cdcSupported: true},
				prerequisites: tc.prerequisites,
			}})

			streams, err := driver.Discover(context.Background(), 1, false)
			require.NoError(t, err)
			require.Len(t, streams, 1)
			assert.Equal(t, tc.expected, streams[0].SyncMode)
		})
	}
}
