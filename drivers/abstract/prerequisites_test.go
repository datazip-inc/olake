package abstract

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reportingDriver is a stubDriver that reports prerequisite results from its Setup.
type reportingDriver struct {
	stubDriver
	prerequisites types.PrerequisiteResults
}

func (r reportingDriver) Prerequisites() types.PrerequisiteResults { return r.prerequisites }

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
	assert.EqualError(t, unreadable.Err, "permission denied")
	assert.NoError(t, results[0].Err)

	assert.Equal(t, []string{"failed_required", "unreadable_required"}, results.GetFailedRequirements())
}

func TestRequireCDCPrerequisites(t *testing.T) {
	timeout := fmt.Errorf("reading binlog_format: %w", context.DeadlineExceeded)

	tests := []struct {
		name          string
		prerequisites types.PrerequisiteResults
		wantCategory  errs.Category // empty means no error
		wantCode      string
		wantMessage   []string
		notInMessage  []string
	}{
		{
			// a driver calls this from Setup when the config selects CDC, so test connection fails
			name: "failed required check is a precondition failure",
			prerequisites: types.PrerequisiteResults{
				{Name: "binlog_format", Required: true, Passed: false},
				{Name: "binlog_retention", Required: false, Passed: false},
				{Name: "log_bin", Required: true, Passed: true},
			},
			wantCategory: errs.CDCPreconditionFailed,
			wantCode:     "mysql.cdc_prerequisites_failed",
			wantMessage:  []string{"binlog_format"},
			notInMessage: []string{"binlog_retention", "log_bin"},
		},
		{
			// nothing is known to be misconfigured, so the cause decides the category
			name: "unevaluated required check keeps its cause",
			prerequisites: types.PrerequisiteResults{
				{Name: "binlog_format", Required: true, Passed: false, CurrentValue: "unavailable", Err: timeout},
			},
			wantCategory: errs.Timeout,
			wantMessage:  []string{"binlog_format", "could not evaluate", "context deadline exceeded"},
		},
		{
			// a definite misconfiguration is the actionable failure
			name: "failed and unevaluated checks report a precondition failure",
			prerequisites: types.PrerequisiteResults{
				{Name: "log_bin", Required: true, Passed: false},
				{Name: "binlog_format", Required: true, Passed: false, CurrentValue: "unavailable", Err: timeout},
			},
			wantCategory: errs.CDCPreconditionFailed,
			wantCode:     "mysql.cdc_prerequisites_failed",
			wantMessage:  []string{"log_bin", "binlog_format (could not evaluate: "},
		},
		{
			// only optional checks failed: CDC may still run
			name: "failed optional check passes",
			prerequisites: types.PrerequisiteResults{
				{Name: "binlog_retention", Required: false, Passed: false, Err: timeout},
			},
		},
		{name: "no checks recorded"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := RequireCDCPrerequisites("mysql", tc.prerequisites)
			if tc.wantCategory == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, part := range tc.wantMessage {
				assert.Contains(t, err.Error(), part)
			}
			for _, part := range tc.notInMessage {
				assert.NotContains(t, err.Error(), part)
			}

			got := errs.From(errs.Classify(err))
			assert.Equal(t, tc.wantCategory, got.Category)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, got.Code)
			}
		})
	}
}

func TestPrerequisitesWithoutReporter(t *testing.T) {
	driver := NewAbstractDriver(context.Background(), stubDriver{typ: "oracle"})
	assert.Nil(t, driver.Prerequisites())
}

func TestValidateCDCPrerequisites(t *testing.T) {
	failedRequired := types.PrerequisiteResults{{Name: "binlog_format", Required: true, Passed: false}}
	failedOptional := types.PrerequisiteResults{{Name: "binlog_retention", Required: false, Passed: false}}

	testCases := []struct {
		name          string
		prerequisites types.PrerequisiteResults
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
