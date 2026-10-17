package abstract

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
	assert.Equal(t, "permission denied", unreadable.Error)
	assert.NoError(t, results[0].Err)
	assert.Empty(t, results[0].Error)
}

// each check gets its own deadline: a hung check is reported unavailable and the rest still run
func TestRunPrerequisitesBoundsEachCheck(t *testing.T) {
	hung := Prerequisite{
		Name: "hung", Required: true, Recommended: "recommended",
		Check: func(ctx context.Context) (string, bool, error) {
			<-ctx.Done()
			return "", false, ctx.Err()
		},
	}
	// a parent deadline sooner than the per-check timeout wins, which keeps this test fast
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	results := RunPrerequisites(ctx, []Prerequisite{
		hung,
		staticCheck("after_hung", false, "ON", true, nil),
	})

	require.Len(t, results, 2)
	assert.Equal(t, "hung", results[0].Name)
	assert.False(t, results[0].Passed)
	assert.Equal(t, "unavailable", results[0].CurrentValue)
	assert.ErrorIs(t, results[0].Err, context.DeadlineExceeded)
	assert.Equal(t, "after_hung", results[1].Name)
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

func TestAbstractDriverPrerequisites(t *testing.T) {
	recorded := types.PrerequisiteResults{{Name: "binlog_format", Required: true, Passed: true}}

	tests := []struct {
		name   string
		driver DriverInterface
		want   types.PrerequisiteResults
	}{
		{
			name:   "driver reporting prerequisites",
			driver: reportingDriver{stubDriver: stubDriver{typ: "mysql"}, prerequisites: recorded},
			want:   recorded,
		},
		// drivers that run no checks report nothing
		{name: "driver without reporter", driver: stubDriver{typ: "oracle"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(context.Background(), tc.driver)
			assert.Equal(t, tc.want, driver.Prerequisites())
		})
	}
}
