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

func TestPrerequisitesWithoutReporter(t *testing.T) {
	driver := NewAbstractDriver(context.Background(), stubDriver{typ: "oracle"})
	assert.Nil(t, driver.Prerequisites())
}

func TestReadEnforcesRequiredPrerequisites(t *testing.T) {
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
		// an optional failure falls through to the next cdc gate (the stub has no cdc config)
		{
			name:          "failed optional check does not block",
			prerequisites: failedOptional,
			cdcStreams:    1,
			expectedCode:  "mysql.cdc_not_configured",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(context.Background(), reportingDriver{
				stubDriver:    stubDriver{typ: "mysql"},
				prerequisites: tc.prerequisites,
			})
			err := driver.Read(context.Background(), nil, nil, make([]types.StreamInterface, tc.cdcStreams), nil)
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
