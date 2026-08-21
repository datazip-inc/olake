package integration

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/stretchr/testify/require"
)

type Test struct {
	*testutils.TestConfig
	ExpectedData                     map[string]interface{}
	ExpectedUpdatedData              map[string]interface{}
	DestinationDataTypeSchema        map[string]string
	UpdatedDestinationDataTypeSchema map[string]string
	DefaultCDCColumnsSchema          map[string]string

	// The fields below exist for the backward-compatibility suite (compatibility.go) and are zero for
	// every other suite, which keeps their behavior identical to before they existed.

	// VerifyDisabled turns runSyncAndVerify into run-only: the sync still has to exit 0, but the
	// destination is not checked against ExpectedData / DestinationDataTypeSchema. Both compatibility
	// runs set it -- the baseline binary predates the current expectations, and the candidate runs
	// at the baseline's state version -- so their only assertion is the cross-run comparison.
	VerifyDisabled bool

	// PreserveDestination keeps a scenario from dropping the destination table it writes to -- both
	// before its first sync and when it finishes. The compatibility suite sets it: its candidate run
	// must meet the table the baseline created, and its comparison reads both after the runs end.
	PreserveDestination bool
}

// Validate checks that the Test is valid and complete in order to derive/setup.
func (cfg *Test) Validate(t *testing.T) {
	t.Helper()
	require.NotNil(t, cfg.TestConfig, "integration.Test.TestConfig is not set")
	cfg.TestConfig.Validate(t)
	require.NotEmpty(t, cfg.TestConfig.DestinationDB, "TestConfig.DestinationDB is not set; the suite cannot verify or drop what it wrote")

	// A run that verifies needs something to verify against. The compatibility suite is the one
	// caller that legitimately has neither, and it says so through VerifyDisabled.
	if !cfg.VerifyDisabled {
		require.NotEmpty(t, cfg.ExpectedData, "integration.Test.ExpectedData is empty and VerifyDisabled is not set")
		require.NotEmpty(t, cfg.DestinationDataTypeSchema, "integration.Test.DestinationDataTypeSchema is empty and VerifyDisabled is not set")
	}
	// TODO: assert each Expected*Data covers the same columns as its schema map. Left out until the
	// suites have run green with it: today a mismatch surfaces only as an opaque diff at verify
	// time, but turning it into a hard failure needs the fixtures checked first.
}
