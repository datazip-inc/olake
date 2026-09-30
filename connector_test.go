package olake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exitTestModeEnv tells the child process which error the stub driver's Setup returns.
const exitTestModeEnv = "OLAKE_EXIT_CODE_TEST_MODE"

// exitTestErrors is the error each child mode fails with.
var exitTestErrors = map[string]error{
	"plain":                      errors.New("connection reset by peer"),
	"retryable":                  fmt.Errorf("%w: kafka sync aborted due to partition loss", constants.ErrRetryable),
	"non_retryable":              fmt.Errorf("%w: publication %q does not exist", constants.ErrNonRetryable, "olake_pub"),
	"non_retryable_wrapped":      fmt.Errorf("failed in pre cdc run: %w", fmt.Errorf("publication validation failed: %w", fmt.Errorf("%w: publication missing", constants.ErrNonRetryable))),
	"non_retryable_precondition": errs.Precondition(errs.StateInvalid, "kafka.offset_mismatch", fmt.Errorf("%w: run clear destination and restart", constants.ErrNonRetryable)),
	"non_retryable_lost_by_s":    fmt.Errorf("outer: %s", fmt.Errorf("%w: publication missing", constants.ErrNonRetryable)),
	"canceled":                   fmt.Errorf("failed to process cdc streams: %w", context.Canceled),
}

// TestRegisterDriverExitCodes runs RegisterDriver in a child process, because it ends with
// os.Exit, and checks the exit code a process supervisor would see for each kind of error.
func TestRegisterDriverExitCodes(t *testing.T) {
	if mode := os.Getenv(exitTestModeEnv); mode != "" {
		runRegisterDriverChild(t, mode)
		return
	}

	testCases := []struct {
		mode     string
		expected int
	}{
		{mode: "plain", expected: constants.ExitCodeFailure},
		{mode: "retryable", expected: constants.ExitCodeFailure},
		{mode: "canceled", expected: constants.ExitCodeFailure},
		{mode: "non_retryable", expected: constants.ExitCodeManualIntervention},
		{mode: "non_retryable_wrapped", expected: constants.ExitCodeManualIntervention},
		{mode: "non_retryable_precondition", expected: constants.ExitCodeManualIntervention},
		// a %s wrap drops the marker: the error falls back to exit 1
		{mode: "non_retryable_lost_by_s", expected: constants.ExitCodeFailure},
	}

	for _, tc := range testCases {
		t.Run(tc.mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRegisterDriverExitCodes$")
			cmd.Env = append(os.Environ(), exitTestModeEnv+"="+tc.mode, "TELEMETRY_DISABLED=true")
			cmd.Dir = t.TempDir() // the child's log files land here, not in the repo
			out, err := cmd.CombinedOutput()

			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "child must exit non-zero; output:\n%s", out)
			assert.Equal(t, tc.expected, exitErr.ExitCode(), "output:\n%s", out)
		})
	}
}

// runRegisterDriverChild runs `olake discover` with a driver whose Setup fails with the mode's error.
// RegisterDriver exits the process, so reaching the end of this function is a failure.
func runRegisterDriverChild(t *testing.T, mode string) {
	setupErr, ok := exitTestErrors[mode]
	require.True(t, ok, "unknown mode %q", mode)

	configPath := filepath.Join(t.TempDir(), "source.json")
	require.NoError(t, os.WriteFile(configPath, []byte("{}"), 0o600))

	os.Args = []string{"olake", "discover", "--config", configPath}
	RegisterDriver(exitTestDriver{setupErr: setupErr})
	t.Fatal("RegisterDriver returned instead of exiting")
}

type exitTestConfig struct{}

func (exitTestConfig) Validate() error { return nil }

// exitTestDriver fails Setup with setupErr and returns zero values everywhere else.
type exitTestDriver struct {
	setupErr error
}

func (d exitTestDriver) GetConfigRef() abstract.Config { return &exitTestConfig{} }
func (d exitTestDriver) Spec() any                     { return nil }
func (d exitTestDriver) Type() string                  { return "postgres" }
func (d exitTestDriver) Setup(context.Context) error   { return d.setupErr }
func (d exitTestDriver) SetupState(*types.State)       {}
func (d exitTestDriver) MaxConnections() int           { return 0 }
func (d exitTestDriver) MaxRetries() int               { return 0 }
func (d exitTestDriver) GetStreamNames(context.Context) ([]types.StreamID, error) {
	return nil, nil
}
func (d exitTestDriver) ProduceSchema(context.Context, types.StreamID) (*types.Stream, error) {
	return nil, nil //nolint:nilnil // never reached: Setup fails first
}
func (d exitTestDriver) GetOrSplitChunks(context.Context, *destination.WriterPool, types.StreamInterface) (*types.Set[types.Chunk], error) {
	return nil, nil //nolint:nilnil // never reached: Setup fails first
}
func (d exitTestDriver) ChunkIterator(context.Context, types.StreamInterface, types.Chunk, abstract.BackfillMsgFn) error {
	return nil
}
func (d exitTestDriver) FetchMaxCursorValues(context.Context, types.StreamInterface) (any, any, error) {
	return nil, nil, nil
}
func (d exitTestDriver) StreamIncrementalChanges(context.Context, types.StreamInterface, abstract.BackfillMsgFn) error {
	return nil
}
func (d exitTestDriver) CDCSupported() bool                     { return false }
func (d exitTestDriver) ChangeStreamConfig() (bool, bool, bool) { return false, false, false }
func (d exitTestDriver) PreCDC(context.Context, []types.StreamInterface) error {
	return nil
}
func (d exitTestDriver) StreamChanges(context.Context, int, map[string]any, abstract.CDCMsgFn) (any, error) {
	return nil, nil //nolint:nilnil // never reached: Setup fails first
}
func (d exitTestDriver) PostCDC(context.Context, int) error { return nil }
