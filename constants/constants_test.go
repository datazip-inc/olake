package constants

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExitCodes pins the exit code contract with process supervisors.
func TestExitCodes(t *testing.T) {
	assert.Equal(t, 1, ExitCodeFailure)
	assert.Equal(t, 3, ExitCodeManualIntervention)
	// the Go runtime exits 2 on an unrecovered panic or a fatal runtime error
	assert.NotEqual(t, 2, ExitCodeManualIntervention)
	assert.NotEqual(t, ExitCodeFailure, ExitCodeManualIntervention)
}
