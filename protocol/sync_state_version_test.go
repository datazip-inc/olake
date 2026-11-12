package protocol

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stateVersionConfig is the smallest connector config the sync pre-run can unmarshal.
type stateVersionConfig struct{}

func (stateVersionConfig) Validate() error { return nil }

// stateVersionDriver serves a config reference; no other DriverInterface method is reached.
type stateVersionDriver struct {
	abstract.DriverInterface
}

func (stateVersionDriver) GetConfigRef() abstract.Config { return &stateVersionConfig{} }

// TestSyncPreRunStateVersion pins the state version a run starts with. A state file that carries
// a version keeps it; a state file with neither a version nor progress is a fresh sync and must
// adopt the latest version instead of silently running with the legacy version 0 behavior.
func TestSyncPreRunStateVersion(t *testing.T) {
	dir := t.TempDir()
	writeFile := func(t *testing.T, name, body string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}
	availableStreams := writeFile(t, "available_streams.json",
		`{"streams":[{"stream":{"name":"t","namespace":"public","sync_mode":"full_refresh"}}]}`)
	selectedStreams := writeFile(t, "selected_streams.json", `{}`)
	config := writeFile(t, "config.json", `{}`)
	destination := writeFile(t, "destination.json", `{}`)

	// The pre-run writes package globals; put them back so other tests are unaffected.
	prevConnector, prevConfig, prevDestination := connector, configPath, destinationConfigPath
	prevStreams, prevAvailable, prevSelected, prevStatePath := streamsPath, availableStreamsPath, selectedStreamsPath, statePath
	prevState, prevCatalog, prevVersion := state, catalog, constants.LoadedStateVersion
	t.Cleanup(func() {
		connector, configPath, destinationConfigPath = prevConnector, prevConfig, prevDestination
		streamsPath, availableStreamsPath, selectedStreamsPath, statePath = prevStreams, prevAvailable, prevSelected, prevStatePath
		state, catalog, constants.LoadedStateVersion = prevState, prevCatalog, prevVersion
	})

	connector = abstract.NewAbstractDriver(context.Background(), stateVersionDriver{})
	configPath, destinationConfigPath = config, destination
	streamsPath = ""
	availableStreamsPath, selectedStreamsPath = availableStreams, selectedStreams

	for _, tc := range []struct {
		name    string
		body    string
		version int
	}{
		{name: "empty object", body: `{}`, version: constants.LatestStateVersion},
		{name: "type only", body: `{"type":"STREAM"}`, version: constants.LatestStateVersion},
		{
			name:    "legacy state with progress",
			body:    `{"type":"STREAM","streams":[{"stream":"t","namespace":"public","sync_mode":"full_refresh","state":{"cursor":7}}]}`,
			version: 0,
		},
		{name: "explicit older version", body: `{"type":"STREAM","version":3}`, version: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			discardStrayLogs(t)
			statePath = writeFile(t, "state.json", tc.body)
			// Sentinel so an unset LoadedStateVersion cannot pass as the expected value.
			constants.LoadedStateVersion = -1

			require.NoError(t, syncCmd.PersistentPreRunE(syncCmd, nil))
			assert.Equal(t, tc.version, constants.LoadedStateVersion)
			assert.Equal(t, tc.version, state.Version)
		})
	}
}
