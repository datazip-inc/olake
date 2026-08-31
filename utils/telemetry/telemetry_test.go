package telemetry

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureTransport stands in for the analytics endpoint so an event can be inspected without
// leaving the process. Nothing here reaches the network.
type captureTransport struct {
	mu     sync.Mutex
	bodies [][]byte
	status int
	err    error
}

func (c *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		c.bodies = append(c.bodies, body)
	}
	if c.err != nil {
		return nil, c.err
	}
	status := c.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}, nil
}

// events returns the decoded properties of each captured event, keyed by event name.
func (c *captureTransport) events(t *testing.T) []map[string]any {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]map[string]any, 0, len(c.bodies))
	for _, body := range c.bodies {
		var payload struct {
			Event      string         `json:"event"`
			Properties map[string]any `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(body, &payload))
		payload.Properties["__event"] = payload.Event
		out = append(out, payload.Properties)
	}
	return out
}

// stubClient points the package-level client at the capture, standing in for what Init builds.
// Init itself is never called from a unit test: it makes two network calls.
func stubClient(t *testing.T) *captureTransport {
	t.Helper()
	capture := &captureTransport{}
	previous := telemetry
	telemetry = &Telemetry{
		httpClient: &http.Client{Transport: capture},
		userID:     "test-user",
		service:    defaultService,
		platform:   platformInfo{OS: "testos", Arch: "testarch", OlakeVersion: "v0.0.0", DeviceCPU: "1 cores"},
		ipAddress:  ipNotFoundPlaceholder,
	}
	t.Cleanup(func() { telemetry = previous })
	return capture
}

// initDoneOnce puts the package in the state Init would leave it in, without Init's network
// calls. A channel can only be closed once, so every test shares this.
var initDoneOnce sync.Once

func markInitDone() { initDoneOnce.Do(func() { close(initDone) }) }

// drain waits for handed-off events the way Flush does, minus Flush's Disabled short-circuit.
// Tests must not depend on whether TELEMETRY_DISABLED happens to be set in the environment.
func drain() { inflight.Wait() }

// TestSendEventCommonProperties pins the properties every event carries. These names are read by
// dashboards, so a rename is a breaking change that no compiler catches.
func TestSendEventCommonProperties(t *testing.T) {
	capture := stubClient(t)
	require.NoError(t, telemetry.sendEvent("Test Event", map[string]any{"custom": 1}))

	events := capture.events(t)
	require.Len(t, events, 1)
	props := events[0]

	for _, key := range []string{
		"os", "arch", "olake_version", "num_cpu", "service", "ip_address",
		"location", "distinct_id", "time", "event_original_name",
	} {
		assert.Contains(t, props, key, "common property %q is missing", key)
	}
	assert.Equal(t, "Test Event - CLI", props["__event"])
	assert.Equal(t, "Test Event - CLI", props["event_original_name"])
	assert.Equal(t, float64(1), props["custom"], "caller properties survive the merge")
}

// TestSendEventErrors covers the two failure modes: a client that was never built, and an
// endpoint that answers with a non-2xx status.
func TestSendEventErrors(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		empty := &Telemetry{}
		assert.Error(t, empty.sendEvent("X", nil))
	})

	t.Run("non-2xx status", func(t *testing.T) {
		capture := stubClient(t)
		capture.status = http.StatusInternalServerError
		assert.Error(t, telemetry.sendEvent("X", nil))
	})

	t.Run("transport failure", func(t *testing.T) {
		capture := stubClient(t)
		capture.err = errors.New("no route to host")
		assert.Error(t, telemetry.sendEvent("X", nil))
	})

	t.Run("nil properties are allowed", func(t *testing.T) {
		stubClient(t)
		assert.NoError(t, telemetry.sendEvent("X", nil))
	})
}

// TestEventPropertyNames pins the property names each tracked event emits. Renaming one silently
// zeroes the corresponding dashboard series, with no schema change to explain it.
func TestEventPropertyNames(t *testing.T) {
	markInitDone()

	mix := types.StreamMix{
		FullRefresh: 1, Incremental: 2, CDC: 3, StrictCDC: 4,
		Selected: 5, Normalized: 6, Partitioned: 7,
	}
	destination := &types.WriterConfig{Type: types.Iceberg, WriterConfig: map[string]any{"catalog_type": "glue"}}

	// Distinct values, so a counter wired to the wrong key is caught rather than matching by
	// coincidence. Both sync events carry the same seven.
	streamMixProps := map[string]any{
		"full_refresh_streams_count": mix.FullRefresh,
		"incremental_streams_count":  mix.Incremental,
		"cdc_streams_count":          mix.CDC,
		"strict_cdc_streams_count":   mix.StrictCDC,
		"selected_streams_count":     mix.Selected,
		"normalized_streams_count":   mix.Normalized,
		"partitioned_streams_count":  mix.Partitioned,
	}

	testCases := []struct {
		name           string
		track          func()
		expectedKeys   []string
		expectedValues map[string]any // checked by value, not just presence
	}{
		{
			name:         "discover",
			track:        func() { TrackDiscover(9, "postgres") },
			expectedKeys: []string{"stream_count", "source_type"},
		},
		{
			name:  "sync started",
			track: func() { TrackSyncStarted("sync-1", mix, "postgres", destination, 11) },
			expectedKeys: []string{
				"sync_start", "sync_id", "stream_count", "source_type", "destination_type", "catalog_type",
			},
			expectedValues: streamMixProps,
		},
		{
			name:  "sync completed",
			track: func() { TrackSyncCompleted("sync-1", mix, destination, false, 100, 2048) },
			expectedKeys: []string{
				"sync_id", "sync_end", "sync_status", "records_synced", "bytes_read",
				"destination_type", "catalog_type",
			},
			expectedValues: streamMixProps,
		},
		{
			name: "failure",
			track: func() {
				TrackFailure("sync", "postgres", "sync-1", errs.Failure{
					Category: errs.AuthFailed, ClassifiedBy: errs.ClassifiedByVendor,
					Code: "28P01", ErrorType: "*pgconn.PgError",
				})
			},
			expectedKeys: []string{"command", "error_source", "category", "classified_by", "sync_id", "code", "error_type"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capture := stubClient(t)
			tc.track()
			drain()

			events := capture.events(t)
			require.Len(t, events, 1, "exactly one event should have been sent")
			for _, key := range tc.expectedKeys {
				assert.Contains(t, events[0], key, "property %q is missing", key)
			}
			// EqualValues: the payload has been through JSON, so counters arrive as float64.
			for key, expected := range tc.expectedValues {
				assert.EqualValues(t, expected, events[0][key], "property %q", key)
			}
		})
	}
}

// TestTrackFailureOmitsAbsentFields covers the deliberate choice not to fake a value: an absent
// code is a legitimate slice in a dashboard, not an empty string to be counted.
func TestTrackFailureOmitsAbsentFields(t *testing.T) {
	markInitDone()

	testCases := []struct {
		name           string
		syncID         string
		failure        errs.Failure
		expectedAbsent []string
	}{
		{
			name:           "no sync id outside sync",
			syncID:         "",
			failure:        errs.Failure{Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByPrecondition},
			expectedAbsent: []string{"sync_id", "code", "error_type"},
		},
		{
			name:           "unclassified carries the type but no code",
			syncID:         "",
			failure:        errs.Failure{Category: errs.Unclassified, ClassifiedBy: errs.ClassifiedByDefault, ErrorType: "*errors.errorString"},
			expectedAbsent: []string{"sync_id", "code"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capture := stubClient(t)
			TrackFailure("check", "postgres", tc.syncID, tc.failure)
			drain()

			events := capture.events(t)
			require.Len(t, events, 1)
			for _, key := range tc.expectedAbsent {
				assert.NotContains(t, events[0], key, "property %q should be omitted, not empty", key)
			}
			assert.Equal(t, string(tc.failure.Category), events[0]["category"])
		})
	}
}

// TestDestinationShape covers reading the destination type and catalog type off a writer config,
// including the shapes that would panic on a bare type assertion.
func TestDestinationShape(t *testing.T) {
	testCases := []struct {
		name                string
		config              *types.WriterConfig
		expectedDestination string
		expectedCatalog     string
	}{
		// no destination at all, e.g. a discover run
		{name: "nil config", config: nil},
		// catalog_type is an iceberg concept only
		{
			name:                "iceberg with a catalog",
			config:              &types.WriterConfig{Type: types.Iceberg, WriterConfig: map[string]any{"catalog_type": "glue"}},
			expectedDestination: string(types.Iceberg),
			expectedCatalog:     "glue",
		},
		{
			name:                "iceberg without a catalog key",
			config:              &types.WriterConfig{Type: types.Iceberg, WriterConfig: map[string]any{}},
			expectedDestination: string(types.Iceberg),
		},
		// a writer config of an unexpected shape must not panic
		{
			name:                "iceberg with a non-map writer config",
			config:              &types.WriterConfig{Type: types.Iceberg, WriterConfig: "not-a-map"},
			expectedDestination: string(types.Iceberg),
		},
		{
			name:                "iceberg with a non-string catalog type",
			config:              &types.WriterConfig{Type: types.Iceberg, WriterConfig: map[string]any{"catalog_type": 42}},
			expectedDestination: string(types.Iceberg),
		},
		// every other destination reports no catalog
		{
			name:                "parquet reports no catalog",
			config:              &types.WriterConfig{Type: types.Parquet, WriterConfig: map[string]any{"catalog_type": "ignored"}},
			expectedDestination: string(types.Parquet),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destination, catalog := destinationShape(tc.config)
			assert.Equal(t, tc.expectedDestination, destination)
			assert.Equal(t, tc.expectedCatalog, catalog)
		})
	}
}

// TestSendContainsPanics covers the guarantee send documents: a panic while reporting must not
// kill the command being reported on, and must still release Flush.
func TestSendContainsPanics(t *testing.T) {
	markInitDone()
	stubClient(t)

	assert.NotPanics(t, func() {
		send("boom", func() { panic("event build failed") })
		drain()
	})

	t.Run("a later event still sends", func(t *testing.T) {
		capture := stubClient(t)
		require.NoError(t, telemetry.sendEvent("After Panic", nil))
		assert.Len(t, capture.events(t), 1)
	})
}

// TestFlush covers what Flush is for: the command exits immediately after it returns, so an
// event still in flight would be discarded as the process dies. When telemetry is off there is
// nothing to wait for and it short-circuits before touching the WaitGroup.
func TestFlush(t *testing.T) {
	markInitDone()
	stubClient(t)

	t.Run("returns immediately with nothing in flight", func(t *testing.T) {
		start := time.Now()
		Flush()
		assert.Less(t, time.Since(start), 200*time.Millisecond)
	})

	t.Run("waits for a handed-off event", func(t *testing.T) {
		// Forced on rather than skipped when TELEMETRY_DISABLED is set, so the branch is
		// covered in either environment.
		withDisabled(t, false, func() {
			ran := make(chan struct{})
			send("slow", func() {
				time.Sleep(150 * time.Millisecond)
				close(ran)
			})

			Flush()
			select {
			case <-ran:
			default:
				require.Fail(t, "Flush returned before the event finished")
			}
		})
	})
}

// withDisabled runs body with Disabled() reporting want. The value is cached behind a sync.Once,
// so once that has fired Disabled() simply returns the cached bool — setting it directly drives
// both branches in one run, in either environment, without copying the Once.
func withDisabled(t *testing.T, want bool, body func()) {
	t.Helper()
	_ = Disabled() // ensure the Once has fired, so the cached value is what Disabled() returns

	previous := disabled
	t.Cleanup(func() { disabled = previous })

	disabled = want
	require.Equal(t, want, Disabled())
	body()
}

// TestFlushShortCircuitsWhenDisabled covers the guard at the top of Flush. With telemetry off
// there is nothing in flight to wait for, so it must return without touching the WaitGroup —
// which is what keeps a disabled run from paying the flush budget.
func TestFlushShortCircuitsWhenDisabled(t *testing.T) {
	markInitDone()
	stubClient(t)

	withDisabled(t, true, func() {
		release := make(chan struct{})
		send("blocked", func() { <-release })

		start := time.Now()
		Flush()
		assert.Less(t, time.Since(start), 100*time.Millisecond,
			"Flush must not wait when telemetry is disabled")

		close(release)
		drain()
	})
}

// TestSendSkipsWhenClientIsMissing covers the guard in send: when Init found telemetry disabled
// the client is never built, and an event must be dropped rather than dereference a nil.
func TestSendSkipsWhenClientIsMissing(t *testing.T) {
	markInitDone()

	previous := telemetry
	telemetry = nil
	t.Cleanup(func() { telemetry = previous })

	ran := false
	assert.NotPanics(t, func() {
		send("orphan", func() { ran = true })
		drain()
	})
	assert.False(t, ran, "the event body must not run without a client")
}

// TestTrackEventsSurviveASendFailure covers the error branch each Track function has: a refused
// endpoint is logged and swallowed, never returned to the command being reported on.
func TestTrackEventsSurviveASendFailure(t *testing.T) {
	markInitDone()

	mix := types.StreamMix{FullRefresh: 1, Selected: 1}
	destination := &types.WriterConfig{Type: types.Parquet}

	testCases := []struct {
		name  string
		track func()
	}{
		{name: "discover", track: func() { TrackDiscover(1, "postgres") }},
		{name: "sync started", track: func() { TrackSyncStarted("s", mix, "postgres", destination, 1) }},
		{name: "sync completed", track: func() { TrackSyncCompleted("s", mix, destination, true, 1, 1) }},
		{name: "failure", track: func() { TrackFailure("sync", "postgres", "s", errs.Failure{Category: errs.AuthFailed}) }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capture := stubClient(t)
			capture.status = http.StatusServiceUnavailable

			assert.NotPanics(t, func() {
				tc.track()
				drain()
			})
			assert.Len(t, capture.events(t), 1, "the attempt is still made, the failure is only logged")
		})
	}
}

// TestSendEventRejectsUnmarshalableProperties covers the marshal error branch: a property that
// cannot be encoded must surface as an error rather than send a malformed body.
func TestSendEventRejectsUnmarshalableProperties(t *testing.T) {
	capture := stubClient(t)

	err := telemetry.sendEvent("Bad", map[string]any{"ch": make(chan int)})

	require.Error(t, err)
	assert.Empty(t, capture.bodies, "nothing may be sent when the payload cannot be built")
}

// TestGetUserID covers the distinct_id every event carries. It is read from disk when present,
// so the trimming and the generated fallback both have to hold: a changed id splits one user
// into two in every dashboard.
func TestGetUserID(t *testing.T) {
	testCases := []struct {
		name          string
		fileBody      string // written to <config folder>/user_id.txt; empty means no file
		writeFile     bool
		eventProps    map[string]any
		telemetryJSON string
		expectedID    string // empty means a fresh id is generated instead
		generated     bool
		checkNoIDFile bool
	}{
		// the file is written by a previous run and read back verbatim
		{name: "plain id from file", fileBody: "abc123", writeFile: true, expectedID: "abc123"},
		// logger.FileLogger writes JSON, so the id comes back quoted and must be trimmed
		{name: "quoted id is trimmed", fileBody: `"abc123"`, writeFile: true, expectedID: "abc123"},
		// an empty file is still a successful read, so it is honored rather than regenerated
		{name: "empty file", fileBody: "", writeFile: true, expectedID: ""},
		// first run on this machine
		{name: "no file, id is generated", generated: true},
		// telemetry.json distinct_id is the caller identity, user_id.txt is a per-machine guess
		{name: "distinct_id wins over the file", fileBody: "from-file", writeFile: true, eventProps: map[string]any{distinctIDKey: "from-ui"}, expectedID: "from-ui"},
		// a caller that sends context but no identity still leaves user_id.txt in charge
		{name: "absent distinct_id falls back", fileBody: "from-file", writeFile: true, eventProps: map[string]any{"job_id": 12}, expectedID: "from-file"},
		// "" is not an identity: attributing to it merges every such deployment into one user
		{name: "empty distinct_id falls back", fileBody: "from-file", writeFile: true, eventProps: map[string]any{distinctIDKey: ""}, expectedID: "from-file"},
		// whitespace-only is treated as missing, same as empty
		{name: "whitespace distinct_id falls back", fileBody: "from-file", writeFile: true, eventProps: map[string]any{distinctIDKey: "  "}, expectedID: "from-file"},
		// the file is written by another process, so the type is not guaranteed
		{name: "non-string distinct_id falls back", fileBody: "from-file", writeFile: true, eventProps: map[string]any{distinctIDKey: 42}, expectedID: "from-file"},
		// no telemetry.json at all: a standalone CLI run
		{name: "nil props fall back", fileBody: "from-file", writeFile: true, expectedID: "from-file"},
		// both files present: telemetry.json owns the identity
		{name: "telemetry.json wins over user_id.txt", fileBody: "from-file", writeFile: true, telemetryJSON: `{"distinct_id":"from-ui","service":"ui"}`, expectedID: "from-ui"},
		// a caller that has dropped user_id.txt entirely
		{name: "telemetry.json alone", telemetryJSON: `{"distinct_id":"from-ui"}`, expectedID: "from-ui", checkNoIDFile: true},
		// a corrupt telemetry.json must not lose the identity user_id.txt already holds
		{name: "malformed telemetry.json falls back", fileBody: "from-file", writeFile: true, telemetryJSON: `{"distinct_id":`, expectedID: "from-file"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			viper.Set(constants.ConfigFolder, dir)
			t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })

			if tc.writeFile {
				require.NoError(t, os.WriteFile(filepath.Join(dir, userIDFile+".txt"), []byte(tc.fileBody), 0o600))
			}
			if tc.telemetryJSON != "" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, eventPropsFile), []byte(tc.telemetryJSON), 0o600))
			}

			props := tc.eventProps
			if tc.telemetryJSON != "" {
				props = loadEventProps()
			}

			got := getUserID(props)

			if tc.generated {
				assert.Regexp(t, "^[0-9a-f]{32}$", got)
				return
			}
			assert.Equal(t, tc.expectedID, got)

			if tc.checkNoIDFile {
				_, err := os.Stat(filepath.Join(dir, userIDFile+".txt"))
				assert.True(t, os.IsNotExist(err), "no id file is written when the caller supplies one")
			}
		})
	}
}

// TestGeneratedUserIDPersists covers the round trip that keeps one machine one user: the first call generates and writes the id, later calls read it back through logger.FileLogger's JSON quotes.
func TestGeneratedUserIDPersists(t *testing.T) {
	dir := t.TempDir()
	viper.Set(constants.ConfigFolder, dir)
	t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })

	generated := getUserID(nil)
	require.Regexp(t, "^[0-9a-f]{32}$", generated)

	written, err := os.ReadFile(filepath.Join(dir, userIDFile+".txt"))
	require.NoError(t, err, "the id must be written for the next run to find")
	assert.Equal(t, generated, strings.Trim(string(written), `"`), "what is written is what was returned")

	assert.Equal(t, generated, getUserID(nil), "a later call must read the file, not generate a new id")
}

func TestGetService(t *testing.T) {
	testCases := []struct {
		name            string
		eventProps      map[string]any
		telemetryJSON   string
		expectedService string
	}{
		// the point of the key: a UI-driven run reports as ui
		{name: "caller service is used", eventProps: map[string]any{serviceKey: "ui"}, expectedService: "ui"},
		// the value lands in an event name, so stray whitespace would split a dashboard series
		{name: "service is trimmed", eventProps: map[string]any{serviceKey: "  ui  "}, expectedService: "ui"},
		// an older caller sends the file without the key
		{name: "absent key falls back", eventProps: map[string]any{distinctIDKey: "u1"}, expectedService: defaultService},
		// "Sync Started - " is not a usable event name
		{name: "empty service falls back", eventProps: map[string]any{serviceKey: ""}, expectedService: defaultService},
		{name: "whitespace service falls back", eventProps: map[string]any{serviceKey: "   "}, expectedService: defaultService},
		// the file is written by another process, so the type is not guaranteed
		{name: "non-string service falls back", eventProps: map[string]any{serviceKey: 42}, expectedService: defaultService},
		// no telemetry.json at all: a standalone CLI run
		{name: "nil props fall back", eventProps: nil, expectedService: defaultService},
		// the composition Init performs: service is read off telemetry.json
		{name: "telemetry.json service is used", telemetryJSON: `{"distinct_id":"from-ui","service":"ui"}`, expectedService: "ui"},
		{name: "telemetry.json without service falls back", telemetryJSON: `{"distinct_id":"from-ui"}`, expectedService: defaultService},
		{name: "malformed telemetry.json falls back", telemetryJSON: `{"service":`, expectedService: defaultService},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			props := tc.eventProps
			if tc.telemetryJSON != "" {
				dir := t.TempDir()
				viper.Set(constants.ConfigFolder, dir)
				t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })
				require.NoError(t, os.WriteFile(filepath.Join(dir, eventPropsFile), []byte(tc.telemetryJSON), 0o600))
				props = loadEventProps()
			}
			assert.Equal(t, tc.expectedService, getService(props))
		})
	}
}

func TestLoadEventProps(t *testing.T) {
	oversized := `{"pad":"` + strings.Repeat("a", maxEventPropsFileSize) + `"}`

	testCases := []struct {
		name     string
		body     string
		setup    string // file, mkdir, absent, no-folder, unopenable
		expected map[string]interface{}
	}{
		// the shape the UI writes next to the sync configs
		{
			name:  "flat object is read whole",
			body:  `{"schema_version":1,"service":"ui","distinct_id":"u1","job_id":12}`,
			setup: "file",
			expected: map[string]interface{}{
				"schema_version": float64(1),
				"service":        "ui",
				"distinct_id":    "u1",
				"job_id":         float64(12),
			},
		},
		// values are passed through untouched, so nested ones must survive to the payload
		{
			name:     "nested values survive",
			body:     `{"labels":{"env":"prod"},"tags":["a","b"]}`,
			setup:    "file",
			expected: map[string]interface{}{"labels": map[string]interface{}{"env": "prod"}, "tags": []interface{}{"a", "b"}},
		},
		// a caller with nothing to add still writes a valid file
		{name: "empty object", body: `{}`, setup: "file", expected: map[string]interface{}{}},
		// a half-written file, e.g. read while the caller is still writing it
		{name: "malformed json is ignored", body: `{"distinct_id":`, setup: "file"},
		// valid json of the wrong shape: decoding into a map fails
		{name: "json array is ignored", body: `["u1"]`, setup: "file"},
		// a created but never written file decodes to EOF
		{name: "empty file is ignored", body: ``, setup: "file"},
		// past the cap the read is cut short, so the object no longer parses
		{name: "oversized file is ignored", body: oversized, setup: "file"},
		// a path that is not a regular file must not panic the run
		{name: "directory at the path is ignored", setup: "mkdir"},
		// the standalone CLI case, and by far the common one
		{name: "absent file is ignored", setup: "absent"},
		{name: "no config folder", setup: "no-folder"},
		{name: "unopenable path", setup: "unopenable"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.setup {
			case "no-folder":
				viper.Set(constants.ConfigFolder, "")
				t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })
			case "unopenable":
				dir := t.TempDir()
				notADir := filepath.Join(dir, "not-a-dir")
				require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o600))
				viper.Set(constants.ConfigFolder, notADir)
				t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })
			default:
				dir := t.TempDir()
				viper.Set(constants.ConfigFolder, dir)
				t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })
				path := filepath.Join(dir, eventPropsFile)
				switch tc.setup {
				case "file":
					require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
				case "mkdir":
					require.NoError(t, os.Mkdir(path, 0o700))
				}
			}

			assert.Equal(t, tc.expected, loadEventProps())
		})
	}
}

func TestSendEventCallerContext(t *testing.T) {
	testCases := []struct {
		name         string
		service      string
		eventProps   map[string]interface{}
		props        map[string]any
		expectedName string
		expected     map[string]any
	}{
		// a UI-driven run suffixes the event and reports service as ui
		{
			name:         "service qualifies the event name",
			service:      "ui",
			expectedName: "Sync Started - ui",
			expected:     map[string]any{"service": "ui"},
		},
		// Init always sets service via getService; a direct CLI run is the default
		{
			name:         "CLI service qualifies the event name",
			service:      defaultService,
			expectedName: "Sync Started - CLI",
			expected:     map[string]any{"service": defaultService},
		},
		// telemetry.json is merged last: new keys are added, overlapping keys override
		{
			name:    "event props add and override",
			service: "ui",
			eventProps: map[string]interface{}{
				"job_id":         float64(12),
				"os":             "k8s",
				"source_type":    "from-ui",
				"distinct_id":    "from-ui",
				"service":        "ui",
				"schema_version": float64(1),
			},
			props:        map[string]any{"source_type": "postgres", "sync_id": "sync-1"},
			expectedName: "Sync Started - ui",
			expected: map[string]any{
				"job_id":         float64(12),
				"os":             "k8s",
				"source_type":    "from-ui",
				"sync_id":        "sync-1",
				"distinct_id":    "from-ui",
				"service":        "ui",
				"schema_version": float64(1),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capture := stubClient(t)
			telemetry.service = tc.service
			telemetry.eventProps = tc.eventProps

			require.NoError(t, telemetry.sendEvent(eventSyncStarted, tc.props))

			events := capture.events(t)
			require.Len(t, events, 1)
			assert.Equal(t, tc.expectedName, events[0]["__event"])
			for key, want := range tc.expected {
				assert.Equal(t, want, events[0][key], "property %q", key)
			}
		})
	}
}
