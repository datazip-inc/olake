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
	require.NoError(t, telemetry.sendEvent("Test Event - CLI", map[string]any{"custom": 1}))

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

	streamMixKeys := []string{
		"full_refresh_streams_count", "incremental_streams_count", "cdc_streams_count",
		"strict_cdc_streams_count", "selected_streams_count", "normalized_streams_count",
		"partitioned_streams_count",
	}

	testCases := []struct {
		name         string
		track        func()
		expectedKeys []string
	}{
		{
			name:         "discover",
			track:        func() { TrackDiscover(9, "postgres") },
			expectedKeys: []string{"stream_count", "source_type"},
		},
		{
			name:  "sync started",
			track: func() { TrackSyncStarted("sync-1", mix, "postgres", destination, 11) },
			expectedKeys: append([]string{
				"sync_start", "sync_id", "stream_count", "source_type", "destination_type", "catalog_type",
			}, streamMixKeys...),
		},
		{
			name:  "sync completed",
			track: func() { TrackSyncCompleted("sync-1", mix, destination, false, 100, 2048) },
			expectedKeys: append([]string{
				"sync_id", "sync_end", "sync_status", "records_synced", "bytes_read",
				"destination_type", "catalog_type",
			}, streamMixKeys...),
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

// TestAddStreamMix pins the seven per-sync counters and the values they carry.
func TestAddStreamMix(t *testing.T) {
	props := map[string]any{}
	addStreamMix(props, types.StreamMix{
		FullRefresh: 1, Incremental: 2, CDC: 3, StrictCDC: 4,
		Selected: 5, Normalized: 6, Partitioned: 7,
	})

	assert.Equal(t, map[string]any{
		"full_refresh_streams_count": 1,
		"incremental_streams_count":  2,
		"cdc_streams_count":          3,
		"strict_cdc_streams_count":   4,
		"selected_streams_count":     5,
		"normalized_streams_count":   6,
		"partitioned_streams_count":  7,
	}, props)
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
		require.NoError(t, telemetry.sendEvent("After Panic - CLI", nil))
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
		if Disabled() {
			t.Skip("TELEMETRY_DISABLED is set: Flush short-circuits and has nothing to wait for")
		}
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

// TestDisabledReadsTheEnvOnce covers the switch every call site gates on. The value is cached,
// so this asserts consistency rather than re-reading the environment.
func TestDisabledReadsTheEnvOnce(t *testing.T) {
	first := Disabled()
	assert.Equal(t, first, Disabled(), "Disabled must be stable within a process")

	if first {
		// Flush short-circuits before touching the WaitGroup when telemetry is off.
		start := time.Now()
		Flush()
		assert.Less(t, time.Since(start), 50*time.Millisecond)
	}
}

// TestFlushTimeoutIsSizedToOneRoundTrip pins the relationship the constant's comment claims:
// the flush budget matches what a single sendEvent is allowed to take.
func TestFlushTimeoutIsSizedToOneRoundTrip(t *testing.T) {
	assert.Equal(t, 5*time.Second, flushTimeout,
		"flushTimeout must stay in step with sendEvent's client and context budgets")
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

	err := telemetry.sendEvent("Bad - CLI", map[string]any{"ch": make(chan int)})

	require.Error(t, err)
	assert.Empty(t, capture.bodies, "nothing may be sent when the payload cannot be built")
}

// TestFlushTimesOut covers the deadline branch: a hung endpoint must not delay the exit past
// flushTimeout, which is the whole reason the wait runs in a goroutine.
func TestFlushTimesOut(t *testing.T) {
	// Skipped under -short, which is also how to run this package with -race: Flush abandons its
	// waiter at the deadline, still parked in inflight.Wait(), and a later Add on that WaitGroup
	// is a documented misuse the detector reports. Harmless in production, where the process
	// exits immediately after Flush returns.
	if testing.Short() {
		t.Skip("takes flushTimeout; also the -race escape hatch, see comment")
	}
	markInitDone()
	stubClient(t)

	withDisabled(t, false, func() {
		release := make(chan struct{})
		send("blocked", func() { <-release })

		start := time.Now()
		Flush()
		elapsed := time.Since(start)

		close(release)
		drain()

		assert.GreaterOrEqual(t, elapsed, flushTimeout, "Flush must wait the full budget")
		assert.Less(t, elapsed, flushTimeout+2*time.Second, "and must not wait appreciably longer")
	})
}

// TestGetUserID covers the distinct_id every event carries. It is read from disk when present,
// so the trimming and the generated fallback both have to hold: a changed id splits one user
// into two in every dashboard.
func TestGetUserID(t *testing.T) {
	testCases := []struct {
		name       string
		fileBody   string // written to <config folder>/user_id.txt; empty means no file
		writeFile  bool
		expectedID string // empty means a fresh id is generated instead
	}{
		// the file is written by a previous run and read back verbatim
		{name: "plain id from file", fileBody: "abc123", writeFile: true, expectedID: "abc123"},
		// logger.FileLogger writes JSON, so the id comes back quoted and must be trimmed
		{name: "quoted id is trimmed", fileBody: `"abc123"`, writeFile: true, expectedID: "abc123"},
		// an empty file is still a successful read, so it is honored rather than regenerated
		{name: "empty file", fileBody: "", writeFile: true, expectedID: ""},
		// first run on this machine
		{name: "no file, id is generated", writeFile: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			viper.Set(constants.ConfigFolder, dir)
			t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })

			if tc.writeFile {
				require.NoError(t, os.WriteFile(filepath.Join(dir, userIDFile+".txt"), []byte(tc.fileBody), 0o600))
			}

			got := getUserID()

			if tc.writeFile {
				assert.Equal(t, tc.expectedID, got)
				return
			}
			// a generated id is 32 hex characters, and must differ from the empty string
			assert.Len(t, got, 32)
			assert.Regexp(t, "^[0-9a-f]{32}$", got)
		})
	}
}

// TestGetUserIDIsStableAcrossReads covers the property the id exists for: the same config folder
// must always yield the same distinct_id.
func TestGetUserIDIsStableAcrossReads(t *testing.T) {
	dir := t.TempDir()
	viper.Set(constants.ConfigFolder, dir)
	t.Cleanup(func() { viper.Set(constants.ConfigFolder, "") })
	require.NoError(t, os.WriteFile(filepath.Join(dir, userIDFile+".txt"), []byte(`"stable-id"`), 0o600))

	first := getUserID()
	for range 20 {
		assert.Equal(t, first, getUserID())
	}
}
