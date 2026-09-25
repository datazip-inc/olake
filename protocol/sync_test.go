package protocol

import (
	"os"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This package's init runs RootCmd.Execute, so the file logger is already pointed at ./logs
// before a test can redirect it. classifyStreams logs its skips, which creates that directory;
// drop it afterwards rather than leaving it in the tree.
func discardStrayLogs(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("logs"); err == nil {
		return // already there before the test; not ours to remove
	}
	t.Cleanup(func() { _ = os.RemoveAll("logs") })
}

// stream is one configured stream in a test catalog. The metadata lives on the catalog's
// selected_streams block, not here: classifyStreams overwrites StreamMetadata from that map.
type stream struct {
	name        string
	mode        types.SyncMode
	normalized  bool
	partitioned bool
	unselected  bool                // present in streams but absent from selected_streams
	filter      *types.FilterConfig // only read when normalized, so it can be made invalid
	updateType  string              // selected_streams update_type; blank as legacy catalogs leave it
	available   []types.UpdateType  // stream's available_update_types; empty on legacy catalogs
}

// catalogOf builds the two halves classifyStreams reads: the configured streams and the
// selected_streams metadata keyed by namespace.
func catalogOf(streams ...stream) *types.Catalog {
	catalog := &types.Catalog{SelectedStreams: map[string][]types.StreamMetadata{}}
	for _, s := range streams {
		catalog.Streams = append(catalog.Streams, &types.ConfiguredStream{
			Stream: &types.Stream{Name: s.name, Namespace: "public", SyncMode: s.mode, AvailableUpdateTypes: s.available},
		})
		if s.unselected {
			continue
		}
		metadata := types.StreamMetadata{StreamName: s.name, Normalization: s.normalized, FilterConfig: s.filter, UpdateType: s.updateType}
		if s.partitioned {
			metadata.PartitionRegex = "/{now,year}"
		}
		catalog.SelectedStreams["public"] = append(catalog.SelectedStreams["public"], metadata)
	}
	return catalog
}

// TestClassifyStreamsMix covers the counters the two sync events carry. They are counted inside
// classifyStreams so they cannot disagree with the run, which only holds if every counter sits
// past the skip branches: a stream that is not synced must not appear in any of them.
func TestClassifyStreamsMix(t *testing.T) {
	testCases := []struct {
		name         string
		streams      []stream
		expectedMix  types.StreamMix
		expectedCode string // non-empty: no stream survives, so the run fails with this code
	}{
		// each mode lands in its own counter, and an unknown mode falls to full refresh
		{
			name: "one stream per sync mode",
			streams: []stream{
				{name: "a", mode: types.FULLREFRESH},
				{name: "b", mode: types.INCREMENTAL},
				{name: "c", mode: types.CDC},
				{name: "d", mode: types.STRICTCDC},
			},
			expectedMix: types.StreamMix{FullRefresh: 1, Incremental: 1, CDC: 1, StrictCDC: 1, Selected: 4},
		},
		// CDC and STRICTCDC share one read path; telemetry must still tell them apart
		{
			name: "cdc and strict cdc are counted separately",
			streams: []stream{
				{name: "a", mode: types.CDC},
				{name: "b", mode: types.CDC},
				{name: "c", mode: types.STRICTCDC},
			},
			expectedMix: types.StreamMix{CDC: 2, StrictCDC: 1, Selected: 3},
		},
		// normalization and partitioning cut across sync mode rather than replacing it
		{
			name: "normalized and partitioned are independent of mode",
			streams: []stream{
				{name: "a", mode: types.FULLREFRESH, normalized: true},
				{name: "b", mode: types.CDC, partitioned: true},
				{name: "c", mode: types.INCREMENTAL, normalized: true, partitioned: true},
			},
			expectedMix: types.StreamMix{
				FullRefresh: 1, CDC: 1, Incremental: 1,
				Selected: 3, Normalized: 2, Partitioned: 2,
			},
		},
		// a stream missing from selected_streams is never synced, so it reaches no counter
		{
			name: "unselected streams are not counted",
			streams: []stream{
				{name: "a", mode: types.CDC},
				{name: "b", mode: types.FULLREFRESH, unselected: true},
				{name: "c", mode: types.INCREMENTAL, unselected: true},
			},
			expectedMix: types.StreamMix{CDC: 1, Selected: 1},
		},
		// nor does one skipped later, for a filter that cannot be applied
		{
			name: "streams skipped for an invalid filter are not counted",
			streams: []stream{
				{name: "a", mode: types.CDC},
				{
					name: "b", mode: types.FULLREFRESH, normalized: true,
					filter: &types.FilterConfig{Conditions: []types.FilterCondition{{Column: ""}}},
				},
				{
					name: "c", mode: types.INCREMENTAL, normalized: true,
					filter: &types.FilterConfig{Conditions: []types.FilterCondition{
						{Column: "x"}, {Column: "y"}, {Column: "z"},
					}},
				},
			},
			expectedMix: types.StreamMix{CDC: 1, Selected: 1},
		},
		// discover clears update_type the target engines cannot read; the stream must wait for
		// an explicit choice, while a legacy catalog without the list still defaults to equality
		{
			name: "streams skipped for an unset or unreadable update type are not counted",
			streams: []stream{
				{name: "a", mode: types.CDC},
				{name: "b", mode: types.CDC, available: []types.UpdateType{types.UpdateTypePosition}},
				{name: "c", mode: types.CDC, updateType: "eq", available: []types.UpdateType{types.UpdateTypePosition}},
				{name: "d", mode: types.CDC, updateType: "pos", available: []types.UpdateType{types.UpdateTypePosition}},
			},
			expectedMix: types.StreamMix{CDC: 2, Selected: 2, StreamWithPosUpdateType: 1},
		},
		// with nothing left to sync there is no mix to report, and the run fails before the
		// sync events are sent rather than reporting a run of zero streams
		{
			name:         "no stream survives selection",
			streams:      []stream{{name: "a", mode: types.CDC, unselected: true}},
			expectedCode: codeNoValidStreams,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			discardStrayLogs(t)

			got, err := classifyStreams(catalogOf(tc.streams...), nil, &types.State{})
			if tc.expectedCode != "" {
				require.Error(t, err)
				assert.Nil(t, got)

				failure := errs.From(errs.Classify(err))
				assert.Equal(t, errs.ConfigInvalid, failure.Category)
				assert.Equal(t, tc.expectedCode, failure.Code)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedMix, got.Mix)

			// the property the payload is read on: every synced stream is in exactly one
			// mode counter, so the four of them account for Selected and nothing else.
			assert.Equal(t, got.Mix.Selected, got.Mix.FullRefresh+got.Mix.Incremental+got.Mix.CDC+got.Mix.StrictCDC,
				"the sync-mode counters must sum to selected_streams_count")
			assert.Equal(t, len(got.SelectedStreams), got.Mix.Selected,
				"selected_streams_count must match the streams the sync was handed")
		})
	}
}
