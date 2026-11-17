package performance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/require"
)

// The output is what rssCommand prints inside a sync container: olake as PID 1, the Java writer it
// starts, and the short-lived shell and grep of the read itself, which must not count.
func TestParseMemory(t *testing.T) {
	const olakeAndJava = "/proc/1/status:Name:\tolake\n" +
		"/proc/1/status:VmRSS:\t  1048576 kB\n" +
		"/proc/57/status:Name:\tjava\n" +
		"/proc/57/status:VmRSS:\t  5242880 kB\n"

	tests := []struct {
		name string
		out  string
		want float64
	}{
		{
			name: "olake and java are added up",
			out:  olakeAndJava,
			want: 6144,
		},
		{
			name: "shell and grep of the read are left out",
			out: olakeAndJava +
				"/proc/912/status:Name:\tsh\n" +
				"/proc/912/status:VmRSS:\t     1024 kB\n" +
				"/proc/913/status:Name:\tgrep\n" +
				"/proc/913/status:VmRSS:\t 99999999 kB\n",
			want: 6144,
		},
		{
			name: "a value that is not a number is skipped",
			out:  olakeAndJava + "/proc/914/status:Name:\tjava\n/proc/914/status:VmRSS:\tnot-a-number kB\n",
			want: 6144,
		},
		{
			name: "a line that is not a status field is skipped",
			out:  "garbage\n" + olakeAndJava,
			want: 6144,
		},
		{
			name: "no output reads zero",
			out:  "",
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseMemory(tt.out))
		})
	}
}

// The peak is the highest read and the P95 the nearest-rank 95th percentile of the reads.
func TestSummarizeMemory(t *testing.T) {
	tests := []struct {
		name    string
		samples []float64
		want    memoryUsage
	}{
		{
			// Shuffled, so neither value can come from the last reads; P95 is at rank ceil(0.95*20) = 19.
			name:    "twenty reads",
			samples: []float64{20, 3, 17, 1, 12, 8, 19, 5, 14, 10, 2, 16, 7, 11, 4, 18, 9, 13, 6, 15},
			want:    memoryUsage{PeakMiB: 20, P95MiB: 19},
		},
		{
			name:    "fewer than twenty reads put P95 at the peak",
			samples: []float64{3, 1, 2},
			want:    memoryUsage{PeakMiB: 3, P95MiB: 3},
		},
		{
			name:    "no reads",
			samples: nil,
			want:    memoryUsage{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, summarizeMemory(tt.samples))
		})
	}
}

// A missing benchmarks.json is a driver's first run, and one written before memory was tracked
// loads its RPS history with no memory history; only a file that cannot be read fails.
func TestLoadBenchmarks(t *testing.T) {
	tests := []struct {
		name    string
		content string // written to benchmarks.json; empty leaves the file missing
		wantErr bool
		wantRPS []float64
	}{
		{
			name:    "missing file starts an empty history",
			wantRPS: []float64{},
		},
		{
			name:    "file from before memory was tracked",
			content: `{"backfill":{"rps":[100,200],"updated_at":"2026-11-03T00:00:00Z"},"cdc":{"rps":[],"updated_at":"2026-11-03T00:00:00Z"}}`,
			wantRPS: []float64{100, 200},
		},
		{
			name:    "file that is not json",
			content: `{"backfill":`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "benchmarks.json")
			if tt.content != "" {
				require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))
			}

			store, err := loadBenchmarks(path)
			if tt.wantErr {
				require.True(t, err != nil, "loading an unreadable benchmarks.json succeeded")
				require.Contains(t, err.Error(), "failed to load benchmarks")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantRPS, store.phase(true).RPS)
			require.Empty(t, store.phase(true).PeakMemoryMiB)
			require.Empty(t, store.phase(true).P95MemoryMiB)
		})
	}
}

// Each recorded series keeps the last maxHistorySize runs in order, the first run creates the file,
// and a phase's runs never land in the other phase's history.
func TestBenchmarkStoreRecord(t *testing.T) {
	tests := []struct {
		name        string
		isBackfill  bool
		runs        int // run i records RPS i*10, peak i*100 MiB and P95 i*50 MiB
		wantRPS     []float64
		wantPeakMiB []float64
		wantP95MiB  []float64
	}{
		{
			name:        "first run creates the file",
			isBackfill:  true,
			runs:        1,
			wantRPS:     []float64{10},
			wantPeakMiB: []float64{100},
			wantP95MiB:  []float64{50},
		},
		{
			name:        "history over the window drops the oldest run",
			isBackfill:  true,
			runs:        maxHistorySize + 1,
			wantRPS:     []float64{20, 30, 40, 50, 60},
			wantPeakMiB: []float64{200, 300, 400, 500, 600},
			wantP95MiB:  []float64{100, 150, 200, 250, 300},
		},
		{
			name:        "cdc is recorded apart from backfill",
			isBackfill:  false,
			runs:        2,
			wantRPS:     []float64{10, 20},
			wantPeakMiB: []float64{100, 200},
			wantP95MiB:  []float64{50, 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "benchmarks.json")
			store, err := loadBenchmarks(path)
			require.NoError(t, err)

			for i := 1; i <= tt.runs; i++ {
				require.NoError(t, store.record(tt.isBackfill, float64(i*10), memoryUsage{PeakMiB: float64(i * 100), P95MiB: float64(i * 50)}))
			}

			reloaded, err := loadBenchmarks(path)
			require.NoError(t, err)
			recorded, other := reloaded.phase(tt.isBackfill), reloaded.phase(!tt.isBackfill)
			require.Equal(t, tt.wantRPS, recorded.RPS)
			require.Equal(t, tt.wantPeakMiB, recorded.PeakMemoryMiB)
			require.Equal(t, tt.wantP95MiB, recorded.P95MemoryMiB)
			require.Empty(t, other.RPS)
			require.Empty(t, other.PeakMemoryMiB)
			require.Empty(t, other.P95MemoryMiB)
		})
	}
}
