package compatibility

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// compatibilityInputGenerationEnvVar pins the input shape instead of deriving it from the baseline, so a
// single generation can be run against any binary. Accepts a generation name or "current".
const compatibilityInputGenerationEnvVar = "OLAKE_COMPATIBILITY_INPUT_GENERATION"

// inputGeneration is the shape of streams.json as of some release.
//
// Without this, both sides of a compatibility run get a streams.json written by TODAY's harness, and any
// key introduced after the baseline reads as a behavior change: the older binary ignores it and
// syncs something the candidate filters away. That is a config-format difference wearing the
// costume of a regression, and it is not what an upgrading user experiences -- their catalog was
// written for the binary they already run.
//
// A compatibility run therefore writes the generation its BASELINE shipped with, to BOTH sides. A
// surviving diff then means the candidate changed its mind about an input the baseline also
// understood, which is the actual backward-compatibility contract: old inputs keep working.
type inputGeneration struct {
	name string
	// introducedIn is the release that first understood this shape. A baseline at or after it,
	// and before the next generation's, runs on this one.
	introducedIn string
	// selectedColumns is `selected_columns`, added by #840 in v0.4.0. Older binaries ignore the
	// key and sync every column.
	selectedColumns bool
	// prefixFlag is `--destination-database-prefix`, added by #461 in v0.2.0. Cobra exits 1
	// SILENTLY on an unknown flag, so handing it to an older binary kills the sync with no
	// diagnostic -- a harness artifact, not a finding. The flag is optional on every version that
	// has it (no MarkFlagRequired anywhere), so omitting it is a legitimate configuration and
	// exercises what an upgrading pre-v0.2.0 pipeline actually does: never pass it at all.
	prefixFlag bool
}

// inputGenerations is ordered oldest first, so resolveInputGeneration can walk it and keep the
// newest entry the baseline is old enough to understand. Each boundary is a release that taught
// the binary a new input, so a baseline below it must not be handed that input.
var inputGenerations = []inputGeneration{
	{name: "pre-namespace", introducedIn: "v0.0.0"},
	{name: "prefix-flag", introducedIn: "v0.2.0", prefixFlag: true},
	{name: "selected-columns", introducedIn: "v0.4.0", prefixFlag: true, selectedColumns: true},
}

// dropUnsupportedFlags removes CLI flags the baseline's generation never had. It runs on every
// argument vector rather than at each call site, so a new caller cannot reintroduce the problem.
func dropUnsupportedFlags(gen *inputGeneration, flags []string) []string {
	if gen == nil || gen.prefixFlag {
		return flags
	}
	kept := make([]string, 0, len(flags))
	for i := 0; i < len(flags); i++ {
		switch {
		case flags[i] == destinationDBPrefixFlag:
			i++ // its value is the next element
		case strings.HasPrefix(flags[i], destinationDBPrefixFlag+"="):
		default:
			kept = append(kept, flags[i])
		}
	}
	return kept
}

const destinationDBPrefixFlag = "--destination-database-prefix"

// currentInputGeneration is the shape the harness writes by default, and what every suite other
// than compatibility runs on.
func currentInputGeneration() *inputGeneration {
	return &inputGenerations[len(inputGenerations)-1]
}

// resolveInputGeneration picks the newest generation the baseline understood, and returns a reason
// fit for logging. A spec that cannot be dated -- "latest", an image ref, a commit -- gets the
// current shape, which is correct for the default baseline and honest about what it proves.
func resolveInputGeneration(spec string) (*inputGeneration, string, error) {
	if forced := strings.TrimSpace(os.Getenv(compatibilityInputGenerationEnvVar)); forced != "" {
		if strings.EqualFold(forced, "current") {
			return currentInputGeneration(), fmt.Sprintf("%s=current", compatibilityInputGenerationEnvVar), nil
		}
		for i := range inputGenerations {
			if strings.EqualFold(inputGenerations[i].name, forced) {
				return &inputGenerations[i], fmt.Sprintf("%s=%s", compatibilityInputGenerationEnvVar, forced), nil
			}
		}
		return nil, "", fmt.Errorf("%s=%q names no known input generation; valid are %s or \"current\"",
			compatibilityInputGenerationEnvVar, forced, strings.Join(inputGenerationNames(), ", "))
	}

	version, ok := parseReleaseTag(spec)
	if !ok {
		return currentInputGeneration(), fmt.Sprintf(
			"baseline %q is not a release tag, so its input shape cannot be dated; using the current one", spec), nil
	}

	gen := &inputGenerations[0]
	for i := range inputGenerations {
		introduced, parsed := parseReleaseTag(inputGenerations[i].introducedIn)
		if parsed && compareRelease(version, introduced) >= 0 {
			gen = &inputGenerations[i]
		}
	}
	return gen, fmt.Sprintf("baseline %s shipped with the %q input shape", spec, gen.name), nil
}

func inputGenerationNames() []string {
	names := make([]string, 0, len(inputGenerations))
	for i := range inputGenerations {
		names = append(names, strconv.Quote(inputGenerations[i].name))
	}
	return names
}

// parseReleaseTag extracts MAJOR.MINOR.PATCH from a bare tag or a full image ref. Anything else,
// including a pre-release suffix, reports false rather than guessing at an ordering.
func parseReleaseTag(spec string) ([3]int, bool) {
	var version [3]int
	if i := strings.LastIndex(spec, ":"); i >= 0 {
		spec = spec[i+1:]
	}
	parts := strings.Split(strings.TrimPrefix(strings.TrimSpace(spec), "v"), ".")
	if len(parts) != 3 {
		return version, false
	}
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return version, false
		}
		version[i] = n
	}
	return version, true
}

func compareRelease(a, b [3]int) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// // applyInputGeneration rewrites one selected stream into the generation's shape. It runs as the
// // last step of updateSelectedStreams so no caller can forget it, and a nil generation -- every
// // suite but compatibility -- is a no-op.
// func applyInputGeneration(gen *inputGeneration, stream map[string]interface{}) error {
// 	if gen == nil {
// 		return nil
// 	}
// 	// This suite compares what a sync produces, never what a filter narrows, so no filter key
// 	// reaches either binary -- including the empty filter_config updateSelectedStreams writes,
// 	// which is itself a key pre-v0.6.0 baselines never knew.
// 	delete(stream, "filter_config")
// 	delete(stream, "filter")
// 	if !gen.selectedColumns {
// 		delete(stream, "selected_columns")
// 	}
// 	return nil
// }
