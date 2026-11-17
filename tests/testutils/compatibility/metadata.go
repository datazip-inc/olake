package compatibility

// Comparing the two runs' destination metadata.
//
// compareDestinationOutputs proves both sides hold the same rows. Two destinations can hold the same
// rows and still differ where readers and resumes look: one more snapshot per sync, equality deletes
// where the other wrote positional ones, a column without stats, an olake_2pc a later sync cannot
// resume from. testutils reads and normalizes each side's metadata (testutils.ReadIcebergMetadata,
// testutils.ReadParquetMetadata); this file applies the suite's rules to both and compares the rest
// as one flat map, so a difference reads as one line per key.

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// additiveCategories are the categories where the upgrade may carry keys the reference lacks: a new
// table property or a new olake_2pc field describes the table without changing what was written.
// Everywhere else a key only the upgrade has is extra output -- another delete file, another
// partition, another commit -- so keys must match both ways.
var additiveCategories = []string{testutils.MetaProperties, testutils.MetaOlake2PC}

// compareDestinationMetadata asserts both sides' metadata match outside the skipped categories. It
// runs after compareDestinationOutputs, so the rows already match. Every reference key must hold the
// same value after the upgrade; a key only the upgrade has passes in additiveCategories alone.
func compareDestinationMetadata(t *testing.T, diag *diagnostics, reference, upgraded testutils.DestinationMetadata, opaque bool, skip []string) {
	referenceFlat, upgradedFlat := flatten(reference, opaque, skip), flatten(upgraded, opaque, skip)
	var added []string
	maps.DeleteFunc(upgradedFlat, func(key, _ string) bool {
		_, inReference := referenceFlat[key]
		category, _, _ := strings.Cut(key, ".")
		if inReference || !slices.Contains(additiveCategories, category) {
			return false
		}
		added = append(added, key)
		return true
	})
	if !maps.Equal(referenceFlat, upgradedFlat) {
		diag.fatalf(t, "destination metadata differs between the reference and upgrade runs.\n%s",
			indent(require.MapDiff("metadata key", "reference run", "post olake upgrade", referenceFlat, upgradedFlat), "  "))
	}
	if len(added) > 0 {
		slices.Sort(added)
		t.Logf("metadata the upgrade added, allowed as additive: %v", added)
	}
	t.Logf("metadata identical: %d keys compared, categories skipped by rule: %v", len(referenceFlat), skip)
}

// flatten is the compared form: "category.key" -> value, without the skipped categories. With opaque
// partitions -- partitioned on a column whose values cannot match across two runs, such as MongoDB's
// server-generated _id -- each partition is reduced to its counts, and what is compared is how many
// partitions have each set of counts.
func flatten(m testutils.DestinationMetadata, opaque bool, skip []string) map[string]string {
	flat := map[string]string{}
	for category, values := range m.Values {
		if slices.Contains(skip, category) {
			continue
		}
		for key, value := range values {
			flat[category+"."+key] = value
		}
	}
	counts := map[string]int64{}
	for category, partitions := range m.Partitions {
		if slices.Contains(skip, category) {
			continue
		}
		for partition, partitionCounts := range partitions {
			if opaque {
				counts[category+".partitions["+partitionSignature(partitionCounts)+"]"]++
				continue
			}
			for key, n := range partitionCounts {
				counts[category+"."+strings.ReplaceAll(key, "%p", partition)] += n
			}
		}
	}
	for key, n := range counts {
		flat[key] = strconv.FormatInt(n, 10)
	}
	return flat
}

// partitionSignature is a partition's counts without the partition, e.g. "data=2, delete position parquet []=1".
func partitionSignature(counts map[string]int64) string {
	signature := make([]string, 0, len(counts))
	for key, n := range counts {
		label := strings.Join(strings.Fields(strings.ReplaceAll(key, "%p", "")), " ")
		if label == "" {
			label = "records"
		}
		signature = append(signature, fmt.Sprintf("%s=%d", label, n))
	}
	slices.Sort(signature)
	return strings.Join(signature, ", ")
}

// opaquePartitions reports whether any partition source column is compared by type only, so its
// values -- and the partition values made from them -- differ between the two runs by construction.
func opaquePartitions(partitionColumns, typeOnly []string) bool {
	return slices.ContainsFunc(partitionColumns, func(column string) bool {
		return slices.Contains(typeOnly, column) || slices.Contains(typeOnly, testutils.Reformat(column))
	})
}
