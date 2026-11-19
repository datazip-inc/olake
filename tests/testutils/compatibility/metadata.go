package compatibility

// Comparing the two runs' destination metadata.
//
// compareDestinationOutputs proves both sides hold the same rows. Two destinations can hold the same
// rows and still differ where readers and resumes look: one more snapshot per sync, equality deletes
// where the other wrote positional ones, a column without stats, an olake_2pc a later sync cannot
// resume from. testutils reads and normalizes each side's metadata (testutils.ReadIcebergMetadata,
// testutils.ReadParquetMetadata); this file compares the two as one flat map, so a difference reads
// as one line per key.

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

// compareDestinationMetadata asserts the reference's metadata survives the upgrade. It runs after
// compareDestinationOutputs, so the rows already match.
//
// The reference must be a subset of the upgrade: every key it has must hold the same value after the
// upgrade, since a removed or changed key is what an older reader or resume trips on. A key only the
// upgrade has is an addition, which is not incompatible, so it is logged rather than failed. Extra
// output still fails, because every count also feeds a total the reference has (see flatten).
//
// A typeOnly column's values are not compared, as they may legitimately differ, so neither is its
// null count: a value that turns from NULL into data changes the count without breaking anything.
func compareDestinationMetadata(t *testing.T, diag *diagnostics, reference, upgraded testutils.DestinationMetadata, opaque bool, typeOnly []string) {
	referenceFlat, upgradedFlat := flatten(reference, opaque), flatten(upgraded, opaque)
	for _, col := range typeOnly {
		// A rule may name the source column; the destination writes it reformatted (see opaquePartitions).
		for _, name := range []string{col, testutils.Reformat(col)} {
			key := testutils.MetaColumnStats + "." + name + ".null_count"
			delete(referenceFlat, key)
			delete(upgradedFlat, key)
		}
	}
	var added []string
	maps.DeleteFunc(upgradedFlat, func(key, _ string) bool {
		if _, inReference := referenceFlat[key]; inReference {
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
		t.Logf("metadata the upgrade added, not compared: %v", added)
	}
	t.Logf("metadata identical: %d keys compared", len(referenceFlat))
}

// countedCategories are the per-partition categories flatten totals. A total is written even when
// it is zero, so output the reference never had -- a first delete file, a file in a new partition --
// changes a key the reference has instead of only adding keys, which the subset comparison allows.
var countedCategories = []string{testutils.MetaFiles, testutils.MetaDataRecords, testutils.MetaDeleteRecords}

// flatten is the compared form: "category.key" -> value. With opaque partitions -- partitioned on a
// column whose values cannot match across two runs, such as MongoDB's server-generated _id -- each
// partition is reduced to its counts, and what is compared is how many partitions have each set of
// counts.
func flatten(m testutils.DestinationMetadata, opaque bool) map[string]string {
	flat := map[string]string{}
	for category, values := range m.Values {
		for key, value := range values {
			flat[category+"."+key] = value
		}
	}
	counts := map[string]int64{}
	for _, category := range countedCategories {
		counts[category+".total"] = 0
	}
	for category, partitions := range m.Partitions {
		for partition, partitionCounts := range partitions {
			for _, n := range partitionCounts {
				counts[category+".total"] += n
			}
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
