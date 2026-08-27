package compatibility

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
)

// ColumnRule is one column's backward-compatibility assertion policy, keyed on the baseline release
// under test. No rule -- the default -- means the column is asserted in full, on type and value.
type ColumnRule struct {
	Column string
	// ExcludeBelow drops the column from the seed data and the catalog when the baseline is older
	// than this release. For hard fails only: a baseline that cannot carry the column at any price.
	ExcludeBelow string
	// AssertValueFrom value-compares the column only when the baseline is at or after this release;
	// older baselines still assert its type through the schema comparison.
	AssertValueFrom string
}

// columnPolicies is a rule set applied to one baseline: which columns stay out of the seed data,
// which are compared by type only, and a log-ready line per decision so the run's assertion
// surface is explicit in its output.
type columnPolicies struct {
	seedExcluded       []string
	assertDatatypeOnly []string
	notes              []string
}

// resolveColumnPolicies evaluates a driver's rules against the baseline spec. A baseline that
// cannot be dated ("latest", an image ref, a commit sha) is treated as newest -- ExcludeBelow and
// AssertValueFrom never fire, only TypeOnly -- mirroring resolveInputGeneration's fallback.
// Malformed rules are an error, never a skip: a typo'd version must not silently change what a
// green run proves.
func resolveColumnPolicies(rules []ColumnRule, spec string) (*columnPolicies, error) {
	version, canCompare := parseReleaseTag(spec)
	policies := &columnPolicies{}
	seen := make(map[string]bool, len(rules))
	for _, rule := range rules {
		if rule.Column == "" {
			return nil, fmt.Errorf("compatibility column rule with an empty column name: %+v", rule)
		}
		if seen[rule.Column] {
			return nil, fmt.Errorf("duplicate compatibility column rule for %q; one rule carries every policy for a column", rule.Column)
		}
		seen[rule.Column] = true
		if rule.ExcludeBelow == "" && rule.AssertValueFrom == "" {
			return nil, fmt.Errorf("compatibility column rule for %q declares no policy", rule.Column)
		}

		if rule.ExcludeBelow != "" {
			boundary, ok := parseReleaseTag(rule.ExcludeBelow)
			if !ok {
				return nil, fmt.Errorf("compatibility column rule for %q: ExcludeBelow %q is not a release tag", rule.Column, rule.ExcludeBelow)
			}
			if canCompare && compareRelease(version, boundary) < 0 {
				policies.seedExcluded = append(policies.seedExcluded, rule.Column)
				policies.notes = append(policies.notes, fmt.Sprintf(
					"column %s: excluded from the seed data, baseline %s is older than %s", rule.Column, spec, rule.ExcludeBelow))
				// Absent from both runs, so its assertion policy is moot.
				continue
			}
		}

		switch {
		case rule.AssertValueFrom != "":
			boundary, ok := parseReleaseTag(rule.AssertValueFrom)
			if !ok {
				return nil, fmt.Errorf("compatibility column rule for %q: AssertValueFrom %q is not a release tag", rule.Column, rule.AssertValueFrom)
			}
			if canCompare && compareRelease(version, boundary) < 0 {
				policies.assertDatatypeOnly = append(policies.assertDatatypeOnly, rule.Column)
				policies.notes = append(policies.notes, fmt.Sprintf(
					"column %s: type-only, baseline %s is older than %s", rule.Column, spec, rule.AssertValueFrom))
			}
		}
	}
	if len(policies.notes) == 0 && len(rules) > 0 {
		policies.notes = append(policies.notes, fmt.Sprintf(
			"all %d column rules inactive against baseline %s; every column is fully asserted", len(rules), spec))
	}
	return policies, nil
}

// assertionPolicies is every rule resolved against one baseline: the one set the run applies. The
// scenarios read catalogExcluded, the fixture's seeding reads seedExcluded, and the comparison
// reads typeOnly; nothing else consults the rules again.
type assertionPolicies struct {
	seedExcluded    []string
	catalogExcluded []string
	typeOnly        []string
	notes           []string
}

// resolveAssertionPolicies folds the driver's and variant's rules -- type-keyed and column-keyed,
// dated and unconditional -- with the always-volatile columns into one policy set for this
// baseline. Thresholds at or below the sweep's floor are dead config and fail loudly.
func resolveAssertionPolicies(fixture *Test, spec, floorTag string, globalFloor [3]int, driverRules compatibilityDriverRules, variantRules compatibilityVariantRules) (*assertionPolicies, error) {
	// The destinations' shared rules first (olake's own columns), then the driver's -- destination
	// columns and source columns are separate lists in the json -- then the variant's.
	typeRules := slices.Clone(compatibilityRules.Destinations.Rules)
	typeRules = append(typeRules, driverRules.DestinationRules...)
	typeRules = append(typeRules, driverRules.Rules...)
	typeRules = append(typeRules, variantRules.Rules...)
	for _, rule := range typeRules {
		for _, threshold := range []string{rule.ExcludeBelow, rule.AssertValueFrom} {
			if threshold == "" {
				continue
			}
			bound, ok := parseReleaseTag(threshold)
			if !ok {
				return nil, fmt.Errorf("compatibility_rules.json: %q is not a release tag", threshold)
			}
			if compareRelease(bound, globalFloor) <= 0 {
				return nil, fmt.Errorf("compatibility_rules.json: rule threshold %s is at or below the oldest reachable baseline %s, so it can never fire (%s); drop the rule or record it as a note",
					threshold, floorTag, rule.Note)
			}
		}
	}

	// The columns a data_types rule can select: the driver's declared schema, the fixture's own
	// tags, and the json column_types tags.
	columnTypes := map[string][]string{}
	for column, declared := range fixture.DeclaredSchema {
		if declared = strings.ToLower(strings.TrimSpace(declared)); declared != "" {
			columnTypes[column] = append(columnTypes[column], declared)
		}
	}
	for _, tags := range []map[string][]string{fixture.ColumnTypes, driverRules.ColumnTypes, variantRules.ColumnTypes} {
		for column, columnTags := range tags {
			for _, tag := range columnTags {
				if !slices.Contains(columnTypes[column], tag) {
					columnTypes[column] = append(columnTypes[column], tag)
				}
			}
		}
	}

	columnRules, alwaysTypeOnly, err := resolveTypeRules(typeRules, columnTypes)
	if err != nil {
		return nil, err
	}
	dated, err := resolveColumnPolicies(columnRules, spec)
	if err != nil {
		return nil, err
	}
	policies := &assertionPolicies{seedExcluded: dated.seedExcluded, notes: dated.notes}
	// Seed-excluded columns leave the catalog too, so streams.json never selects a column the
	// fixture left out of the table; the env sweep hook appends.
	policies.catalogExcluded = slices.Clone(dated.seedExcluded)
	if raw := os.Getenv(compatibilityExcludeColumnsEnvVar); raw != "" {
		policies.catalogExcluded = append(policies.catalogExcluded, strings.Split(raw, ",")...)
	}

	// Compared by type but never by value: the driver's CDC columns (source-log coordinates),
	// the dated rules' columns, and the unconditional type_only ones -- olake's own and any
	// driver's exceptions among them, all from the json. The destinations' value_compared list
	// carves the deterministic columns back out of the CDC set; a driver re-adds one with a rule.
	volatile := map[string]bool{}
	for column := range fixture.CDCColumnsSchema {
		volatile[column] = true
	}
	for _, column := range compatibilityRules.Destinations.ValueCompared.Columns {
		delete(volatile, column)
	}
	for _, column := range append(dated.assertDatatypeOnly, alwaysTypeOnly...) {
		volatile[column] = true
	}
	policies.typeOnly = slices.Sorted(maps.Keys(volatile))
	return policies, nil
}
