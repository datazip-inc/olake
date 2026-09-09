package compatibility

import (
	"fmt"
	"maps"
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

// resolveColumnPolicies evaluates a driver's rules against the baseline spec (a commit arrives here
// as its equivalentRelease), filling the rule-derived half of an assertionPolicies -- seedExcluded,
// the dated type-only columns and a note per decision -- which resolveAssertionPolicies completes.
// A baseline that cannot be dated ("latest", an image ref) is treated as newest: ExcludeBelow and
// AssertValueFrom never fire, only TypeOnly. Malformed rules are an error, never a skip: a typo'd
// version must not silently change what a green run proves.
func resolveColumnPolicies(rules []ColumnRule, spec string) (*assertionPolicies, error) {
	version, isRelease := parseReleaseTag(spec)
	policies := &assertionPolicies{}
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

		switch {
		case rule.ExcludeBelow != "":
			boundary, ok := parseReleaseTag(rule.ExcludeBelow)
			if !ok {
				return nil, fmt.Errorf("compatibility column rule for %q: ExcludeBelow %q is not a release tag", rule.Column, rule.ExcludeBelow)
			}
			if isRelease && compareRelease(version, boundary) < 0 {
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
			if isRelease && compareRelease(version, boundary) < 0 {
				policies.typeOnly = append(policies.typeOnly, rule.Column)
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
// dated and unconditional -- with the always-volatile columns into one policy set for this baseline.
func resolveAssertionPolicies(fixture *TestHandler, spec string, driverRules compatibilityDriverRules, dataFormat string) (*assertionPolicies, error) {
	// The destinations' shared rules first (olake's own columns), then the driver's -- destination
	// columns and source columns are separate lists in the json -- then the data format's.
	typeRules := slices.Clone(compatibilityRules.Destinations.Rules)
	typeRules = append(typeRules, driverRules.DestinationRules...)
	typeRules = append(typeRules, driverRules.Rules...)
	typeRules = append(typeRules, driverRules.Variants[dataFormat].Rules...)

	// The columns a data_types rule can select: the driver's declared schema and the fixture's own
	// tags.
	columnTypes := map[string][]string{}
	for column, declared := range fixture.DeclaredSchema {
		if declared = strings.ToLower(strings.TrimSpace(declared)); declared != "" {
			columnTypes[column] = append(columnTypes[column], declared)
		}
	}
	for column, columnTags := range fixture.ColumnTypes {
		for _, tag := range columnTags {
			if !slices.Contains(columnTypes[column], tag) {
				columnTypes[column] = append(columnTypes[column], tag)
			}
		}
	}

	columnRules, alwaysTypeOnly, err := resolveTypeRules(typeRules, columnTypes)
	if err != nil {
		return nil, err
	}
	policies, err := resolveColumnPolicies(columnRules, spec)
	if err != nil {
		return nil, err
	}
	// Seed-excluded columns leave the catalog too, so streams.json never selects a column the
	// fixture left out of the table.
	policies.catalogExcluded = slices.Clone(policies.seedExcluded)

	// Compared by type but never by value: the driver's CDC columns (source-log coordinates),
	// the dated rules' columns, and the unconditional type_only ones -- olake's own and any
	// driver's exceptions among them, all from the json.
	volatile := map[string]bool{}
	for column := range fixture.CDCColumnsSchema {
		volatile[column] = true
	}
	for _, column := range append(policies.typeOnly, alwaysTypeOnly...) {
		volatile[column] = true
	}
	policies.typeOnly = slices.Sorted(maps.Keys(volatile))
	return policies, nil
}
