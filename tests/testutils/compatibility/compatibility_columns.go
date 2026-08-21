package compatibility

import (
	"fmt"
)

// ColumnRule is one column's backward-compatibility assertion policy, keyed on the baseline release
// under test. No rule -- the default -- means the column is asserted in full, on type and value.
//
// Two shapes of known finding map onto the two version fields. A column an old binary cannot sync
// AT ALL (it wedges or kills the run) is dropped from the seed data below the release that fixed
// it: ExcludeBelow. A column whose value legitimately changed at some release -- the current form
// is the intended one, the old form was the bug -- is compared by type only below that release and
// in full from it: AssertValueFrom. TypeOnly is the unconditional form of the latter.
type ColumnRule struct {
	Column string
	// ExcludeBelow drops the column from the seed data and the catalog when the baseline is older
	// than this release. For hard fails only: a baseline that cannot carry the column at any price.
	ExcludeBelow string
	// AssertValueFrom value-compares the column only when the baseline is at or after this release;
	// older baselines still assert its type through the schema comparison.
	AssertValueFrom string
	// TypeOnly never compares the value, whatever the baseline.
	TypeOnly bool
}

// columnPolicies is a rule set applied to one baseline: which columns stay out of the seed data,
// which are compared by type only, and a log-ready line per decision so the run's assertion
// surface is explicit in its output.
type columnPolicies struct {
	seedExcluded []string
	typeOnly     []string
	notes        []string
}

// resolveColumnPolicies evaluates a driver's rules against the baseline spec. A baseline that
// cannot be dated ("latest", an image ref, a commit sha) is treated as newest -- ExcludeBelow and
// AssertValueFrom never fire, only TypeOnly -- mirroring resolveInputGeneration's fallback.
// Malformed rules are an error, never a skip: a typo'd version must not silently change what a
// green run proves.
func resolveColumnPolicies(rules []ColumnRule, spec string) (*columnPolicies, error) {
	version, dated := parseReleaseTag(spec)
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
		if rule.ExcludeBelow == "" && rule.AssertValueFrom == "" && !rule.TypeOnly {
			return nil, fmt.Errorf("compatibility column rule for %q declares no policy", rule.Column)
		}
		if rule.TypeOnly && rule.AssertValueFrom != "" {
			return nil, fmt.Errorf("compatibility column rule for %q sets both TypeOnly and AssertValueFrom, which contradict", rule.Column)
		}

		if rule.ExcludeBelow != "" {
			boundary, ok := parseReleaseTag(rule.ExcludeBelow)
			if !ok {
				return nil, fmt.Errorf("compatibility column rule for %q: ExcludeBelow %q is not a release tag", rule.Column, rule.ExcludeBelow)
			}
			if dated && compareRelease(version, boundary) < 0 {
				policies.seedExcluded = append(policies.seedExcluded, rule.Column)
				policies.notes = append(policies.notes, fmt.Sprintf(
					"column %s: excluded from the seed data, baseline %s is older than %s", rule.Column, spec, rule.ExcludeBelow))
				// Absent from both runs, so its assertion policy is moot.
				continue
			}
		}

		switch {
		case rule.TypeOnly:
			policies.typeOnly = append(policies.typeOnly, rule.Column)
			policies.notes = append(policies.notes, fmt.Sprintf("column %s: type-only, never value-compared", rule.Column))
		case rule.AssertValueFrom != "":
			boundary, ok := parseReleaseTag(rule.AssertValueFrom)
			if !ok {
				return nil, fmt.Errorf("compatibility column rule for %q: AssertValueFrom %q is not a release tag", rule.Column, rule.AssertValueFrom)
			}
			if dated && compareRelease(version, boundary) < 0 {
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
