package compatibility

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"golang.org/x/mod/semver"
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

// resolveColumnPolicies resolves the dated rules against the baseline into seedExcluded, typeOnly and
// a note per decision. An undatable baseline ("latest", an image ref) reads as newest, so none fire.
func resolveColumnPolicies(rules []ColumnRule, spec string) *assertionPolicies {
	isRelease := semver.IsValid(spec)
	policies := &assertionPolicies{}
	for _, rule := range rules {
		if rule.ExcludeBelow != "" && isRelease && semver.Compare(spec, rule.ExcludeBelow) < 0 {
			policies.seedExcluded = append(policies.seedExcluded, rule.Column)
			policies.notes = append(policies.notes, fmt.Sprintf(
				"column %s: excluded from the seed data, baseline %s is older than %s", rule.Column, spec, rule.ExcludeBelow))
			// Absent from both runs, so its assertion policy is moot.
			continue
		}
		if rule.AssertValueFrom != "" && isRelease && semver.Compare(spec, rule.AssertValueFrom) < 0 {
			policies.typeOnly = append(policies.typeOnly, rule.Column)
			policies.notes = append(policies.notes, fmt.Sprintf(
				"column %s: type-only, baseline %s is older than %s", rule.Column, spec, rule.AssertValueFrom))
		}
	}
	if len(policies.notes) == 0 && len(rules) > 0 {
		policies.notes = append(policies.notes, fmt.Sprintf(
			"all %d column rules inactive against baseline %s; every column is fully asserted", len(rules), spec))
	}
	return policies
}

// assertionPolicies is every rule resolved against one baseline: the one set the run applies. The
// scenarios read catalogExcluded, the fixture's seeding reads seedExcluded, and the comparison
// reads typeOnly; nothing else consults the rules again.
type assertionPolicies struct {
	seedExcluded []string
	typeOnly     []string
	notes        []string
}

// resolveAssertionPolicies folds the driver's and variant's rules -- type-keyed and column-keyed,
// dated and unconditional -- with the always type-only columns into one policy set for this baseline.
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
	for column, declared := range fixture.DestinationSchema {
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
	policies := resolveColumnPolicies(columnRules, spec)

	// Compared by type but never by value: the driver's CDC columns (source-log coordinates),
	// the dated rules' columns, and the unconditional type_only ones -- olake's own and any
	// driver's exceptions among them, all from the json.
	typeOnly := map[string]bool{}
	for column := range fixture.CDCColumnsSchema {
		typeOnly[column] = true
	}
	for _, column := range append(policies.typeOnly, alwaysTypeOnly...) {
		typeOnly[column] = true
	}
	policies.typeOnly = slices.Sorted(maps.Keys(typeOnly))
	return policies, nil
}

// resolveTypeRules maps type-keyed rules onto the fixture's declared column types, merging
// multiple matches per column into one ColumnRule. A data_types rule matching no declared
// column is an error: the fixture does not carry the type, so the rule would assert nothing.
func resolveTypeRules(rules []compatibilityTypeRule, columnTypes map[string][]string) ([]ColumnRule, []string, error) {
	alwaysTypeOnly := map[string]bool{}
	merged := map[string]*ColumnRule{}
	apply := func(column string, policy compatibilityPolicy) error {
		if policy.TypeOnly {
			alwaysTypeOnly[column] = true
		}
		// Only a dated policy becomes a ColumnRule; type_only alone is carried by alwaysTypeOnly.
		if policy.ExcludeBelow == "" && policy.AssertValueFrom == "" {
			return nil
		}
		rule, ok := merged[column]
		if !ok {
			rule = &ColumnRule{Column: column}
			merged[column] = rule
		}
		if policy.ExcludeBelow != "" {
			if rule.ExcludeBelow != "" && rule.ExcludeBelow != policy.ExcludeBelow {
				return fmt.Errorf("column %s: conflicting exclude_below %s and %s", column, rule.ExcludeBelow, policy.ExcludeBelow)
			}
			rule.ExcludeBelow = policy.ExcludeBelow
		}
		if policy.AssertValueFrom != "" {
			if rule.AssertValueFrom != "" && rule.AssertValueFrom != policy.AssertValueFrom {
				return fmt.Errorf("column %s: conflicting assert_value_from %s and %s", column, rule.AssertValueFrom, policy.AssertValueFrom)
			}
			rule.AssertValueFrom = policy.AssertValueFrom
		}
		return nil
	}

	columns := make([]string, 0, len(columnTypes))
	for column := range columnTypes {
		columns = append(columns, column)
	}
	slices.Sort(columns)

	for _, r := range rules {
		switch {
		case r.Column != "":
			if err := apply(r.Column, r.compatibilityPolicy); err != nil {
				return nil, nil, err
			}
		case len(r.DataTypes) > 0:
			found := false
			for _, column := range columns {
				matches := slices.ContainsFunc(r.DataTypes, func(dataType string) bool {
					return slices.Contains(columnTypes[column], dataType)
				})
				if !matches {
					continue
				}
				if err := apply(column, r.compatibilityPolicy); err != nil {
					return nil, nil, err
				}
				found = true
			}
			if !found {
				return nil, nil, fmt.Errorf("no declared column matches data_types %v (%s); tag the column in the fixture's ColumnTypes or drop the rule", r.DataTypes, r.Note)
			}
		}
	}

	out := make([]ColumnRule, 0, len(merged))
	for _, column := range slices.Sorted(maps.Keys(merged)) {
		out = append(out, *merged[column])
	}
	return out, slices.Collect(maps.Keys(alwaysTypeOnly)), nil
}
