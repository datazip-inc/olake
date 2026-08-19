package testutils

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// compatibility_rules.json is the compat suite's whole configuration: per-driver and per-group
// baseline gates, and the column rules that decide what each column can be asserted on. Adding a
// driver, a gate or a rule is an edit to that file alone. What stays in code is only what binds a
// name to behaviour: the group list in compatGroupSpecs, since each variant calls a harness func.

// compatGate bounds which baselines a scope runs against. Empty fields mean no bound.
type compatGate struct {
	MinBaseline   string   `json:"min_baseline"`
	SkipBaselines []string `json:"skip_baselines"`
	Note          string   `json:"note"`
}

// compatTypeRule selects columns by data-type tag (resolved through CompatColumnTypes) or by an
// olake-owned column name, and carries the same policy fields as CompatColumnRule.
type compatTypeRule struct {
	DataTypes       []string `json:"data_types"`
	Column          string   `json:"column"`
	ExcludeBelow    string   `json:"exclude_below"`
	AssertValueFrom string   `json:"assert_value_from"`
	TypeOnly        bool     `json:"type_only"`
	Note            string   `json:"note"`
}

// compatVariantRules gates and rules for one source data format (s3's csv/json/parquet).
type compatVariantRules struct {
	compatGate
	Rules []compatTypeRule `json:"rules"`
}

type compatDriverRules struct {
	compatGate
	Groups   map[string]compatGate         `json:"groups"`
	Rules    []compatTypeRule              `json:"rules"`
	Variants map[string]compatVariantRules `json:"variants"`
}

type compatRulesConfig struct {
	Groups  map[string]compatGate        `json:"groups"`
	Drivers map[string]compatDriverRules `json:"drivers"`
}

//go:embed compatibility_rules.json
var rawCompatRules []byte

// compatRules is parsed and validated at package init, so a malformed or misspelled rules file
// fails every suite loudly instead of silently under-enforcing.
var compatRules = func() compatRulesConfig {
	var cfg compatRulesConfig
	dec := json.NewDecoder(bytes.NewReader(rawCompatRules))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		panic("tests/testutils/compatibility_rules.json: " + err.Error())
	}
	if err := cfg.validate(); err != nil {
		panic("tests/testutils/compatibility_rules.json: " + err.Error())
	}
	return cfg
}()

var knownCompatDrivers = []constants.DriverType{
	constants.MongoDB, constants.Postgres, constants.MySQL, constants.Oracle,
	constants.DB2, constants.S3, constants.Kafka, constants.MSSQL,
}

// validate rejects what would otherwise fail open: a driver or group name nothing matches, and a
// baseline that is not a release tag. Either would silently drop the gate or rule it carries.
func (c compatRulesConfig) validate() error {
	groupNames := make([]string, 0, len(compatGroupSpecs()))
	for _, spec := range compatGroupSpecs() {
		groupNames = append(groupNames, spec.name)
	}
	checkGroups := func(scope string, groups map[string]compatGate) error {
		for name, gate := range groups {
			if !slices.Contains(groupNames, name) {
				return fmt.Errorf("%s: unknown group %q (known: %v)", scope, name, groupNames)
			}
			if err := gate.validate(scope + " group " + name); err != nil {
				return err
			}
		}
		return nil
	}
	if err := checkGroups("groups", c.Groups); err != nil {
		return err
	}
	for name, driver := range c.Drivers {
		if !slices.Contains(knownCompatDrivers, constants.DriverType(name)) {
			return fmt.Errorf("unknown driver %q (known: %v)", name, knownCompatDrivers)
		}
		if err := driver.validate(name); err != nil {
			return err
		}
		if err := checkGroups("driver "+name, driver.Groups); err != nil {
			return err
		}
		for format, variant := range driver.Variants {
			scope := fmt.Sprintf("driver %s variant %s", name, format)
			if err := variant.validate(scope); err != nil {
				return err
			}
			if err := validateRules(scope, variant.Rules); err != nil {
				return err
			}
		}
		if err := validateRules("driver "+name, driver.Rules); err != nil {
			return err
		}
	}
	return nil
}

func (g compatGate) validate(scope string) error {
	for _, tag := range append([]string{g.MinBaseline}, g.SkipBaselines...) {
		if tag == "" {
			continue
		}
		if _, ok := parseReleaseTag(tag); !ok {
			return fmt.Errorf("%s: %q is not a release tag", scope, tag)
		}
	}
	return nil
}

func validateRules(scope string, rules []compatTypeRule) error {
	for i, r := range rules {
		if r.Column == "" && len(r.DataTypes) == 0 {
			return fmt.Errorf("%s rule %d: selects nothing (needs column or data_types): %s", scope, i, r.Note)
		}
		if r.Column != "" && len(r.DataTypes) > 0 {
			return fmt.Errorf("%s rule %d: sets both column and data_types; use one", scope, i)
		}
		if r.ExcludeBelow == "" && r.AssertValueFrom == "" && !r.TypeOnly {
			return fmt.Errorf("%s rule %d: asserts nothing (needs exclude_below, assert_value_from or type_only): %s", scope, i, r.Note)
		}
		for _, tag := range []string{r.ExcludeBelow, r.AssertValueFrom} {
			if tag == "" {
				continue
			}
			if _, ok := parseReleaseTag(tag); !ok {
				return fmt.Errorf("%s rule %d: %q is not a release tag", scope, i, tag)
			}
		}
	}
	return nil
}

// skipReason says why a baseline is out of this gate's range ("" = it runs). Tags are validated at
// load, so anything unparseable here is a bug rather than bad config.
func (g compatGate) skipReason(version [3]int, dated bool) string {
	if !dated {
		return ""
	}
	if g.MinBaseline != "" {
		if boundary, ok := parseReleaseTag(g.MinBaseline); ok && compareRelease(version, boundary) < 0 {
			return fmt.Sprintf("baseline is older than %s", g.MinBaseline)
		}
	}
	for _, skip := range g.SkipBaselines {
		if boundary, ok := parseReleaseTag(skip); ok && compareRelease(version, boundary) == 0 {
			return fmt.Sprintf("baseline %s is a known bounded regression here", skip)
		}
	}
	return ""
}

// mergedGate overlays a driver's gate for a group on the global one: the higher floor wins and the
// skip windows union, so a driver can only ever narrow what it runs.
func mergedGate(global, driver compatGate) compatGate {
	merged := compatGate{MinBaseline: global.MinBaseline, Note: global.Note}
	if driver.MinBaseline != "" && (merged.MinBaseline == "" || releaseTagLess(merged.MinBaseline, driver.MinBaseline)) {
		merged.MinBaseline, merged.Note = driver.MinBaseline, driver.Note
	}
	merged.SkipBaselines = append(append([]string{}, global.SkipBaselines...), driver.SkipBaselines...)
	return merged
}

func releaseTagLess(a, b string) bool {
	av, aok := parseReleaseTag(a)
	bv, bok := parseReleaseTag(b)
	return aok && bok && compareRelease(av, bv) < 0
}

// resolveTypeRules maps type-keyed rules onto the fixture's declared column types, merging
// multiple matches per column into one CompatColumnRule. A data_types rule matching no declared
// column is an error: the fixture does not carry the type, so the rule would assert nothing.
func resolveTypeRules(rules []compatTypeRule, columnTypes map[string][]string) ([]CompatColumnRule, error) {
	merged := map[string]*CompatColumnRule{}
	var order []string
	apply := func(column string, r compatTypeRule) error {
		rule, ok := merged[column]
		if !ok {
			rule = &CompatColumnRule{Column: column}
			merged[column] = rule
			order = append(order, column)
		}
		if r.ExcludeBelow != "" {
			if rule.ExcludeBelow != "" && rule.ExcludeBelow != r.ExcludeBelow {
				return fmt.Errorf("column %s: conflicting exclude_below %s and %s", column, rule.ExcludeBelow, r.ExcludeBelow)
			}
			rule.ExcludeBelow = r.ExcludeBelow
		}
		if r.AssertValueFrom != "" {
			if rule.AssertValueFrom != "" && rule.AssertValueFrom != r.AssertValueFrom {
				return fmt.Errorf("column %s: conflicting assert_value_from %s and %s", column, rule.AssertValueFrom, r.AssertValueFrom)
			}
			rule.AssertValueFrom = r.AssertValueFrom
		}
		if r.TypeOnly {
			rule.TypeOnly = true
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
			if err := apply(r.Column, r); err != nil {
				return nil, err
			}
		case len(r.DataTypes) > 0:
			found := false
			for _, column := range columns {
				matches := slices.ContainsFunc(r.DataTypes, func(dt string) bool {
					return slices.Contains(columnTypes[column], dt)
				})
				if !matches {
					continue
				}
				if err := apply(column, r); err != nil {
					return nil, err
				}
				found = true
			}
			if !found {
				return nil, fmt.Errorf("no declared column matches data_types %v (%s); tag the column in CompatColumnTypes or drop the rule", r.DataTypes, r.Note)
			}
		}
	}

	out := make([]CompatColumnRule, 0, len(order))
	for _, column := range order {
		out = append(out, *merged[column])
	}
	return out, nil
}
