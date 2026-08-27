package compatibility

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// compatibility_rules.json is the compatibility suite's whole configuration: per-driver and per-group
// baseline gates, and the column rules that decide what each column can be asserted on. Adding a
// driver, a gate or a rule is an edit to that file alone. What stays in code is only what binds a
// name to behavior: the group list in compatibilityGroupSpecs, since each variant calls a harness func.

// compatibilityGate bounds which baselines a scope runs against. Empty fields mean no bound.
type compatibilityGate struct {
	MinBaseline   string   `json:"min_baseline"`
	SkipBaselines []string `json:"skip_baselines"`
	Note          string   `json:"note"`
}

// compatibilityTypeRule selects columns by data-type tag (resolved against the declared schema plus
// the json column_types maps) or by an
// olake-owned column name, and carries the same policy fields as ColumnRule.
type compatibilityTypeRule struct {
	DataTypes       []string `json:"data_types"`
	Column          string   `json:"column"`
	ExcludeBelow    string   `json:"exclude_below"`
	AssertValueFrom string   `json:"assert_value_from"`
	TypeOnly        bool     `json:"type_only"`
	Note            string   `json:"note"`
}

// compatibilityDestination is one destination's gates. A destination may gate itself (parquet) and/or
// carry named modes (iceberg's arrow and legacy); both shapes decode into this one type, so adding
// a destination or a mode is a config edit.
type compatibilityDestination struct {
	compatibilityGate
	Modes map[string]compatibilityGate
}

var compatibilityGateFields = map[string]bool{"min_baseline": true, "skip_baselines": true, "note": true}

func (d *compatibilityDestination) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	gate := map[string]json.RawMessage{}
	d.Modes = map[string]compatibilityGate{}
	for key, value := range raw {
		if compatibilityGateFields[key] {
			gate[key] = value
			continue
		}
		var mode compatibilityGate
		if err := strictUnmarshal(value, &mode); err != nil {
			return fmt.Errorf("mode %q: %w", key, err)
		}
		d.Modes[key] = mode
	}
	if len(gate) == 0 {
		return nil
	}
	encoded, err := json.Marshal(gate)
	if err != nil {
		return err
	}
	return strictUnmarshal(encoded, &d.compatibilityGate)
}

// strictUnmarshal rejects unknown keys. A custom UnmarshalJSON does not inherit the outer
// decoder's strictness, so nested gates re-apply it here; a typo must never fail open.
func strictUnmarshal(data []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

// compatibilityVariantRules gates and rules for one source data format (s3's csv/json/parquet).
type compatibilityVariantRules struct {
	compatibilityGate
	Rules []compatibilityTypeRule `json:"rules"`
	// ColumnTypes tags fixture columns with type identifiers the declared destination schema
	// cannot express (a charset, a parquet physical type); data_types rules select on both.
	ColumnTypes map[string][]string `json:"column_types"`
}

type compatibilityDriverRules struct {
	compatibilityGate
	Destinations map[string]compatibilityDestination `json:"destinations"`
	Rules        []compatibilityTypeRule             `json:"rules"`
	// DestinationRules covers the columns this driver makes APPEAR in the destination (its
	// materialized key, olake's metadata) as opposed to the source columns its fixture seeds.
	DestinationRules []compatibilityTypeRule              `json:"destination_rules"`
	Variants         map[string]compatibilityVariantRules `json:"variants"`
	ColumnTypes      map[string][]string                  `json:"column_types"`
}

// compatibilityDestinationsRules is the destinations block: the rules for the columns every
// destination writer emits (olake's own metadata), and each destination's gates.
type compatibilityDestinationsRules struct {
	Rules         []compatibilityTypeRule `json:"rules"`
	ValueCompared struct {
		Columns []string `json:"columns"`
		Note    string   `json:"note"`
	} `json:"value_compared"`
	Iceberg compatibilityDestination `json:"iceberg"`
	Parquet compatibilityDestination `json:"parquet"`
}

// gates keys the destinations the way the writer groups look them up.
func (r compatibilityDestinationsRules) gates() map[string]compatibilityDestination {
	return map[string]compatibilityDestination{"iceberg": r.Iceberg, "parquet": r.Parquet}
}

type compatibilityRulesConfig struct {
	Drivers      map[string]compatibilityDriverRules `json:"drivers"`
	Destinations compatibilityDestinationsRules      `json:"destinations"`
}

//go:embed compatibility_rules.json
var rawCompatibilityRules []byte

// compatibilityRules is parsed and validated at package init, so a malformed or misspelled rules file
// fails every suite loudly instead of silently under-enforcing.
var compatibilityRules = func() compatibilityRulesConfig {
	var cfg compatibilityRulesConfig
	dec := json.NewDecoder(bytes.NewReader(rawCompatibilityRules))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		panic("tests/testutils/compatibility_rules.json: " + err.Error())
	}
	if err := cfg.validate(); err != nil {
		panic("tests/testutils/compatibility_rules.json: " + err.Error())
	}
	return cfg
}()

var knownCompatibilityDrivers = []constants.DriverType{
	constants.MongoDB, constants.Postgres, constants.MySQL, constants.Oracle,
	constants.DB2, constants.S3, constants.Kafka, constants.MSSQL,
}

// validate rejects what would otherwise fail open: a driver or group name nothing matches, and a
// baseline that is not a release tag. Either would silently drop the gate or rule it carries.
func (c compatibilityRulesConfig) validate() error {
	modes := map[string][]string{}
	for _, spec := range compatibilityGroupSpecs() {
		modes[spec.destination] = append(modes[spec.destination], spec.mode)
	}
	checkDestinations := func(scope string, destinations map[string]compatibilityDestination) error {
		for name, dest := range destinations {
			known, ok := modes[name]
			if !ok {
				return fmt.Errorf("%s: unknown destination %q (known: %v)", scope, name, slices.Sorted(maps.Keys(modes)))
			}
			if err := dest.compatibilityGate.validate(scope + " destination " + name); err != nil {
				return err
			}
			for mode, gate := range dest.Modes {
				if !slices.Contains(known, mode) {
					return fmt.Errorf("%s destination %s: unknown mode %q (known: %v)", scope, name, mode, known)
				}
				if err := gate.validate(fmt.Sprintf("%s destination %s mode %s", scope, name, mode)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := checkDestinations("destinations", c.Destinations.gates()); err != nil {
		return err
	}
	if err := validateRules("destinations", c.Destinations.Rules); err != nil {
		return err
	}
	for _, column := range c.Destinations.ValueCompared.Columns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("destinations.value_compared carries an empty column name")
		}
	}
	for name, driver := range c.Drivers {
		if !slices.Contains(knownCompatibilityDrivers, constants.DriverType(name)) {
			return fmt.Errorf("unknown driver %q (known: %v)", name, knownCompatibilityDrivers)
		}
		if err := driver.validate(name); err != nil {
			return err
		}
		if err := checkDestinations("driver "+name, driver.Destinations); err != nil {
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
		if err := validateRules("driver "+name+" destination_rules", driver.DestinationRules); err != nil {
			return err
		}
	}
	return nil
}

func (g compatibilityGate) validate(scope string) error {
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

func validateRules(scope string, rules []compatibilityTypeRule) error {
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
func (g compatibilityGate) skipReason(version [3]int, dated bool) string {
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
func mergedGate(global, driver compatibilityGate) compatibilityGate {
	merged := compatibilityGate{MinBaseline: global.MinBaseline, Note: global.Note}
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
// multiple matches per column into one ColumnRule. A data_types rule matching no declared
// column is an error: the fixture does not carry the type, so the rule would assert nothing.
func resolveTypeRules(rules []compatibilityTypeRule, columnTypes map[string][]string) ([]ColumnRule, []string, error) {
	alwaysTypeOnly := map[string]bool{}
	merged := map[string]*ColumnRule{}
	var order []string
	apply := func(column string, r compatibilityTypeRule) error {
		if r.TypeOnly {
			alwaysTypeOnly[column] = true
		}
		// Only a dated policy becomes a ColumnRule; type_only alone is carried by alwaysTypeOnly.
		if r.ExcludeBelow == "" && r.AssertValueFrom == "" {
			return nil
		}
		rule, ok := merged[column]
		if !ok {
			rule = &ColumnRule{Column: column}
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
				return nil, nil, err
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
					return nil, nil, err
				}
				found = true
			}
			if !found {
				return nil, nil, fmt.Errorf("no declared column matches data_types %v (%s); tag the column in the fixture's ColumnTypes or drop the rule", r.DataTypes, r.Note)
			}
		}
	}

	out := make([]ColumnRule, 0, len(order))
	for _, column := range order {
		out = append(out, *merged[column])
	}
	return out, slices.Sorted(maps.Keys(alwaysTypeOnly)), nil
}

// parseReleaseTag reads "vX.Y.Z" (optionally behind a "repo:tag" prefix) into a comparable triple;
// ok is false for anything that is not a release tag (a sha, "latest", a bare image).
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
