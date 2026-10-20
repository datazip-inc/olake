package compatibility

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"slices"

	"github.com/datazip-inc/olake/tests/testutils/constants"
	"golang.org/x/mod/semver"
)

// compatibility_rules.json is the compatibility suite's whole configuration: "*" holds the baseline
// gate and column rules every driver shares, a driver's key its own. Adding a driver, a gate or a
// rule is an edit to that file alone.

// allDrivers is the compatibility_rules.json key whose gate and rules apply to every driver.
const allDrivers = "*"

// compatibilityGate bounds which baselines a scope runs against. Empty fields mean no bound.
type compatibilityGate struct {
	MinBaseline string                  `json:"min_baseline"`
	Note        string                  `json:"note"`
	Rules       []compatibilityTypeRule `json:"rules"`
}

// compatibilityRuleScope names the columns a rule applies to: exactly one of data_types (a driver's
// match its fixture's declared types, "*"'s the destination's) or columns, by name.
type compatibilityRuleScope struct {
	DataTypes []string `json:"data_types"`
	Columns   []string `json:"columns"`
}

// compatibilityPolicy is what a rule asserts about the columns it selects, at least one of the
// three, and the note saying why.
type compatibilityPolicy struct {
	ExcludeBelow    string `json:"exclude_below"`
	AssertValueFrom string `json:"assert_value_from"`
	TypeOnly        bool   `json:"type_only"`
	Note            string `json:"note"`
}

// compatibilityTypeRule is one selector with its policy; validateRules enforces both counts.
type compatibilityTypeRule struct {
	compatibilityRuleScope
	compatibilityPolicy
}

type compatibilityRulesConfig map[string]compatibilityGate

var (
	//go:embed compatibility_rules.json
	rawCompatibilityRules []byte

	// compatibilityRules is parsed and validated at package init, so a malformed or misspelled rules file
	// fails every suite loudly instead of silently under-enforcing.
	compatibilityRules compatibilityRulesConfig

	knownCompatibilityDrivers = []constants.DriverType{
		constants.MongoDB, constants.Postgres, constants.MySQL, constants.Oracle,
		constants.DB2, constants.S3, constants.Kafka, constants.MSSQL,
	}
)

func init() {
	dec := json.NewDecoder(bytes.NewReader(rawCompatibilityRules))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&compatibilityRules); err != nil {
		log.Fatalf("tests/testutils/compatibility/compatibility_rules.json: %s", err)
	}
	if err := compatibilityRules.validate(); err != nil {
		log.Fatalf("tests/testutils/compatibility/compatibility_rules.json: %s", err)
	}
}

// validate rejects what would otherwise fail open: a driver name nothing matches, and a baseline
// that is not a release tag. Either would silently drop the gate or rule it carries.
func (c compatibilityRulesConfig) validate() error {
	for name, gate := range c {
		if name != allDrivers && !slices.Contains(knownCompatibilityDrivers, constants.DriverType(name)) {
			return fmt.Errorf("unknown driver %q (known: %q and %v)", name, allDrivers, knownCompatibilityDrivers)
		}
		if err := gate.validate(fmt.Sprintf("%q", name)); err != nil {
			return err
		}
	}
	return nil
}

func (g compatibilityGate) validate(scope string) error {
	if g.MinBaseline != "" && !semver.IsValid(g.MinBaseline) {
		return fmt.Errorf("%s: %q is not a release tag", scope, g.MinBaseline)
	}
	return validateRules(scope, g.Rules)
}

func validateRules(scope string, rules []compatibilityTypeRule) error {
	for i, r := range rules {
		if len(r.Columns) == 0 && len(r.DataTypes) == 0 {
			return fmt.Errorf("%s rule %d: selects nothing (needs columns or data_types): %s", scope, i, r.Note)
		}
		if len(r.Columns) > 0 && len(r.DataTypes) > 0 {
			return fmt.Errorf("%s rule %d: sets both columns and data_types; use one", scope, i)
		}
		if r.ExcludeBelow == "" && r.AssertValueFrom == "" && !r.TypeOnly {
			return fmt.Errorf("%s rule %d: asserts nothing (needs exclude_below, assert_value_from or type_only): %s", scope, i, r.Note)
		}
		for _, tag := range []string{r.ExcludeBelow, r.AssertValueFrom} {
			if tag == "" {
				continue
			}
			if !semver.IsValid(tag) {
				return fmt.Errorf("%s rule %d: %q is not a release tag", scope, i, tag)
			}
		}
	}
	return nil
}

// validateThresholds rejects a rule threshold at or below the oldest release with a state version
// update, which could never fire. The floor comes from state-versions.json.
func (c compatibilityRulesConfig) validateThresholds() error {
	floorTag, err := compatibilityGlobalFloor()
	if err != nil {
		return err
	}
	for _, gate := range c {
		for _, rule := range gate.Rules {
			for _, threshold := range []string{rule.ExcludeBelow, rule.AssertValueFrom} {
				if threshold == "" {
					continue
				}
				if semver.Compare(threshold, floorTag) <= 0 {
					return fmt.Errorf("compatibility_rules.json: rule threshold %s is at or below the oldest reachable baseline %s, so it can never fire (%s); drop the rule or record it as a note",
						threshold, floorTag, rule.Note)
				}
			}
		}
	}
	return nil
}

// skipReason says why a baseline is out of this gate's range ("" = it runs). An undatable baseline
// (a commit with no release reachable, an image ref) is never gated.
func (g compatibilityGate) skipReason(spec string) string {
	if !semver.IsValid(spec) {
		return ""
	}
	if g.MinBaseline != "" && semver.Compare(spec, g.MinBaseline) < 0 {
		return fmt.Sprintf("baseline is older than %s", g.MinBaseline)
	}
	return ""
}
