package abstract

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

// Prerequisite is one CDC setup check. Check returns the server's current value and whether it
// is acceptable; an error means the value could not be read.
type Prerequisite struct {
	Name        string
	Required    bool
	Recommended string
	Description string
	Check       func(ctx context.Context) (current string, ok bool, err error)
}

// PrerequisiteReporter is implemented by drivers that run prerequisite checks during Setup.
// Setup records the results and fails on unmet required checks only when the config
// explicitly selects CDC.
type PrerequisiteReporter interface {
	Prerequisites() types.PrerequisiteResults
}

// RunPrerequisites evaluates every check (no early exit) and orders the results
// required failed → optional failed → passed.
func RunPrerequisites(ctx context.Context, checks []Prerequisite) types.PrerequisiteResults {
	results := make(types.PrerequisiteResults, 0, len(checks))
	for _, c := range checks {
		current, ok, err := c.Check(ctx)
		if err != nil {
			logger.Warnf("prerequisite %s could not be evaluated: %s", c.Name, err)
			current, ok = "unavailable", false
		}
		if !ok {
			logger.Warnf("prerequisite %s not met (required=%t): current=%s recommended=%s",
				c.Name, c.Required, current, c.Recommended)
		}
		results = append(results, types.PrerequisiteResult{
			Name:             c.Name,
			Required:         c.Required,
			Passed:           ok,
			CurrentValue:     current,
			RecommendedValue: c.Recommended,
			Err:              err,
			Description:      c.Description,
		})
	}

	rank := func(c types.PrerequisiteResult) int {
		switch {
		case c.Passed:
			return 2
		case c.Required:
			return 0
		default:
			return 1
		}
	}
	slices.SortStableFunc(results, func(a, b types.PrerequisiteResult) int { return rank(a) - rank(b) })
	return results
}

// RequireCDCPrerequisites returns an error when a required check did not pass. A driver calls it
// from Setup when the config explicitly selects CDC, so that test connection fails instead of
// reporting a source that cannot sync. A check that ran and failed is a CDC precondition failure;
// when every failure is a check that could not be evaluated, the causes are kept so the failure
// is classified by them (network, permission, timeout) rather than as a misconfiguration.
func RequireCDCPrerequisites(driverType string, prerequisites types.PrerequisiteResults) error {
	var names []string
	var causes []error
	misconfigured := false
	for _, c := range prerequisites {
		if !c.Required || c.Passed {
			continue
		}
		if c.Err == nil {
			misconfigured = true
			names = append(names, c.Name)
			continue
		}
		names = append(names, fmt.Sprintf("%s (could not evaluate: %s)", c.Name, c.Err))
		// errs.From only reads classified errors, so a raw cause would lose to an outer precondition
		causes = append(causes, errs.Classify(c.Err))
	}
	if len(names) == 0 {
		return nil
	}
	if misconfigured {
		// a definite misconfiguration is the actionable failure, even if another check also errored
		return errs.Precondition(errs.CDCPreconditionFailed,
			fmt.Sprintf("%s.cdc_prerequisites_failed", driverType),
			fmt.Errorf("required CDC prerequisites not met: %s", strings.Join(names, ", ")))
	}
	return fmt.Errorf("required CDC prerequisites could not be evaluated: %s: %w",
		strings.Join(names, ", "), errors.Join(causes...))
}

// Prerequisites returns the checks recorded by the driver's Setup, if it runs any.
func (a *AbstractDriver) Prerequisites() types.PrerequisiteResults {
	if r, ok := a.driver.(PrerequisiteReporter); ok {
		return r.Prerequisites()
	}
	return nil
}
