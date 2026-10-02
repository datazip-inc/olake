package abstract

import (
	"context"
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
// Setup records the results and never fails because of them; Read enforces required checks.
type PrerequisiteReporter interface {
	Prerequisites() types.Prerequisites
}

// RunPrerequisites evaluates every check (no early exit) and orders the results
// required failed → optional failed → passed.
func RunPrerequisites(ctx context.Context, checks []Prerequisite) types.Prerequisites {
	results := make(types.Prerequisites, 0, len(checks))
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
		results = append(results, types.PrerequisiteCheck{
			Name:             c.Name,
			Required:         c.Required,
			Passed:           ok,
			CurrentValue:     current,
			RecommendedValue: c.Recommended,
			Description:      c.Description,
		})
	}

	rank := func(c types.PrerequisiteCheck) int {
		switch {
		case c.Passed:
			return 2
		case c.Required:
			return 0
		default:
			return 1
		}
	}
	slices.SortStableFunc(results, func(a, b types.PrerequisiteCheck) int { return rank(a) - rank(b) })
	return results
}

// ValidateCDCPrerequisites fails when CDC streams are selected and a required check did not pass.
// Full-refresh and incremental-only syncs are never blocked.
func (a *AbstractDriver) ValidateCDCPrerequisites(cdcStreams []types.StreamInterface) error {
	if len(cdcStreams) == 0 {
		return nil
	}
	if failed := a.Prerequisites().FailedRequired(); len(failed) > 0 {
		return errs.Precondition(errs.CDCPreconditionFailed,
			fmt.Sprintf("%s.cdc_prerequisites_failed", a.driver.Type()),
			fmt.Errorf("required CDC prerequisites not met: %s", strings.Join(failed, ", ")))
	}
	return nil
}

// Prerequisites returns the checks recorded by the driver's Setup, if it runs any.
func (a *AbstractDriver) Prerequisites() types.Prerequisites {
	if r, ok := a.driver.(PrerequisiteReporter); ok {
		return r.Prerequisites()
	}
	return nil
}
