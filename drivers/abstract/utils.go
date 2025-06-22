package abstract

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/utils/logger"
)

// Condition represents a single condition in a filter
type Condition struct {
	Column   string
	Operator string
	Value    string
}

// Filter represents the parsed filter
type Filter struct {
	Condition1      Condition
	Condition2      *Condition // nil if only one condition
	LogicalOperator string
}

func RetryOnBackoff(attempts int, sleep time.Duration, f func() error) (err error) {
	for cur := 0; cur < attempts; cur++ {
		if err = f(); err == nil {
			return nil
		}
		if strings.Contains(err.Error(), destination.DestError) {
			break // if destination error, break the retry loop
		}
		if attempts > 1 && cur != attempts-1 {
			logger.Infof("retry attempt[%d], retrying after %.2f seconds due to err: %s", cur+1, sleep.Seconds(), err)
			time.Sleep(sleep)
			sleep = sleep * 2
		}
	}

	return err
}

var FilterRegex = regexp.MustCompile(`^(\w+)\s*(>|<|>=|<=|=|!=)\s*(\"[^\"]*\"|\S+)\s*(?:(and|or)\s*(\w+)\s*(>|<|>=|<=|=|!=)\s*(\"[^\"]*\"|\S+))?\s*$`)

// ParseFilter parses the filter string into a Filter struct
func ParseFilter(filter string) (Filter, error) {
	if filter == "" {
		return Filter{}, nil
	}
	matches := FilterRegex.FindStringSubmatch(filter)
	if matches == nil {
		return Filter{}, fmt.Errorf("invalid filter format: %s", filter)
	}
	condition1 := Condition{
		Column:   matches[1],
		Operator: matches[2],
		Value:    matches[3],
	}
	if matches[4] == "" {
		return Filter{Condition1: condition1}, nil
	}
	condition2 := Condition{
		Column:   matches[5],
		Operator: matches[6],
		Value:    matches[7],
	}
	return Filter{
		Condition1:      condition1,
		Condition2:      &condition2,
		LogicalOperator: matches[4],
	}, nil
}
