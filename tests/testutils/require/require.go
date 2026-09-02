// Package require is the harness's drop-in for testify's require: same names, same arguments,
// same semantics, with every failure report re-emitted red and bold and attributed to the caller.
package require

import (
	"fmt"
	"strings"
	"testing"
	"time"

	trequire "github.com/stretchr/testify/require"
)

const (
	red   = "\033[31m"
	reset = "\033[0m"
)

// failT collects what testify would have reported so run can replay it on the real testing.T --
// through the caller's frame, which is what keeps the file:line prefix on the test that failed.
type failT struct {
	messages []string
	failed   bool
}

func (c *failT) Errorf(format string, args ...any) {
	c.messages = append(c.messages, fmt.Sprintf(format, args...))
}

func (c *failT) FailNow() {
	c.failed = true
}

// run replays a captured failure on t: the report in color, then the FailNow testify requested.
func run(t *testing.T, check func(c *failT)) {
	t.Helper()
	c := &failT{}
	check(c)
	for _, message := range c.messages {
		t.Errorf("%s", colorize(message))
	}
	if c.failed {
		t.FailNow()
	}
}

func colorize(message string) string {
	lines := strings.Split(message, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = red + line + reset
		}
	}
	return strings.Join(lines, "\n")
}

func Contains(t *testing.T, s, contains any, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Contains(c, s, contains, msgAndArgs...) })
}

func Containsf(t *testing.T, s, contains any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Containsf(c, s, contains, msg, msgAndArgs...) })
}

func Empty(t *testing.T, object any, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Empty(c, object, msgAndArgs...) })
}

func Equal(t *testing.T, expected, actual any, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Equal(c, expected, actual, msgAndArgs...) })
}

func Equalf(t *testing.T, expected, actual any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Equalf(c, expected, actual, msg, msgAndArgs...) })
}

func Falsef(t *testing.T, value bool, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Falsef(c, value, msg, msgAndArgs...) })
}

func GreaterOrEqualf(t *testing.T, e1, e2 any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.GreaterOrEqualf(c, e1, e2, msg, msgAndArgs...) })
}

func Greaterf(t *testing.T, e1, e2 any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Greaterf(c, e1, e2, msg, msgAndArgs...) })
}

func Len(t *testing.T, object any, length int, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Len(c, object, length, msgAndArgs...) })
}

func LessOrEqualf(t *testing.T, e1, e2 any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.LessOrEqualf(c, e1, e2, msg, msgAndArgs...) })
}

func NoError(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.NoError(c, err, msgAndArgs...) })
}

func NoErrorf(t *testing.T, err error, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.NoErrorf(c, err, msg, msgAndArgs...) })
}

func NotContainsf(t *testing.T, s, contains any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.NotContainsf(c, s, contains, msg, msgAndArgs...) })
}

func NotEmpty(t *testing.T, object any, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.NotEmpty(c, object, msgAndArgs...) })
}

func NotEqualf(t *testing.T, expected, actual any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.NotEqualf(c, expected, actual, msg, msgAndArgs...) })
}

func NotNil(t *testing.T, object any, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.NotNil(c, object, msgAndArgs...) })
}

func True(t *testing.T, value bool, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.True(c, value, msgAndArgs...) })
}

func Truef(t *testing.T, value bool, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Truef(c, value, msg, msgAndArgs...) })
}

func Zerof(t *testing.T, i any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Zerof(c, i, msg, msgAndArgs...) })
}

func Eventually(t *testing.T, condition func() bool, waitFor, tick time.Duration, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Eventually(c, condition, waitFor, tick, msgAndArgs...) })
}
