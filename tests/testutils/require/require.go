// Package require is the harness's drop-in for testify's require: same names, same arguments,
// same semantics, with every failure report re-emitted red and attributed to the caller, and two
// maps that differ summarized as a table of the keys that do.
package require

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
	trequire "github.com/stretchr/testify/require"
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
func run(t *testing.T, check func(c *failT), summaries ...func() string) {
	t.Helper()
	c := capture(check, summaries...)
	for _, message := range c.messages {
		t.Errorf("%s", testutils.Red(message))
	}
	if c.failed {
		t.FailNow()
	}
}

// capture runs check against a recording T. If it reported anything, each summary is rendered
// and appended to the report; an empty one is dropped, so a summary that does not apply to the
// operands costs nothing.
func capture(check func(c *failT), summaries ...func() string) *failT {
	c := &failT{}
	check(c)
	if len(c.messages) == 0 {
		return c
	}
	for _, summary := range summaries {
		if s := summary(); s != "" {
			c.messages = append(c.messages, s)
		}
	}
	return c
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
	run(t, func(c *failT) { trequire.Equal(c, expected, actual, msgAndArgs...) },
		func() string { return MapDiff("key", "expected", "actual", expected, actual) })
}

func Equalf(t *testing.T, expected, actual any, msg string, msgAndArgs ...any) {
	t.Helper()
	run(t, func(c *failT) { trequire.Equalf(c, expected, actual, msg, msgAndArgs...) },
		func() string { return MapDiff("key", "expected", "actual", expected, actual) })
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

// MapDiff renders the keys on which two maps disagree as a table -- one row per key, the two
// sides' values beside each other under the labels given -- so a reader sees the three columns
// that changed type without scanning two sixty-column dumps for them.
func MapDiff(noun, leftLabel, rightLabel string, left, right any) string {
	l, r := concrete(reflect.ValueOf(left)), concrete(reflect.ValueOf(right))
	if !comparableMaps(l, r) {
		return ""
	}
	rows := mapRows("", l, r)
	if len(rows) == 0 {
		return ""
	}
	verb := "differ"
	if len(rows) == 1 {
		verb = "differs"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s:\n", plural(len(rows), noun), verb)
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  %s\t%s\t%s\n", noun, leftLabel, rightLabel)
	for _, r := range rows {
		fmt.Fprintf(w, "  %s\t%s\t%s\n", r.path, r.left, r.right)
	}
	_ = w.Flush()
	return strings.TrimRight(b.String(), "\n")
}

type diffRow struct{ path, left, right string }

const absent = "(absent)"

// mapRows lists the keys on which two maps disagree, descending into values that are both maps
// so a difference deep in a nested document is reported by its path, not as two dumps of the
// subtree that contains it.
func mapRows(prefix string, l, r reflect.Value) []diffRow {
	var rows []diffRow
	for _, k := range sortedMapKeys(l) {
		path := prefix + fmt.Sprint(k)
		lv, rv := l.MapIndex(k), r.MapIndex(k)
		switch {
		case !rv.IsValid():
			rows = append(rows, diffRow{path, fmt.Sprint(lv), absent})
		case comparableMaps(concrete(lv), concrete(rv)):
			rows = append(rows, mapRows(path+".", concrete(lv), concrete(rv))...)
		case !reflect.DeepEqual(lv.Interface(), rv.Interface()):
			rows = append(rows, diffRow{path, fmt.Sprint(lv), fmt.Sprint(rv)})
		}
	}
	for _, k := range sortedMapKeys(r) {
		if !l.MapIndex(k).IsValid() {
			rows = append(rows, diffRow{prefix + fmt.Sprint(k), absent, fmt.Sprint(r.MapIndex(k))})
		}
	}
	return rows
}

// comparableMaps reports whether both values are maps sharing a key type, so either's keys can
// index the other.
func comparableMaps(l, r reflect.Value) bool {
	return l.Kind() == reflect.Map && r.Kind() == reflect.Map && l.Type().Key() == r.Type().Key()
}

// concrete unwraps interfaces and pointers, which is how a decoded JSON document holds its nested
// objects, down to the value they carry. A nil stays as it is.
func concrete(v reflect.Value) reflect.Value {
	for (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) && !v.IsNil() {
		v = v.Elem()
	}
	return v
}

// sortedMapKeys orders a map's keys by their printed form, the order a reader scans a column
// list in.
func sortedMapKeys(m reflect.Value) []reflect.Value {
	keys := m.MapKeys()
	slices.SortFunc(keys, func(a, b reflect.Value) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	return keys
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
