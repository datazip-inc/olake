package testutils

import (
	"regexp"
	"strings"
)

var ddlCharset = regexp.MustCompile(`(?i)CHARACTER SET (\w+)`)

// DDLColumnTypes reads column type tags off a CREATE TABLE column list -- the base type, its
// unsigned form and charset where the dialect has them -- so a fixture declares nothing by hand.
func DDLColumnTypes(ddl string) map[string][]string {
	types := map[string][]string{}
	for _, line := range strings.Split(ddl, "\n") {
		fields := strings.Fields(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if len(fields) < 2 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "PRIMARY", "KEY", "UNIQUE", "INDEX", "CONSTRAINT":
			continue
		}
		typ, _, _ := strings.Cut(strings.ToLower(fields[1]), "(")
		tags := []string{typ}
		if strings.Contains(strings.ToUpper(line), " UNSIGNED") {
			tags = append(tags, "unsigned "+typ)
		}
		if m := ddlCharset.FindStringSubmatch(line); m != nil {
			tags = append(tags, strings.ToLower(m[1]))
		}
		types[fields[0]] = tags
	}
	return types
}
