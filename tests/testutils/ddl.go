package testutils

import (
	"regexp"
	"strings"
)

var ddlCharset = regexp.MustCompile(`(?i)CHARACTER SET (\w+)`)

func DataTypeTags(datatype string) []string {
	fields := strings.Fields(datatype)
	if len(fields) == 0 {
		return nil
	}
	typ, _, _ := strings.Cut(strings.ToLower(fields[0]), "(")
	tags := []string{typ}
	if strings.Contains(strings.ToUpper(datatype), " UNSIGNED") {
		tags = append(tags, "unsigned "+typ)
	}
	if m := ddlCharset.FindStringSubmatch(datatype); m != nil {
		tags = append(tags, strings.ToLower(m[1]))
	}
	return tags
}
