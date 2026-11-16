#!/usr/bin/env bash
# Unit tests across every module in the go.work workspace, run from the repo root (make test.unit).
# Directory patterns ({{.Dir}}/...), not module-path patterns: in a go.work workspace a path pattern
# like <module>/... prefix-matches into sibling modules.
# Writes a merged cover profile (coverage.out) spanning every workspace module. The per-package
# "coverage: x%" lines are filtered out of the log and replaced by one per-package table printed
# at the end, even when tests fail. The go test exit status is kept so failures still fail the run.
set -uo pipefail

# cover profile (stdin) -> markdown table, one row per package group + Overall.
cover_markdown() {
    awk '
NR == 1 { next }
{ stm[$1] = $2; if ($3 > 0) hit[$1] = 1 }
END {
	for (k in stm) {
		split(k, a, ":"); f = a[1]
		sub("github.com/datazip-inc/olake/", "", f)
		n = split(f, p, "/")
		if (n == 1) g = "(root)"
		else if ((p[1] == "drivers" || p[1] == "destination") && n >= 3) g = p[1] "/" p[2]
		else g = p[1]
		tot[g] += stm[k]; all += stm[k]
		if (k in hit) { cov[g] += stm[k]; allcov += stm[k] }
	}
	print "| Package | Coverage | Statements |"
	print "|---|---|---|"
	for (g in tot) printf "| `%s` | %.1f%% | %d/%d |\n", g, 100 * cov[g] / tot[g], cov[g], tot[g] | "sort"
	close("sort")
	if (all > 0) printf "| **Overall** | **%.1f%%** | **%d/%d** |\n", 100 * allcov / all, allcov, all
}'
}

# markdown table (stdin) -> aligned boxed table for terminals / job logs.
cover_box() {
    awk -F'|' '
NR == 2 { next }
{
	rows = NR
	for (i = 2; i < NF; i++) { gsub(/^ +| +$/, "", $i); c[NR, i] = $i; if (length($i) > w[i]) w[i] = length($i) }
}
END {
	sep = "+"
	for (i = 2; i <= 4; i++) { s = sprintf("%*s", w[i] + 2, ""); gsub(/ /, "-", s); sep = sep s "+" }
	for (r = 1; r <= rows; r++) {
		if (r == 2) continue
		if (r == 1 || r == 3 || r == rows) print sep
		out = "|"
		for (i = 2; i <= 4; i++) out = out sprintf(" %-*s |", w[i], c[r, i])
		print out
	}
	print sep
}'
}

# Left unquoted on purpose: the newline-separated patterns word-split into separate arguments.
packages=$(go list -m -f '{{.Dir}}' | sed "s#^$(pwd)#.#"'; s#$#/...#')
coverpkg=$(echo $packages | tr ' ' ',')

tab=$(printf '\t')
rcfile=$(mktemp)

# Run the tests and stream the log through sed; the go test exit status goes to $rcfile because
# the pipeline's own status is sed's.
{
    go test -v -count=1 \
        -coverpkg="$coverpkg" \
        -coverprofile=coverage.out \
        $packages 2>&1
    echo $? >"$rcfile"
} |
    sed -E \
        -e '/^[[:space:]]*coverage: /d' \
        -e 's#^[[:space:]]+(github\.com/[^[:space:]]+)[[:space:]]+coverage: 0\.0% of statements.*$#?   '"$tab"'\1'"$tab"'[no test files]#' \
        -e 's#[[:space:]]+coverage: [0-9.]+% of statements.*$##'

rc=$(cat "$rcfile")
rm -f "$rcfile"

if [[ -s coverage.out ]]; then
    cover_markdown <coverage.out | tr -d '`*' | cover_box
fi

exit "$rc"
