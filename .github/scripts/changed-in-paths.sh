#!/bin/bash
set -euo pipefail

# Prints true when the pull request being built changes a file matching one of the glob patterns
# fed one per line on stdin; false otherwise, including for any event that has no PR file list.
match=false
if [ "$GITHUB_EVENT_NAME" = "pull_request" ]; then
  files=$(gh api --paginate \
    "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER/files" \
    --jq '.[].filename')
  while read -r pattern; do
    [ -n "$pattern" ] || continue
    for file in $files; do
      case "$file" in $pattern) match=true; break 2 ;; esac
    done
  done
fi
echo "$match"
