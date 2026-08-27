#!/bin/bash
set -euo pipefail

tested=$(make -s print.source-drivers)
known=$(make -s print.drivers)

if [ "$GITHUB_EVENT_NAME" != "pull_request" ] || [ "$GITHUB_BASE_REF" = master ]; then
  selected="$tested"
else
  changed=$(gh api --paginate \
    "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER/files" \
    --jq '.[].filename')
  selected=""
  for file in $changed; do
    case "$file" in
      drivers/*/*|tests/*/*) d=${file#*/}; d=${d%%/*} ;;
      *) d="" ;;
    esac
    case " $tested " in *" $d "*) selected="$selected $d"; continue ;; esac
    case " $known " in *" $d "*) continue ;; esac
    selected="$tested"
    break
  done
fi

drivers=$(printf '%s\n' $selected | sort -u | jq -Rc '[., inputs] | map(select(. != ""))')
echo "drivers=$drivers" >> "$GITHUB_OUTPUT"
echo "Affected drivers: $drivers"

labels=$(printf '%s' "$drivers" | jq -c 'map({key: ., value: ((.[0:1] | ascii_upcase) + .[1:])}) | from_entries')
echo "driver-labels=$labels" >> "$GITHUB_OUTPUT"
