#!/bin/bash
set -euo pipefail

# Returns true if this merge queue entry has already passed a workflow
# on the PR against the same base commit.
# Returns false otherwise, including for events other than merge_group.
# The PR workflow saves the base commit it tested in an artifact named
# `<prefix>-<pr head sha>`, holding a file named `<prefix>`.
#
# REDUNDANT_WORKFLOW         workflow file to check (default: tests.yml)
# REDUNDANT_ARTIFACT_PREFIX  artifact and file name prefix (default: tested-base)
#
# If another PR is ahead in the merge queue, this entry uses that PR's
# queue commit as its base, so the base commits won't match.

workflow="${REDUNDANT_WORKFLOW:-tests.yml}"
prefix="${REDUNDANT_ARTIFACT_PREFIX:-tested-base}"

# Prints true when CI can be skipped for this merge queue entry, false otherwise.
is_ci_skippable() {
  # Not a merge queue run: never skip.
  if [ "$GITHUB_EVENT_NAME" != "merge_group" ]; then
    echo false
    return
  fi

  # PR number, from a head_ref like gh-readonly-queue/staging/pr-7-<sha>.
  local pr
  pr=$(sed -n 's#.*/pr-\([0-9][0-9]*\)-.*#\1#p' <<<"$MERGE_GROUP_HEAD_REF")
  if [ -z "$pr" ]; then
    echo false
    return
  fi

  # A successful run of the workflow must exist for the PR's head commit.
  local pr_head run_id
  pr_head=$(gh api "repos/$GITHUB_REPOSITORY/pulls/$pr" --jq .head.sha)
  run_id=$(gh run list \
    --repo "$GITHUB_REPOSITORY" \
    --workflow "$workflow" \
    --event pull_request \
    --commit "$pr_head" \
    --status success \
    --limit 1 \
    --json databaseId \
    --jq '.[0].databaseId // empty')
  if [ -z "$run_id" ]; then
    echo false
    return
  fi

  # That run must have left the base commit it tested against.
  local dir
  dir=$(mktemp -d)
  if ! gh run download "$run_id" --repo "$GITHUB_REPOSITORY" -n "$prefix-$pr_head" -D "$dir" >/dev/null 2>&1; then
    echo false
    return
  fi

  # Skippable only when the queue entry is built on that same base commit.
  if [ "$(tr -d '[:space:]' <"$dir/$prefix")" = "$MERGE_GROUP_BASE_SHA" ]; then
    echo true
  else
    echo false
  fi
}

# `|| echo false`: if a command fails unexpectedly (e.g. a GitHub API error), run CI as normal.
is_ci_skippable || echo false
