#!/usr/bin/env bash
set -euo pipefail

# Adds everyone outside the team who raised the same GitHub issue to a Reo developer list, so Reo
# can rank their companies regardless of size. DRY_RUN=1 prints the matches and skips Reo.
REPOS=${REPOS:-"olake olake-ui olake-fusion olake-helm olake-docs"}
MIN_PEOPLE=${MIN_PEOPLE:-2}

query='query($owner: String!, $name: String!, $endCursor: String) {
  repository(owner: $owner, name: $name) {
    issues(first: 50, after: $endCursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        url title
        author { login __typename ... on User { company } }
        authorAssociation
        comments(first: 100) { nodes { author { login __typename ... on User { company } } authorAssociation bodyText } }
        reactions(content: THUMBS_UP, first: 100) { nodes { user { login company } } }
        timelineItems(itemTypes: [LABELED_EVENT, ASSIGNED_EVENT, CLOSED_EVENT, MARKED_AS_DUPLICATE_EVENT], first: 50) {
          nodes {
            __typename
            ... on LabeledEvent { actor { login } }
            ... on AssignedEvent { actor { login } }
            ... on ClosedEvent { actor { login } }
            ... on MarkedAsDuplicateEvent { canonical { ... on Issue { url } } }
          }
        }
      }
    }
  }
}'

# People: the author, 👍 reactors and commenters, minus staff and contributors claiming the task.
# shortcut: the first 100 comments and reactions per issue are read, enough for every issue so far.
rule='
def maintainer: . == "OWNER" or . == "MEMBER" or . == "COLLABORATOR";
def claims_work: test("assign|up for grab|work(ing)? on (this|it)|take (this|it)|pick (this|it)|can i (get|take|work|pick|try)|i(.d| would) like to|is (this|it|the issue) (still )?(open|available)|pull request|\\bpr\\b|i(.ve| have) (added|fixed|made|raised|opened|created|submitted|implemented)"; "i");
([.[] | (select(.authorAssociation | maintainer) | .author.login),
        (.comments.nodes[] | select(.authorAssociation | maintainer) | .author.login),
        (.author.login as $a | .timelineItems.nodes[] | select(.__typename != "MarkedAsDuplicateEvent") | .actor.login | select(. != null and . != $a))]
 | unique) as $staff
| def outsider: . != null and .__typename != "Bot" and (.login | IN($staff[]) | not) and ((.login + " " + (.company // "")) | test("datazip"; "i") | not);
[.[] | select(.author | outsider)
 | .url as $url
 | {url, title,
    key: ([.timelineItems.nodes[] | select(.__typename == "MarkedAsDuplicateEvent") | .canonical.url | select(. != null and . != $url)] | first // $url),
    people: ([.author.login]
      + [.comments.nodes[] | select((.author | outsider) and .authorAssociation != "CONTRIBUTOR" and (.bodyText | claims_work | not)) | .author.login]
      + [.reactions.nodes[].user | select(. != null) | . + {__typename: "User"} | select(outsider) | .login])}]
| group_by(.key)
| map({url: .[0].key, title: ((map(select(.url == .key)) | first // .[0]).title), people: (map(.people[]) | unique)})
| map(select(.people | length >= $min))'

# set -e is off inside $(...), so stop on a failed fetch: partial data can count staff as outsiders.
shared=$(for repo in $REPOS; do
  gh api graphql --paginate -f query="$query" -F owner=datazip-inc -F name="$repo" \
    --jq '.data.repository.issues.nodes[]' || exit
done | jq -s --argjson min "$MIN_PEOPLE" "$rule")

jq -r '.[] | "\(.people | length) people  \(.url)  \(.title)"' <<<"$shared"
jq -r '"\(length) shared issues, \([.[].people[]] | unique | length) people"' <<<"$shared"
if [ -n "${DRY_RUN:-}" ]; then
  jq -r '[.[].people[]] | unique | join(" ")' <<<"$shared"
  exit 0
fi

: "${REO_API_KEY:?}" "${REO_USER:?}" "${REO_LIST_ID:?}"
# Reo takes at most 1,000 records per call and appends them to the list.
jq -c '[.[].people[]] | unique | range(0; length; 1000) as $i
  | {type: "DEVELOPER", entities: [.[$i:$i + 1000][] | {primaryKey: ., clientKey: "github", fieldType: "String"}]}' <<<"$shared" |
  while read -r batch; do
    curl -fsS -X PUT "https://ingest.reo.dev/api/product/list/$REO_LIST_ID" \
      -H "X-API-KEY: $REO_API_KEY" -H "user: $REO_USER" -H "Content-Type: application/json" -d "$batch"
    echo
  done
