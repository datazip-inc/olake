#!/usr/bin/env bash
set -euo pipefail

if [ -z "${SHA:-}" ]; then
  echo "::notice::no commit to prepare images for; skipping"
  exit 0
fi

TAG=$(git rev-parse --short "$SHA")

# The pull pass first: it is what decides whether the jar below has to be built at all, and a hit
# costs seconds against the minutes a build does. All at once -- these are network bound, and eight
# of them one after another is minutes spent waiting on nothing. No -e on the child shell: a pull
# that misses is the expected case here, not a failure.
pull_one() {
  if docker pull -q "$CACHE_REPO/source-$1:$TAG" >/dev/null 2>&1; then
    docker tag "$CACHE_REPO/source-$1:$TAG" "olakego/source-$1:$TAG"
    echo "restored olakego/source-$1:$TAG from the cache"
  fi
}
export -f pull_one
export TAG CACHE_REPO
printf '%s\n' $DRIVERS | xargs -P 0 -I{} bash -c 'pull_one {}'

# Presence decides what is missing, rather than each pull reporting back: the pulls ran in their own
# shells, and an image either landed locally or it did not.
missing=()
for driver in $DRIVERS; do
  if ! docker image inspect "olakego/source-$driver:$TAG" >/dev/null 2>&1; then
    echo "no cached image for $driver at $TAG; it will be built"
    missing+=("$driver")
  fi
done

if [ ${#missing[@]} -eq 0 ]; then
  echo "every image for $TAG came from $CACHE_REPO"
  exit 0
fi

# That tree's own Iceberg writer jar, once for all of them: the Dockerfile copies it out of the
# build context, the commit's Go side speaks that jar's RPC, and parallel builds would otherwise
# race maven on one output directory. Kept off stdout unless it fails -- several hundred lines of
# maven nobody reads when it works.
echo "building the iceberg jar for $TAG..."
started=$SECONDS
if ! jar_log=$(make -C "$SRC" iceberg.jar 2>&1); then
  echo "$jar_log"
  echo "::error::failed to build the iceberg jar at $TAG"
  exit 1
fi
echo "built the iceberg jar in $((SECONDS - started))s"

# TODO: we can use make command for build once this PR merges as local builds gets tagged as olake/source... instead of olakego/source...
#
# Output is collected and printed whole, in a group per driver: several of these run at once, and
# interleaved --progress=plain streams leave you unable to tell how many images actually built.
build_one() {
  local log="$WORK/$1.log"
  if ! docker buildx build --progress=plain --cache-from type=gha,scope=olake-base \
      --load --build-arg DRIVER_NAME="$1" -t "olakego/source-$1:$TAG" "$SRC" > "$log" 2>&1; then
    echo "::group::build $1 -- FAILED"
    cat "$log"
    echo "::endgroup::"
    return 1
  fi

  # Non-fatal: a fork's token is read-only, and failing to publish only costs the next run the
  # build this one just did.
  docker tag "olakego/source-$1:$TAG" "$CACHE_REPO/source-$1:$TAG"
  docker push "$CACHE_REPO/source-$1:$TAG" >> "$log" 2>&1 \
    || echo "::notice::could not publish source-$1:$TAG (read-only token?)"

  echo "::group::build $1 -- built and published"
  cat "$log"
  echo "::endgroup::"
}
export -f build_one
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
export TAG SRC CACHE_REPO WORK

# xargs -P fans these out with no pids to track and no marker files: if any build fails it still
# finishes the rest and exits 123, which set -e turns into this script's failure. -e on the child
# shell too, so a failed build stops before it tags and pushes what it did not produce. Bounded,
# because each one is a full Go compile and the caller has its own builds running beside it.
echo "building ${missing[*]}"
printf '%s\n' "${missing[@]}" | xargs -P "${MAX_PARALLEL:-3}" -I{} bash -euc 'build_one {}'
