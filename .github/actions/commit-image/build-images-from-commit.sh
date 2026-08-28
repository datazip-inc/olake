#!/usr/bin/env bash
set -euo pipefail

if [ -z "${SHA:-}" ]; then
  echo "::notice::no commit to prepare images for; skipping"
  exit 0
fi

TAG=$(git rev-parse --short "$SHA")

# The pull pass first: it is what decides whether the jar below has to be built at all, and a hit
# costs seconds against the minutes a build does.
missing=()
for driver in $DRIVERS; do
  # TEMPORARY: the restore is commented out so every image is built from scratch, to time the worst
  # case. Restore before merging, together with the lookup in test-preflight.yml.
  # if docker pull -q "$CACHE_REPO/source-$driver:$TAG"; then
  #   docker tag "$CACHE_REPO/source-$driver:$TAG" "olakego/source-$driver:$TAG"
  #   echo "restored olakego/source-$driver:$TAG from the cache"
  #   continue
  # fi
  echo "no cached image for $driver at $TAG; it will be built"
  missing+=("$driver")
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
build_one() {
  docker buildx build --progress=plain --cache-from type=gha,scope=olake-base \
    --load --build-arg DRIVER_NAME="$1" -t "olakego/source-$1:$TAG" "$SRC"

  # Non-fatal: a fork's token is read-only, and failing to publish only costs the next run the
  # build this one just did.
  docker tag "olakego/source-$1:$TAG" "$CACHE_REPO/source-$1:$TAG"
  docker push "$CACHE_REPO/source-$1:$TAG" || echo "::notice::could not publish source-$1:$TAG (read-only token?)"
}
export -f build_one
export TAG SRC CACHE_REPO

# xargs -P fans these out with no pids to track and no marker files: if any build fails it still
# finishes the rest and exits 123, which set -e turns into this script's failure. -e on the child
# shell too, so a failed build stops before it tags and pushes what it did not produce. Bounded,
# because each one is a full Go compile and the caller has its own builds running beside it.
echo "building ${missing[*]}"
printf '%s\n' "${missing[@]}" | xargs -P "${MAX_PARALLEL:-3}" -I{} bash -euc 'build_one {}'
