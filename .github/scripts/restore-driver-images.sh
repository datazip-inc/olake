#!/usr/bin/env bash
# Loads this run's driver images, which the integration matrix exported as one artifact per driver,
# and builds whatever did not arrive -- a fork PR publishes nothing, and the job still has to stand
# on its own. Reads DRIVERS, plus GH_TOKEN for the download.
set -euo pipefail

restore_single_image() {
  local dir="$RUNNER_TEMP/images/$1"
  gh run download "$GITHUB_RUN_ID" -n "driver-image-$1" -D "$dir" >/dev/null 2>&1 || return 0
  docker load -i "$dir/source-$1.tar"
}
export -f restore_single_image
export GH_TOKEN GITHUB_RUN_ID RUNNER_TEMP
printf '%s\n' $DRIVERS | xargs -P "${MAX_PARALLEL:-4}" -I{} bash -c 'restore_single_image {}'

# Presence decides what is missing, the way the base branch images do it: the restores ran in their
# own shells, and an image either landed in the daemon or it did not.
missing=()
for driver in $DRIVERS; do
  docker image inspect "olakego/source-$driver:local" >/dev/null 2>&1 || missing+=("$driver")
done

# An if, not `[ ... ] && exit 0`: on the false branch that list returns non-zero and set -e would
# end the script right here, silently skipping the build below.
if [ ${#missing[@]} -eq 0 ]; then
  echo "every driver image came from this run's integration build"
  exit 0
fi

echo "::notice::no artifact for ${missing[*]}; building them here"
make -j --output-sync=target docker.all.build DRIVERS="${missing[*]}" \
  DOCKER_BUILD="docker buildx build --progress=plain --cache-from type=gha,scope=olake-base --load"
