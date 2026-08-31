#!/usr/bin/env bash
set -euo pipefail

TAG=${TAG:-local}
ARTIFACT_PREFIX=${ARTIFACT_PREFIX:-driver-image}

restore_single_image() {
  local dir="$RUNNER_TEMP/images/$1"
  gh run download "$GITHUB_RUN_ID" -n "$ARTIFACT_PREFIX-$1" -D "$dir" >/dev/null 2>&1 || return 0
  docker load -i "$dir/source-$1.tar"
}
export -f restore_single_image
export GH_TOKEN GITHUB_RUN_ID RUNNER_TEMP ARTIFACT_PREFIX
printf '%s\n' $DRIVERS | xargs -P "${MAX_PARALLEL:-4}" -I{} bash -c 'restore_single_image {}'

missing=()
for driver in $DRIVERS; do
  docker image inspect "olakego/source-$driver:$TAG" >/dev/null 2>&1 || missing+=("$driver")
done

if [ ${#missing[@]} -eq 0 ]; then
  echo "every driver image came from this run's integration build"
  exit 0
fi

echo "::notice::no artifact for ${missing[*]}; building them here"
make -j --output-sync=target docker.all.build DRIVERS="${missing[*]}" IMAGE_TAG="$TAG" \
  DOCKER_BUILD="docker buildx build --progress=plain --cache-from type=gha,scope=olake-base --load"
