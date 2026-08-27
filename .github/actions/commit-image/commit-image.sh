#!/usr/bin/env bash
set -euo pipefail

if [ -z "${SHA:-}" ]; then
  echo "::notice::no commit to prepare an image for; skipping"
  exit 0
fi

tag=$(git rev-parse --short "$SHA")
image="olakego/source-$DRIVER:$tag"
cache="$CACHE_REPO/source-$DRIVER:$tag"

if docker pull -q "$cache"; then
  docker tag "$cache" "$image"
  echo "restored $image from $cache"
  exit 0
fi

# That tree's own Iceberg jar: the Dockerfile copies it out of the build context, and the commit's
# Go side speaks that jar's RPC.
make -C "$SRC" iceberg.jar
docker buildx build --progress=plain --cache-from type=gha,scope=olake-base \
  --load --build-arg DRIVER_NAME="$DRIVER" -t "$image" "$SRC"

# Non-fatal: a fork's token is read-only, and failing to publish only costs the next run the build
# this one just did.
docker tag "$image" "$cache"
docker push "$cache" || echo "::notice::could not publish $cache (read-only token?)"
