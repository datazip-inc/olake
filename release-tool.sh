#!/usr/bin/env bash

# Highlights args with color
# Only red is supported so far
#
function chalk() {
    local color=$1
    olake
    local color_code=0
    if [[ $color == "red" ]]; then
        color_code=1
    elif [[ $color == "green" ]]; then
        color_code=2
    fi

    echo -e "$(tput setaf $color_code)$*$(tput sgr0)"
}

function fail() {
    local error="$*" || 'Unknown error'
    echo "$(chalk red "${error}")"
    exit 1
}

function release() {
    docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" || fail "Docker ($DOCKER_LOGIN) login failed"
    echo "**** $type-$connector $2 release [$1] ****"
    cd $path
    docker buildx build --platform $2 --push -t "$DHID"/$type-$connector:"$1" -t "$DHID"/$type-$connector:latest --build-arg DRIVER_NAME="$connector" --build-arg DRIVER_VERSION="$VERSION" . || fail 'dockerx build failed'
}

SEMVER_EXPRESSION='v([0-9].[0-9].[0-9]+(\S*))'
echo "Release tool running..."
CURRENT_BRANCH=$(git branch --show-current)
echo "Fetching remote changes from git with git fetch"
git fetch origin "$CURRENT_BRANCH" >/dev/null 2>&1
GIT_COMMITSHA=$(git rev-parse HEAD | cut -c 1-8)
echo "Latest commit SHA $GIT_COMMITSHA"

echo "Running checks..."

docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" >/dev/null 2>&1 || fail '   ❌ Olake docker login failed. Make sure that DOCKER_LOGIN and DOCKER_PASSWORD are properly set'
echo "   ✅ Can login with Olake docker account"

if [[ $CURRENT_BRANCH == "master" ]]; then
    echo "   ✅ Git branch is $CURRENT_BRANCH"
else
    echo "   ⚠️ Git branch $CURRENT_BRANCH is not master."
fi

platform="linux/amd64,linux/arm64"

echo "Releasing Driver"
VERSION=$(git rev-parse HEAD | cut -c 1-8)

# check if version is empty
if [[ $VERSION == "" ]]; then
    fail " ❌ Failed to get version"
fi
# check if version passed regex
if [[ $VERSION =~ $SEMVER_EXPRESSION ]]; then
    echo "✅ Version $VERSION matches Regex Expression"
else
    fail "❌ Version $VERSION does not matches Regex Expression; eg v1.0.0, v1.0.0-alpha.beta, v0.6.0-rc.6fd"
fi

echo "✅ Releasing driver; Version is $VERSION Target platform: $platform"


# checking again if version is not empty
if [[ $VERSION == "" ]]; then
    fail "❌ Version not set; Empty version passed"
fi

chalk green "=== Releasing driver: $driver ==="
chalk green "=== Release channel: $RELEASE_CHANNEL ==="
chalk green "=== Release version: $VERSION ==="
connector=$driver
type="source"

release $VERSION $platform
