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
    docker buildx build --platform $2 --push -t "$DHID"/$type-$connector:"$RELEASE_CHANNEL"-"$1" -t "$DHID"/$type-$connector:"$RELEASE_CHANNEL"-latest --build-arg GIT_COMMITSHA="$GIT_COMMITSHA" --build-arg GIT_VERSION="$GIT_VERSION" --build-arg RELEASE_CHANNEL="$RELEASE_CHANNEL" . || fail 'dockerx build failed'
}

SEMVER_EXPRESSION='v([0-9].[0-9].[0-9]+(\S*))'
echo "Release tool running..."
CURRENT_BRANCH=$(git branch --show-current)
echo "Fetching remote changes from git with git fetch"
git fetch origin "$CURRENT_BRANCH" >/dev/null 2>&1
GIT_COMMITSHA=$(git rev-parse HEAD | cut -c 1-8)
echo "Latest commit SHA $GIT_COMMITSHA"

echo "Running checks..."

docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" >/dev/null 2>&1 || fail '   ❌ DZ-Cloud docker login failed. Make sure that DOCKER_LOGIN and DOCKER_PASSWORD are properly set'
echo "   ✅ Can login with DZ-Cloud docker account"

if [[ $CURRENT_BRANCH == "master" ]]; then
    echo "   ✅ Git branch is $CURRENT_BRANCH"
else
    echo "   ⚠️ Git branch $CURRENT_BRANCH is not master."
fi

platform="linux/amd64,linux/arm64"
if [[ $TARGET_ARCH == "arm" ]]; then
    platform="linux/arm64"
elif [[ $TARGET_ARCH == "both" ]]; then
    platform="linux/amd64,linux/arm64"
fi

if [[ $RELEASE_CHANNEL == "stable" ]]; then
    echo "Releasing stable. Checking if HEAD is tagged"
    git describe --tags --exact-match HEAD >/dev/null 2>&1 || fail "   ❌ HEAD is not tagged. Run git describe --exact-match HEAD "
    latest_tag=$(git describe --tags --exact-match HEAD)

    IFS='/' read -ra tag_elements <<<"$latest_tag"

    if [ "${#tag_elements[@]}" -ne 2 ]; then
        fail "Error: The latest_tag($latest_tag) does not have exactly two elements."
    fi
    argument=$tag_elements[0]
    VERSION=$$tag_elements[1]

    # check if version is empty
    if [[ $VERSION == "" ]]; then
        fail " ❌ Failed to get version via latest tag"
    fi
    # check if version passed regex
    if [[ $VERSION =~ $SEMVER_EXPRESSION ]]; then
        echo "✅ Version $VERSION matches Regex Expression"
    else
        fail "❌ Version $VERSION does not matches Regex Expression; eg v1.0.0, v1.0.0-alpha.beta, v0.6.0-rc.6fd"
    fi

    echo "   ✅ Releasing stable channel; Latest tag is $latest_tag, Version is $VERSION Target platform: $platform"
else
    echo "Releasing edge channel version: $VERSION Target platform: $platform"
fi

# checking again if version is not empty
if [[ $VERSION == "" ]]; then
    fail "❌ Version not set; Empty version passed"
fi

if [[ $argument == driver-* ]]; then
    driver="${argument#driver-}"
    chalk green "=== Releasing driver: $driver ==="
    connector=$driver
    type="driver"
elif [[ $argument == adapter-* ]]; then
    adapter="${argument#adapter-}"
    chalk green "=== Releasing adapter: $adapter ==="
    connector=$adapter
    type="adapter"
else
    fail "The argument does not have a recognized prefix."
fi

if [[ $2 == "driver" ]]; then
    path=drivers/$connector
elif [[ $2 == "adapter" ]]; then
    path=adapters/$connector
else
    fail "The argument does not have a recognized prefix."
fi

chalk green "=== Release channel: $RELEASE_CHANNEL ==="

chalk green "=== Release version: $VERSION ==="

# Set GIT_VERSION with VERSION
GIT_VERSION=$VERSION

release $VERSION $platform
