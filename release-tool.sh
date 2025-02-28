#!/usr/bin/env bash

# Function for colored output
function chalk() {
    local color=$1
    local text=$2
    local color_code=0
    if [[ $color == "red" ]]; then
        color_code=1
    elif [[ $color == "green" ]]; then
        color_code=2
    fi
    echo -e "$(tput setaf $color_code)${text}$(tput sgr0)"
}

# Function to fail with a message
function fail() {
    local error="${1:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

# Function to check and enable buildx support
function setup_buildx() {
    echo "Setting up Docker buildx and QEMU..."
    docker buildx version >/dev/null 2>&1 || fail "Docker buildx is not installed. Please install it."
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes || fail "Failed to set up QEMU"
    docker buildx create --use --name multiarch-builder || echo "Buildx builder already exists, using it."
    docker buildx inspect --bootstrap || fail "Failed to bootstrap buildx builder"
    echo "✅ Buildx and QEMU setup complete"
}

# Function to perform the release
function release() {
    local version=$1
    local platform=$2
    local is_test=${3:-false}
    local image_name="$DHID/$type-$connector"
    local tag_version=""
    
    if [[ "$is_test" == "true" ]]; then
        tag_version="testing-${version}"
    else
        tag_version="${version}"
    fi

    echo "Logging into Docker..."
    docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" || fail "Docker login failed for $DOCKER_LOGIN"
    echo "**** Releasing $image_name for platforms [$platform] with version [$tag_version] ****"

    # Attempt multi-platform build
    echo "Attempting multi-platform build..."
    
    # For test images, only tag with the specified version, not 'latest'
    if [[ "$is_test" == "true" ]]; then
        docker buildx build --platform "$platform" --push \
            -t "${image_name}:${tag_version}" \
            --build-arg DRIVER_NAME="$connector" \
            --build-arg DRIVER_VERSION="$VERSION" . || fail "Multi-platform build failed. Exiting..."
    else
        docker buildx build --platform "$platform" --push \
            -t "${image_name}:${tag_version}" \
            -t "${image_name}:latest" \
            --build-arg DRIVER_NAME="$connector" \
            --build-arg DRIVER_VERSION="$VERSION" . || fail "Multi-platform build failed. Exiting..."
    fi
    
    echo "$(chalk green "Release successful for $image_name version $tag_version")"
}

# Main script execution
SEMVER_EXPRESSION='v([0-9].[0-9].[0-9]+(\S*))'
echo "Release tool running..."
CURRENT_BRANCH=$(git branch --show-current)
echo "Fetching remote changes from git with git fetch"
git fetch origin "$CURRENT_BRANCH" >/dev/null 2>&1
GIT_COMMITSHA=$(git rev-parse HEAD | cut -c 1-8)
echo "Latest commit SHA: $GIT_COMMITSHA"

echo "Running checks..."

# Verify Docker login
docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" >/dev/null 2>&1 || fail "❌ Docker login failed. Ensure DOCKER_LOGIN and DOCKER_PASSWORD are set."
echo "✅ Docker login successful"

# Check branch
if [[ $CURRENT_BRANCH == "master" ]]; then
    echo "✅ Git branch is $CURRENT_BRANCH"
else
    echo "⚠️ Git branch $CURRENT_BRANCH is not master. Proceeding anyway."
fi

# Determine if we're in test mode
# Default to false if not set
TEST_MODE="${TEST_MODE:-false}"
echo "Test mode: $TEST_MODE"

# Check version (skip strict validation for test images)
if [[ -z "$VERSION" ]]; then
    fail "❌ Version not set. Empty version passed."
elif [[ "$TEST_MODE" == "true" ]]; then
    echo "✅ Test mode active - skipping semantic version validation for version: $VERSION"
elif [[ $VERSION =~ $SEMVER_EXPRESSION ]]; then
    echo "✅ Version $VERSION matches semantic versioning."
else
    fail "❌ Version $VERSION does not match semantic versioning. Example: v1.0.0, v1.0.0-alpha.beta, v0.6.0-rc.6fd"
fi

# Setup buildx and QEMU
setup_buildx

# Release the driver
platform="linux/amd64,linux/arm64"
echo "✅ Releasing driver $DRIVER for version $VERSION to platforms: $platform"

chalk green "=== Releasing driver: $DRIVER ==="
chalk green "=== Release channel: $RELEASE_CHANNEL ==="
chalk green "=== Release version: $VERSION ==="
chalk green "=== Test mode: $TEST_MODE ==="
connector=$DRIVER
type="source"

release "$VERSION" "$platform" "$TEST_MODE"
