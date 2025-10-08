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
    # Check if TERM is set before using tput
    if [[ -n "$TERM" ]]; then
        echo -e "$(tput setaf $color_code)${text}$(tput sgr0)"
    else
        # Fallback if TERM is not set
        if [[ $color == "red" ]]; then
            echo -e "\033[31m${text}\033[0m"
        elif [[ $color == "green" ]]; then
            echo -e "\033[32m${text}\033[0m"
        else
            echo -e "${text}"
        fi
    fi
}

# Function to build the Java project with Maven
function build_java_project() {
    echo "Building Java project with Maven..."
    # Use a fixed timestamp based on the last Java source change for reproducible JARs
    local MODULE_DIR="destination/iceberg/olake-iceberg-java-writer"
    local MODULE_SRC_DIR="${MODULE_DIR}/src"
    export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct -- "${MODULE_SRC_DIR}")"
    # Change to the directory containing the POM file
    cd "${MODULE_DIR}" || fail "Failed to change to Maven project directory"
    echo "Building Maven project in $(pwd)"
    mvn clean package -Dmaven.test.skip=true -Dproject.build.outputTimestamp="${SOURCE_DATE_EPOCH}" || fail "Maven build failed"
    # Record commit that produced this JAR for safe reuse decisions
    local CURRENT_MODULE_COMMIT
    CURRENT_MODULE_COMMIT="$(git log -1 --format=%H -- "${MODULE_SRC_DIR}")"
    echo -n "$CURRENT_MODULE_COMMIT" > target/.built-commit
    # Return to the original directory
    cd - || fail "Failed to return to original directory"
    echo "$(chalk green "✅ Java project successfully built")"
}

# Build the JAR only if a prebuilt artifact isn't already present (enables sharing across jobs)
function ensure_java_jar() {
    local MODULE_DIR="destination/iceberg/olake-iceberg-java-writer"
    local MODULE_SRC_DIR="${MODULE_DIR}/src"
    local jar_path="${MODULE_DIR}/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar"
    local built_commit_file="${MODULE_DIR}/target/.built-commit"
    local current_module_commit
    current_module_commit="$(git log -1 --format=%H -- "${MODULE_SRC_DIR}")"

    if [[ -f "$jar_path" && -s "$jar_path" && -f "$built_commit_file" ]]; then
        local built_commit
        built_commit="$(cat "$built_commit_file" 2>/dev/null || echo)"
        if [[ "$built_commit" == "$current_module_commit" ]]; then
            echo "Using prebuilt JAR: $jar_path (commit $built_commit)"
            return 0
        fi
        echo "Java sources changed since last build ($built_commit -> $current_module_commit); rebuilding JAR"
    fi
    build_java_project
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
    local branch=${3:-master}
    local image_name="$DHID/$type-$connector"
    
    # Default to dev mode
    local tag_version="dev-${version}"
    local latest_tag="dev-latest"

    # Override for special branches
    if [[ "$branch" == "master" ]]; then
        tag_version="${version}"
        latest_tag="latest"
    elif [[ "$branch" == "staging" ]]; then
        tag_version="stag-${version}"
        latest_tag="stag-latest"
    fi

    echo "Logging into Docker..."
    docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" || fail "Docker login failed for $DOCKER_LOGIN"
    echo "**** Releasing $image_name for platforms [$platform] with version [$tag_version] ****"

    # Attempt multi-platform build
    echo "Attempting multi-platform build..."
    
    docker buildx build --platform "$platform" --push \
        -t "${image_name}:${tag_version}" \
        -t "${image_name}:${latest_tag}" \
        --build-arg DRIVER_NAME="$connector" \
        --build-arg DRIVER_VERSION="$VERSION" . || fail "Multi-platform build failed. Exiting..."
    
    echo "$(chalk green "Release successful for $image_name version $tag_version")"
}

# Main script execution
SEMVER_EXPRESSION='v([0-9]+\.[0-9]+\.[0-9]+)$'
STAGING_VERSION_EXPRESSION='v([0-9]+\.[0-9]+\.[0-9]+)-[a-zA-Z0-9_.-]+'

echo "Release tool running..."
CURRENT_BRANCH=$(git branch --show-current)
echo "Building on branch: $CURRENT_BRANCH"
echo "Fetching remote changes from git with git fetch"
git fetch origin "$CURRENT_BRANCH" >/dev/null 2>&1
GIT_COMMITSHA=$(git rev-parse HEAD | cut -c 1-8)
echo "Latest commit SHA: $GIT_COMMITSHA"

echo "Running checks..."

# Verify Docker login
docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" >/dev/null 2>&1 || fail "❌ Docker login failed. Ensure DOCKER_LOGIN and DOCKER_PASSWORD are set."
echo "✅ Docker login successful"

# Version validation based on branch (default is dev with no restrictions)
if [[ -z "$VERSION" ]]; then
    fail "❌ Version not set. Empty version passed."
fi

# Only validate special branches
if [[ "$CURRENT_BRANCH" == "master" ]]; then
    [[ $VERSION =~ $SEMVER_EXPRESSION ]] || fail "❌ Version $VERSION does not match semantic versioning required for master branch (e.g., v1.0.0)"
    echo "✅ Version $VERSION matches semantic versioning for master branch"
elif [[ "$CURRENT_BRANCH" == "staging" ]]; then
    [[ $VERSION =~ $STAGING_VERSION_EXPRESSION ]] || fail "❌ Version $VERSION does not match staging version format (e.g., v1.0.0-rc1)"
    echo "✅ Version $VERSION matches format for staging branch"
else
    echo "✅ Flexible versioning allowed for development branch: $VERSION"
fi

# Setup buildx and QEMU
setup_buildx

# Release the driver
platform="linux/amd64,linux/arm64"
echo "✅ Releasing driver $DRIVER for version $VERSION on branch $CURRENT_BRANCH to platforms: $platform"

chalk green "=== Releasing driver: $DRIVER ==="
chalk green "=== Branch: $CURRENT_BRANCH ==="
chalk green "=== Release version: $VERSION ==="
connector=$DRIVER
type="source"


# Build Java project (or reuse prebuilt artifact if present)
ensure_java_jar

release "$VERSION" "$platform" "$CURRENT_BRANCH"