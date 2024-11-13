function fail() {
    local error="${*:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

joined_arguments=""

function build_and_run() {
    local connector="$1"
    if [[ $2 == "driver" ]]; then
        path=drivers/$connector
    elif [[ $2 == "adapter" ]]; then
        path=adapters/$connector
    else
        fail "The argument does not have a recognized prefix."
    fi
    cd $path || fail "Failed to navigate to path: $path"
    go mod tidy
    go build -ldflags="-w -s -X constants/constants.version=${GIT_VERSION} -X constants/constants.commitsha=${GIT_COMMITSHA} -X constants/constants.releasechannel=${RELEASE_CHANNEL}" -o g5 main.go

    echo "============================== Executing connector: $connector with args [$joined_arguments] =============================="
    ./g5 $joined_arguments
}

if [ $# -gt 0 ]; then
    argument="$1"

    # Capture and join remaining arguments, skipping the first one
    remaining_arguments=("${@:2}")
    joined_arguments=$(
        IFS=' '
        echo "${remaining_arguments[*]}"
    )

    if [[ $argument == driver-* ]]; then
        driver="${argument#driver-}"
        echo "============================== Building driver: $driver =============================="
        build_and_run "$driver" "driver" "$joined_arguments"
    elif [[ $argument == adapter-* ]]; then
        adapter="${argument#adapter-}"
        echo "============================== Building adapter: $adapter =============================="
        build_and_run "$adapter" "adapter" "$joined_arguments"
    else
        fail "The argument does not have a recognized prefix."
    fi
else
    fail "No arguments provided."
fi
