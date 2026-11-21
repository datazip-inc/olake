#!/bin/bash

function fail() {
    local error="${*:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

function build_and_run() {
    local connector="$1"
    if [[ "$2" == "driver" ]]; then
        local path="drivers/$connector"
    else
        fail "The argument does not have a recognized prefix."
    fi
    shift 2
    local -a connector_arguments=("$@")

    # Check if writer.json is specified in the arguments
    local writer_file=""
    local using_iceberg=false

    # Parse the arguments to find the writer.json file path
    local previous_arg=""
    local arg
    for arg in "${connector_arguments[@]}"; do
        if [[ "$previous_arg" == "--destination" || "$previous_arg" == "-d" ]]; then
            writer_file="$arg"
            break
        fi
        previous_arg="$arg"
    done

    # If writer file was found, check if it contains iceberg
    if [[ -n "$writer_file" && -f "$writer_file" ]]; then
        echo "Checking writer file: $writer_file for iceberg destination..."
        if grep -qi "iceberg" "$writer_file"; then
            echo "Iceberg destination detected in writer file."
            using_iceberg=true
        fi
    fi

    # If using iceberg, build the writer JAR (skips maven when up to date)
    if [[ "$using_iceberg" == true ]]; then
        make iceberg.jar || fail "Iceberg writer JAR build failed"
    fi

    [[ -n "${OLAKE_SKIP_MOD_TIDY:-}" ]] || (cd "$path" && go mod tidy)

    # prepare.<driver> provisions build deps (db2: the IBM clidriver) and GO_ENV.<driver>
    # supplies its cgo env, so this covers the whole driver-specific setup.
    make dev.$connector.build || fail "build failed"

    cd "$path" || fail "Failed to navigate to path: $path"

    local formatted_arguments
    printf -v formatted_arguments '%q ' "${connector_arguments[@]}"
    echo "============================== Executing connector: $connector with args [$formatted_arguments] =============================="
    ./olake "${connector_arguments[@]}"
}

if [ $# -gt 0 ]; then
    argument="$1"

    # Capture remaining arguments without losing their original boundaries
    remaining_arguments=("${@:2}")

    if [[ $argument == driver-* ]]; then
        driver="${argument#driver-}"
        echo "============================== Building driver: $driver =============================="
        build_and_run "$driver" "driver" "${remaining_arguments[@]}"
    else
        fail "The argument does not have a recognized prefix."
    fi
else
    fail "No arguments provided."
fi
