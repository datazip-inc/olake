#!/bin/bash

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
    else
        fail "The argument does not have a recognized prefix."
    fi
    
    # Check if writer.json is specified in the arguments
    local writer_file=""
    local using_iceberg=false
    
    # Parse the arguments to find the writer.json file path
    local previous_arg=""
    for arg in $joined_arguments; do
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

    [[ -n "$OLAKE_SKIP_MOD_TIDY" ]] || (cd $path && go mod tidy)

    # prepare.<driver> provisions build deps (db2: the IBM clidriver) and GO_ENV.<driver>
    # supplies its cgo env, so this covers the whole driver-specific setup.
    make dev.$connector.build || fail "build failed"

    cd $path || fail "Failed to navigate to path: $path"

    echo "============================== Executing connector: $connector with args [$joined_arguments] =============================="
    ./olake $joined_arguments
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
    else
        fail "The argument does not have a recognized prefix."
    fi
else
    fail "No arguments provided."
fi