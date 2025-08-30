# /*
#  * Copyright 2025 Olake By Datazip
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */

#!/bin/bash

function fail() {
    local error="${*:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

joined_arguments=""

# Function to check and build the Java JAR file for iceberg if needed
function check_and_build_jar() {
    local connector="$1"

    
    echo "============================== Checking for Iceberg JAR file =============================="
    
    # Check if the JAR exists in the base directory
    if [ -f "olake-iceberg-java-writer.jar" ]; then
        echo "JAR file found in base directory."
        return 0
    fi
    
    # Check in the target directory
    if [ -f "destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar" ]; then
        echo "JAR file found in target directory, copying to base directory..."
        cp destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar ./olake-iceberg-java-writer.jar
        return 0
    fi
    
    # If JAR not found, build it
    echo "Iceberg JAR file not found. Building with Maven..."
    
    # Store current directory
    local current_dir=$(pwd)
    
    # Navigate to the Maven project directory
    if [ -d "destination/iceberg/olake-iceberg-java-writer" ]; then
        cd destination/iceberg/olake-iceberg-java-writer
    else
        fail "Cannot find Iceberg Maven project directory."
    fi
    
    # Build with Maven
    mvn clean package -Dmaven.test.skip=true || fail "Maven build failed"
    
    # Return to original directory
    cd "$current_dir"
    
    # Copy the JAR file to the base directory
    if [ -f "destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar" ]; then
        cp destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar ./olake-iceberg-java-writer.jar
    else
        fail "Maven build completed but could not find the JAR file."
    fi
    
    echo "============================== JAR file built and copied to base directory =============================="
}

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
    
    # If using iceberg, check and potentially build the JAR
    if [[ "$using_iceberg" == true ]]; then
        check_and_build_jar "iceberg"
    fi

    cd $path || fail "Failed to navigate to path: $path"
    
    go mod tidy
    go build -ldflags="-w -s -X constants/constants.version=${GIT_VERSION} -X constants/constants.commitsha=${GIT_COMMITSHA} -X constants/constants.releasechannel=${RELEASE_CHANNEL}" -o olake main.go || fail "build failed"
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
