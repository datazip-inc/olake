#!/bin/bash

function fail() {
    local error="${*:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

joined_arguments=""

# Function to download DB2 clidriver using curl as fallback
function download_db2_clidriver() {
    local install_dir="$1"
    local os_type=$(uname -s)
    local arch_type=$(uname -m)
    local download_url=""
    local file_name=""
    
    echo "Detecting OS and architecture: $os_type / $arch_type"
    
    case "$os_type" in
        "Darwin")
            if [[ "$arch_type" == "arm64" ]]; then
                download_url="https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/macarm64_odbc_cli.tar.gz"
                file_name="macarm64_odbc_cli.tar.gz"
            else
                download_url="https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/macos64_odbc_cli.tar.gz"
                file_name="macos64_odbc_cli.tar.gz"
            fi
            ;;
        "Linux")
            download_url="https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/linuxx64_odbc_cli.tar.gz"
            file_name="linuxx64_odbc_cli.tar.gz"
            ;;
        "MINGW"*|"CYGWIN"*|"MSYS"*)
            download_url="https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/ntx64_odbc_cli.zip"
            file_name="ntx64_odbc_cli.zip"
            ;;
        *)
            fail "Unsupported operating system: $os_type"
            ;;
    esac
    
    echo "Downloading DB2 CLI driver from: $download_url"
    
    # Download the file
    curl -L -o "$install_dir/$file_name" "$download_url" || fail "Failed to download DB2 CLI driver"
    
    # Extract the file
    cd "$install_dir" || fail "Failed to navigate to $install_dir"
    
    if [[ "$file_name" == *.tar.gz ]]; then
        tar -xzf "$file_name" || fail "Failed to extract DB2 CLI driver"
    elif [[ "$file_name" == *.zip ]]; then
        unzip -o "$file_name" || fail "Failed to extract DB2 CLI driver"
    fi
    
    # Clean up the downloaded archive
    rm -f "$file_name"
    
    echo "DB2 CLI driver extracted successfully"
}

# Function to check and setup DB2 clidriver
function setup_db2_clidriver() {
    echo "============================== Checking for DB2 CLI Driver =============================="
    
    # Define the clidriver installation path
    local clidriver_path=""
    
    # Check common locations for clidriver
    if [ -d "/opt/clidriver" ]; then
        clidriver_path="/opt/clidriver"
        echo "CLI driver found at /opt/clidriver"
    elif [ -d "$HOME/clidriver" ]; then
        clidriver_path="$HOME/clidriver"
        echo "CLI driver found at $HOME/clidriver"
    elif [ -d "$(pwd)/clidriver" ]; then
        clidriver_path="$(pwd)/clidriver"
        echo "CLI driver found at $(pwd)/clidriver"
    fi
    
    # If clidriver not found, download it
    if [ -z "$clidriver_path" ]; then
        echo "DB2 CLI driver not found. Downloading..."
        
        # Store current directory
        local current_dir=$(pwd)
        
        # Installation directory
        local install_dir="$HOME"
        
        # Download clidriver using curl
        download_db2_clidriver "$install_dir"
        
        # Return to original directory
        cd "$current_dir"
        
        # Check if installation was successful
        if [ -d "$HOME/clidriver" ]; then
            clidriver_path="$HOME/clidriver"
            echo "CLI driver installed at $HOME/clidriver"
        else
            fail "DB2 CLI driver installation failed - clidriver directory not found"
        fi
    fi
    
    # Set environment variables
    export IBM_DB_HOME="$clidriver_path"
    export PATH="$IBM_DB_HOME/bin:$PATH"
    export CGO_CFLAGS="-I$IBM_DB_HOME/include"
    export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -Wl,-rpath,$IBM_DB_HOME/lib"
    export LD_LIBRARY_PATH="$IBM_DB_HOME/lib:$LD_LIBRARY_PATH"
    
    # For macOS, set DYLD_LIBRARY_PATH
    if [[ "$(uname)" == "Darwin" ]]; then
        export DYLD_LIBRARY_PATH="$IBM_DB_HOME/lib:$DYLD_LIBRARY_PATH"
    fi
    
    echo "DB2 environment variables set:"
    echo "  IBM_DB_HOME=$IBM_DB_HOME"
    echo "  CGO_CFLAGS=$CGO_CFLAGS"
    echo "  CGO_LDFLAGS=$CGO_LDFLAGS"
    echo "  LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
    
    # Verify the lib directory exists and is accessible
    if [ ! -d "$IBM_DB_HOME/lib" ]; then
        fail "DB2 CLI driver lib directory not found at $IBM_DB_HOME/lib"
    fi
    
    echo "============================== DB2 CLI Driver Setup Complete =============================="
}

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

    # If driver is db2, setup the clidriver environment
    if [[ "$connector" == "db2" ]]; then
        setup_db2_clidriver
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