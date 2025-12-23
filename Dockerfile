# Build Stage
FROM golang:1.24-alpine AS base

WORKDIR /home/app
COPY . .

ARG DRIVER_NAME=olake
# Install build dependencies for DB2 (and others)
# libc6-compat is needed for cgo linking on Alpine
RUN apk add --no-cache git libc6-compat build-base

# Condition DB2 setup
# We use a single RUN block to limit layers and handle conditionals
RUN if [ "$DRIVER_NAME" = "db2" ]; then \
      apk add --no-cache bash libxml2-dev && \
      go run github.com/ibmdb/go_ibm_db/installer@latest; \
    else \
      mkdir -p /go/pkg/mod/github.com/ibmdb/clidriver; \
    fi

# Build the Go binary
WORKDIR /home/app/drivers/${DRIVER_NAME}
RUN if [ "$DRIVER_NAME" = "db2" ]; then \
      export IBM_DB_HOME=/go/pkg/mod/github.com/ibmdb/clidriver && \
      export CGO_CFLAGS=-I$IBM_DB_HOME/include && \
      export CGO_LDFLAGS=-L$IBM_DB_HOME/lib && \
      export LD_LIBRARY_PATH=$IBM_DB_HOME/lib && \
      go build -o /olake main.go; \
    else \
      go build -o /olake main.go; \
    fi

# Final Runtime Stage
FROM alpine:3.18

# Install Java 17 and iproute2 for ss command
RUN apk add --no-cache openjdk17 iproute2 gcompat libxml2 bash

# Copy the binary from the build stage
COPY --from=base /olake /home/olake

# Copy the DB2 CLI Driver (Runtime Libraries)
# This directory was ensured to exist in the builder stage
COPY --from=base /go/pkg/mod/github.com/ibmdb/clidriver /opt/clidriver

# Runtime Environment for DB2
ENV IBM_DB_HOME=/opt/clidriver
ENV PATH=$IBM_DB_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$IBM_DB_HOME/lib

ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake

# Copy the pre-built JAR file from Maven
# First try to copy from the source location (works after Maven build)
COPY destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /home/olake-iceberg-java-writer.jar

# Copy the spec files for driver and destinations
COPY --from=base /home/app/drivers/${DRIVER_NAME}/resources/spec.json /drivers/${DRIVER_NAME}/resources/spec.json
COPY --from=base /home/app/destination/iceberg/resources/spec.json /destination/iceberg/resources/spec.json
COPY --from=base /home/app/destination/parquet/resources/spec.json /destination/parquet/resources/spec.json

# Metadata
LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}

# Set working directory
WORKDIR /home

# Entrypoint
ENTRYPOINT ["./olake"]