# Build Stage
FROM golang:1.23-alpine AS base

WORKDIR /home/app
COPY . .

ARG DRIVER_NAME=olake
# Build the Go binary
WORKDIR /home/app/drivers/${DRIVER_NAME}
RUN go build -o /olake main.go

# Final Runtime Stage
FROM alpine:3.18

# Install Java 17 instead of Java 11
RUN apk add --no-cache openjdk17

# Copy the binary from the build stage
COPY --from=base /olake /home/olake
ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake

# Copy the pre-built JAR file from Maven
# First try to copy from the source location (works after Maven build)
COPY destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /home/olake-iceberg-java-writer.jar

# Copy the spec files
COPY drivers/${DRIVER_NAME}/spec.json /home/spec.json
COPY drivers/${DRIVER_NAME}/iceberg-spec.json /home/iceberg-spec.json
COPY drivers/${DRIVER_NAME}/parquet-spec.json /home/parquet-spec.json

# Metadata
LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}

# Set working directory
WORKDIR /home

# Entrypoint
ENTRYPOINT ["./olake"]