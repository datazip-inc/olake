# Collect the root Makefile and per-driver fragments (drivers/*/driver.mk) so
# the prepare layer below is cache-keyed on just the make files, not the whole
# source tree.
FROM golang:1.25.12-bookworm AS makefiles
WORKDIR /home/app
COPY . .
RUN mkdir /out && cp --parents Makefile $(find drivers -maxdepth 2 -name driver.mk) /out

# Build Stage
FROM golang:1.25.12-bookworm AS builder

WORKDIR /home/app

ARG DRIVER_NAME=olake

# Driver-specific setup and build live in make (prepare.<driver> / build.<driver>,
# per-driver logic in drivers/<driver>/driver.mk). prepare runs before the source
# COPY so slow downloads (e.g. the db2 clidriver) stay layer-cached across source
# changes.
COPY --from=makefiles /out/ .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make prepare.${DRIVER_NAME}

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make build.${DRIVER_NAME} OUTPUT=/olake

# Runtime Stage (common to all drivers)
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    libxml2 \
    ca-certificates \
    libpam-modules \
    libcrypt1 \
    && rm -rf /var/lib/apt/lists/*

# Driver metadata
ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake

# Copy the binary from the build stage
COPY --from=builder /olake /home/olake

# Sets the version of olake in ENV
ENV DRIVER_VERSION=${DRIVER_VERSION}

# Copy the pre-built JAR file from Maven
# First try to copy from the source location (works after Maven build)
COPY destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /home/olake-iceberg-java-writer.jar

# Copy driver and destination spec files
COPY --from=builder /home/app/drivers/${DRIVER_NAME}/resources/spec.json /drivers/${DRIVER_NAME}/resources/spec.json
COPY --from=builder /home/app/destination/iceberg/resources/spec.json /destination/iceberg/resources/spec.json
COPY --from=builder /home/app/destination/parquet/resources/spec.json /destination/parquet/resources/spec.json

# Driver-specific runtime files staged by `make prepare.<driver>` (empty for most drivers)
COPY --from=builder /runtime-overlay/ /
RUN ldconfig

# Metadata labels
LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}

# Set working directory
WORKDIR /home

# Entrypoint
ENTRYPOINT ["./olake"]