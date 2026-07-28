FROM golang:1.25.12-bookworm AS makefiles
WORKDIR /home/app
COPY . .
RUN mkdir /out && cp --parents Makefile $(find drivers -maxdepth 2 -name driver.mk) /out

# Build Stage
FROM golang:1.25.12-bookworm AS builder

WORKDIR /home/app

ARG DRIVER_NAME=olake

# prepare the driver
# OVERLAY_DIR tells it to stage runtime files for this image instead of onto a host (copied onto / below).
COPY --from=makefiles /out/ .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make prepare.${DRIVER_NAME} OVERLAY_DIR=/runtime-overlay

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make dev.${DRIVER_NAME}.build OVERLAY_DIR=/runtime-overlay

FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    libxml2 \
    ca-certificates \
    libpam-modules \
    libcrypt1 \
    iproute2 \
    lsof \
    && rm -rf /var/lib/apt/lists/*

# Driver metadata
ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake

# Copy the binary from the build stage (dev.<driver>.build writes it in-tree)
COPY --from=builder /home/app/drivers/${DRIVER_NAME}/olake /home/olake

# Sets the version of olake in ENV
ENV DRIVER_VERSION=${DRIVER_VERSION}

# Copy the pre-built JAR file from Maven
# First try to copy from the source location (works after Maven build)
COPY destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /home/olake-iceberg-java-writer.jar

# Pre-load the Iceberg writer's startup classes into an AppCDS archive.
RUN out=$(java -XX:+UseG1GC -XX:ArchiveClassesAtExit=/home/olake-iceberg-java-writer.jsa \
      -jar /home/olake-iceberg-java-writer.jar '{' 2>&1 || true); \
    echo "$out" | grep -q 'OlakeRpcServer.main' || { \
      echo 'AppCDS dump never reached OlakeRpcServer.main - is the jar shaded?' >&2; \
      echo "$out" | head -20 >&2; exit 1; }; \
    test -s /home/olake-iceberg-java-writer.jsa

# Copy driver and destination spec files
COPY --from=builder /home/app/drivers/${DRIVER_NAME}/resources/spec.json /drivers/${DRIVER_NAME}/resources/spec.json
COPY --from=builder /home/app/destination/iceberg/resources/spec.json /destination/iceberg/resources/spec.json
COPY --from=builder /home/app/destination/parquet/resources/spec.json /destination/parquet/resources/spec.json

# Driver-specific runtime files staged by `make prepare.<driver>`. Empty for every driver except
# db2, which stages the IBM clidriver its cgo build links against.
COPY --from=builder /runtime-overlay/ /
RUN ldconfig

# Metadata labels
LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}

# Set working directory
WORKDIR /home

# Entrypoint
ENTRYPOINT ["./olake"]