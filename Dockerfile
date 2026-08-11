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

FROM eclipse-temurin:17-jre-noble AS runtime-base

# Install runtime dependencies. The JRE comes from the base image: installing
# Debian's openjdk-*-jre-headless pulls ca-certificates-java, whose postinst is
# a perl script, which drags in perl (2 CRITICAL CVEs with no fix in any Debian
# release) plus libnss3. Temurin ships the JRE directly and avoids both.
#
# ca-certificates-java + the symlink keep the OS trust store and the JVM's in
# sync. Debian's openjdk did this for us; Temurin instead bakes a static cacerts,
# so a private CA added to the OS store would never reach the JVM and TLS to an
# internal REST catalog, Hive metastore or self-signed S3 endpoint would fail with
# "PKIX path building failed" while the Go side (which reads /etc/ssl/certs) kept
# working. Ubuntu's ca-certificates-java, unlike Debian's, needs no perl.
# update-ca-certificates must run here: the symlink dangles until it materializes
# the keystore, and a dangling cacerts means the JVM trusts nothing at all.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libxml2 \
    ca-certificates \
    ca-certificates-java \
    libpam-modules \
    libcrypt1 \
    iproute2 \
    lsof \
    && ln -sf /etc/ssl/certs/java/cacerts "$JAVA_HOME/lib/security/cacerts" \
    && update-ca-certificates -f \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base

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