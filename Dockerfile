# Build Stage
FROM golang:1.24-bookworm AS builder

WORKDIR /home/app
COPY . .

ARG DRIVER_NAME=olake

# DB2 conditional setup
RUN if [ "$DRIVER_NAME" = "db2" ]; then \
  go run github.com/ibmdb/go_ibm_db/installer@latest; \
else \
  mkdir -p /go/pkg/mod/github.com/ibmdb/clidriver; \
fi

# Build the Go binary
WORKDIR /home/app/drivers/${DRIVER_NAME}
RUN if [ "$DRIVER_NAME" = "db2" ]; then \
  export IBM_DB_HOME=/go/pkg/mod/github.com/ibmdb/clidriver && \
  export CGO_CFLAGS="-I$IBM_DB_HOME/include" && \
  export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -Wl,-rpath,$IBM_DB_HOME/lib" && \
  export LD_LIBRARY_PATH=$IBM_DB_HOME/lib && \
  go build -o /olake main.go; \
else \
  go build -o /olake main.go; \
fi

# Final Runtime Stage
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

# Copy the binary from the build stage
COPY --from=builder /olake /home/olake

# Copy DB2 CLI Driver
COPY --from=builder /go/pkg/mod/github.com/ibmdb/clidriver /opt/clidriver

# Set DB2 CLI environment variables
ENV IBM_DB_HOME=/opt/clidriver
ENV PATH=$IBM_DB_HOME/bin:$PATH
ENV CGO_CFLAGS="-I$IBM_DB_HOME/include"
ENV CGO_LDFLAGS="-L$IBM_DB_HOME/lib -Wl,-rpath,$IBM_DB_HOME/lib"
ENV LD_LIBRARY_PATH=$IBM_DB_HOME/lib

# Driver metadata
ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake

# Copy the pre-built JAR file from Maven
# First try to copy from the source location (works after Maven build)
COPY destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /home/olake-iceberg-java-writer.jar

# Copy driver and destination spec files
COPY --from=builder /home/app/drivers/${DRIVER_NAME}/resources/spec.json /drivers/${DRIVER_NAME}/resources/spec.json
COPY --from=builder /home/app/destination/iceberg/resources/spec.json /destination/iceberg/resources/spec.json
COPY --from=builder /home/app/destination/parquet/resources/spec.json /destination/parquet/resources/spec.json

# Metadata labels
LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}

# Set working directory
WORKDIR /home

# Entrypoint
ENTRYPOINT ["./olake"]