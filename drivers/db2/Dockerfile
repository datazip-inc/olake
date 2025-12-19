# -------------------------
# Build Stage
# -------------------------
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git libxml2-dev wget tar build-base bash curl ca-certificates

# Download and extract IBM DB2 CLI Driver
WORKDIR /opt
RUN wget https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/linuxx64_odbc_cli.tar.gz \
    && tar -xzf linuxx64_odbc_cli.tar.gz \
    && rm linuxx64_odbc_cli.tar.gz

# Set DB2 driver environment
ENV DB2HOME=/opt/clidriver
ENV CGO_CFLAGS=-I/opt/clidriver/include
ENV CGO_LDFLAGS=-L/opt/clidriver/lib
ENV LD_LIBRARY_PATH=/opt/clidriver/lib:$LD_LIBRARY_PATH

# Copy entire repo to ensure internal packages resolve
WORKDIR /home/app
COPY . .

# Build DB2 driver binary
WORKDIR /home/app/drivers/db2
RUN go build -o /db2-driver main.go

# -------------------------
# Runtime Stage
# -------------------------
FROM alpine:3.18

# Install runtime dependencies
RUN apk add --no-cache bash ca-certificates libxml2

# Set DB2 driver environment (needed at runtime)
ENV DB2HOME=/opt/clidriver
ENV LD_LIBRARY_PATH=/opt/clidriver/lib:$LD_LIBRARY_PATH

# Copy binary from build stage
COPY --from=builder /db2-driver /usr/local/bin/db2-driver

# Copy DB2 CLI driver (optional if needed at runtime)
COPY --from=builder /opt/clidriver /opt/clidriver

# Set working directory
WORKDIR /app

# Default command
CMD ["db2-driver"]
