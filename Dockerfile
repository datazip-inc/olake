# Runtime base (shared across all drivers): OS + Java + JAR + destination specs
FROM alpine:3.18 AS runtime-base

RUN apk add --no-cache openjdk17 iproute2

# Copy the pre-built JAR and destination specs (these rarely change)
COPY destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /home/olake-iceberg-java-writer.jar
COPY destination/iceberg/resources/spec.json /destination/iceberg/resources/spec.json
COPY destination/parquet/resources/spec.json /destination/parquet/resources/spec.json

WORKDIR /home

# Build the Go driver binary (driver-specific)
FROM golang:1.23-alpine AS builder

ARG DRIVER_NAME=olake
WORKDIR /home/app
COPY . .
WORKDIR /home/app/drivers/${DRIVER_NAME}
RUN go build -o /olake main.go

# Final image: reuse runtime base, then add only driver-specific layers
FROM runtime-base

ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake

COPY --from=builder /olake /home/olake
COPY drivers/${DRIVER_NAME}/resources/spec.json /drivers/${DRIVER_NAME}/resources/spec.json

LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}

WORKDIR /home

ENTRYPOINT ["./olake"]