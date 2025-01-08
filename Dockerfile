# Build Stage
FROM golang:1.22-alpine AS base

WORKDIR /home/app
COPY . .

ARG DRIVER_FOLDER_NAME=olake
# Build the Go binary
WORKDIR /home/app/drivers/${DRIVER_FOLDER_NAME}
RUN go build -o /olake main.go

# Final Runtime Stage
FROM alpine:3.18

# Copy the binary from the build stage
COPY --from=base /olake /home/olake

ARG DRIVER_FOLDER_NAME=olake
# Metadata
LABEL io.eggwhite.version=2.0.24
LABEL io.eggwhite.name=olake/source-${DRIVER_FOLDER_NAME}

# Set working directory
WORKDIR /home

# Entrypoint
ENTRYPOINT ["./olake"]
