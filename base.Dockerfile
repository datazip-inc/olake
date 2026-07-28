# Shared base image for OLake driver builds. Local-only, never published.
# KEEP IN SYNC with the runtime stage of ./Dockerfile, else docker builds will be slow in CI.
FROM debian:bookworm-slim AS runtime

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
