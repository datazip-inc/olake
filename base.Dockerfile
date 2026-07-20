# Shared base images for OLake driver builds.
#   build   — Go toolchain + JRE/maven/node(chalk)/jq; used only by the disposable
#             integration-test container (utils/testutils/test_utils.go), which
#             builds this image locally on first use.
#   runtime — slim base the shipped driver images sit on.
#
# Local-only — not published to a registry. Rebuild when this file changes:
#   make docker.base.build   # host arch (set BASE_PLATFORMS to cross-build)
#
# The golang version comes from the go directive in go.mod: docker.base.build
# passes it as GO_VERSION and derives the image tag (build-go<version>) from the
# same line, so bumping go.mod is the only step. The default (golang:1-bookworm,
# the rolling latest-Go-1.x tag) ONLY APPLIES TO DIRECT DOCKER BUILDS.

# ---------------------------------------------------------------------------
ARG GO_VERSION=1
FROM golang:${GO_VERSION}-bookworm AS build

RUN apt-get update && apt-get install -y \
        openjdk-17-jre-headless maven nodejs npm jq \
    && npm install -g chalk-cli \
    && rm -rf /var/lib/apt/lists/*
