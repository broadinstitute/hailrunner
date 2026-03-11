#!/bin/bash

set -ex

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HAILRUNNER_VERSION="0.1.0"
HAIL_VERSION="0.2.132"
PUSH_TAG="us-docker.pkg.dev/broad-dsde-methods/hailrunner/hailrunner:${HAILRUNNER_VERSION}"

docker buildx build \
    --build-arg HAIL_VERSION="$HAIL_VERSION" \
    -t "${PUSH_TAG}" \
    --platform linux/amd64 \
    --push \
    "$SCRIPT_DIR"
