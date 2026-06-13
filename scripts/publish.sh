#!/usr/bin/env bash
# publish.sh — build and push the ocean-gateway image to ECR.
#
# Usage:
#   ./scripts/publish.sh [TAG]
#
# TAG defaults to the short git commit SHA. Override for a release:
#   ./scripts/publish.sh v1.2.3
#
# Prerequisites: aws CLI configured with push access to the ECR repository.

set -euo pipefail

REPO="185657066287.dkr.ecr.eu-west-1.amazonaws.com/ocean-gateway"
REGION="eu-west-1"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── resolve tag ───────────────────────────────────────────────────────────────
TAG="${1:-}"
if [ -z "$TAG" ]; then
  TAG="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
fi

COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

IMAGE="${REPO}:${TAG}"
IMAGE_LATEST="${REPO}:latest"

echo "Building  : $IMAGE"
echo "Commit    : $COMMIT"
echo "Date      : $DATE"
echo

# ── ECR login ─────────────────────────────────────────────────────────────────
echo "Logging in to ECR..."
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}"

# ── build ─────────────────────────────────────────────────────────────────────
echo
echo "Building image..."
docker build \
  --platform linux/amd64 \
  --build-arg VERSION="$TAG" \
  --build-arg COMMIT="$COMMIT" \
  --build-arg DATE="$DATE" \
  -t "$IMAGE" \
  -t "$IMAGE_LATEST" \
  "$REPO_ROOT"

# ── push ──────────────────────────────────────────────────────────────────────
echo
echo "Pushing $IMAGE ..."
docker push "$IMAGE"
echo "Pushing $IMAGE_LATEST ..."
docker push "$IMAGE_LATEST"

echo
echo "Done."
echo "  $IMAGE"
echo "  $IMAGE_LATEST"
