#!/usr/bin/env bash
# Post-job cleanup: stop and remove the Robotocore container.
# Usage: cleanup.sh <container-id>
set -euo pipefail

CONTAINER_ID="${1:-${ROBOTOCORE_CONTAINER_ID:-}}"

if [ -z "$CONTAINER_ID" ]; then
  echo "No container ID provided, nothing to clean up."
  exit 0
fi

echo "Stopping Robotocore container: $CONTAINER_ID"
docker stop "$CONTAINER_ID" 2>/dev/null || true

echo "Removing Robotocore container: $CONTAINER_ID"
docker rm -f "$CONTAINER_ID" 2>/dev/null || true

echo "Cleanup complete."
