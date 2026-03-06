#!/bin/bash
set -e

echo "============================================"
echo "  Robotocore — AWS Emulator"
echo "  Port: ${ROBOTOCORE_PORT:-4566}"
echo "  State: ${ROBOTOCORE_STATE_DIR:-in-memory only}"
echo "============================================"

# Create state directory if configured
if [ -n "$ROBOTOCORE_STATE_DIR" ]; then
    mkdir -p "$ROBOTOCORE_STATE_DIR"
fi

exec uv run uvicorn robotocore.gateway.app:app \
    --host "${ROBOTOCORE_HOST:-0.0.0.0}" \
    --port "${ROBOTOCORE_PORT:-4566}" \
    "$@"
