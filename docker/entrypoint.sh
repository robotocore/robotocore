#!/bin/bash
set -e

exec uv run uvicorn robotocore.gateway.app:app \
    --host "${ROBOTOCORE_HOST:-0.0.0.0}" \
    --port "${ROBOTOCORE_PORT:-4566}" \
    "$@"
