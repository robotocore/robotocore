#!/usr/bin/env bash
# Fidelity parity — run full or quick-check. Env-aware: works from this repo.
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-full}" in
  full|run) exec uv run python scripts/harness/parity.py ;;
  next)     exec uv run python scripts/harness/parity.py --next ;;
  *)        exec uv run python scripts/harness/parity.py "$@" ;;
esac
