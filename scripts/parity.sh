#!/usr/bin/env bash
# Fidelity parity — run full or quick-check. Env-aware: works from this repo.
# Default ("full") auto-picks the mode inside parity.py: dual while the python
# ground-truth server is up, rust-only (baseline expectations) once it is gone.
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-full}" in
  full|run)  exec uv run python scripts/harness/parity.py ;;
  rust-only) exec uv run python scripts/harness/parity.py --rust-only ;;
  next)      exec uv run python scripts/harness/parity.py --next ;;
  *)         exec uv run python scripts/harness/parity.py "$@" ;;
esac
