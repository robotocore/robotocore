#!/usr/bin/env bash
# Local replication of the live-server subset of CI (.github/workflows/ci.yml):
# lint, the three unit splits, the integration suite, the compatibility
# shards, the cross-service suite, and parity — the live-server ones against
# a freshly started robotocore on a port nobody else owns. (CI also runs
# iac/terraform, cdk, pulumi, shape-regression and the security audits,
# which this script does not cover yet.)
#
# Why: bare `uv run pytest tests/` is not a meaningful local command — the
# compatibility suites are only meaningful against a robotocore whose state
# starts empty (they populate the API as they run). Pointing a shard at some
# other long-running localhost:4566 silently produces phantom failures.
#
# Usage:
#   scripts/local-ci.sh [stage ...]    # default: every stage sequentially
#   scripts/local-ci.sh lint unit compat
# Stages: lint, unit, integration, compat, cross-service, parity
#
# The compat / cross-service / parity stages each boot their own fresh server
# (CI parity: HTTPS and DNS disabled, plain HTTP) and stop it afterwards. Set
# LOCAL_CI_JOBS=N for parallel pytest workers (default 4; compat shards 8).

set -uo pipefail

cd "$(dirname "$0")/.."

stages=("$@")
[[ ${#stages[@]} -eq 0 ]] && stages=(lint unit integration compat cross-service parity)

validate_stages() {
  for stage in "$@"; do
    case "$stage" in
      lint|unit|integration|compat|cross-service|parity) true ;;
      *) echo "unknown stage: $stage" >&2; echo "stages: lint unit integration compat cross-service parity" >&2; return 1 ;;
    esac
  done
}
if ! validate_stages "${stages[@]}"; then
  exit 2
fi

results=()
exit_code=0

note_result() { # note_result <PASS|FAIL> <name> <secs>
  results+=("$1 $2 ($3s)")
  [[ $1 == FAIL ]] && exit_code=1
}

free_port() {
  python3 - <<'EOF'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
EOF
}

server_pid=""
start_robotocore() {
  # $1 = port. Boots a fresh robotocore and waits until /_robotocore/health is 200.
  local port=$1
  ROBOTOCORE_PORT="$port" HTTPS_DISABLED=1 DNS_DISABLED=1 \
    uv run python -m robotocore.main > .local-ci-server.log 2>&1 &
  server_pid=$!
  for _ in $(seq 1 60); do
    curl -sf "http://127.0.0.1:${port}/_robotocore/health" > /dev/null 2>&1 && return 0
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "robotocore exited while starting:" >&2; cat .local-ci-server.log >&2; exit 1
    fi
    sleep 1
  done
  echo "server on :${port} did not become healthy in 60s" >&2
  cat .local-ci-server.log >&2
  stop_all  # unreachable means something failed; keep the trap contract clean
  exit 1
}

stop_current_server() {
  if [[ -n "${server_pid:-}" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
    server_pid=""
  fi
}
trap stop_current_server EXIT

timed() { # timed <label> <cmd...>
  local label=$1; shift
  echo "── ${label}"
  local t0=$SECONDS
  if "$@"; then
    note_result PASS "$label" $((SECONDS - t0))
  else
    note_result FAIL "$label" $((SECONDS - t0))
  fi
}

has_stage() { [[ " ${stages[*]} " == *" $1 "* ]]; }

# ---- lint ------------------------------------------------------------------
if has_stage lint; then
  timed "lint: ruff" uv run ruff check src/ tests/ scripts/
  timed "lint: ruff format" uv run ruff format --check src/ tests/ scripts/
  timed "lint: mypy" uv run mypy src/robotocore/ --ignore-missing-imports
fi

# ---- unit + integration (serverless; fixtures boot in-process servers) -----
if has_stage unit; then
  timed "unit: gateway" uv run pytest \
    tests/unit/gateway/ tests/unit/protocols/ tests/unit/providers/ \
    tests/unit/test_handler_chain_unit.py tests/unit/test_iam_middleware.py \
    -n"${LOCAL_CI_JOBS:-4}" -q
  timed "unit: services" uv run pytest tests/unit/services/ -q
  timed "unit: infra" uv run pytest tests/unit/ \
    --ignore=tests/unit/gateway/ --ignore=tests/unit/protocols/ \
    --ignore=tests/unit/providers/ --ignore=tests/unit/services/ \
    --ignore=tests/unit/test_handler_chain_unit.py \
    --ignore=tests/unit/test_iam_middleware.py \
    -n"${LOCAL_CI_JOBS:-4}" -q
fi

if has_stage integration; then
  timed "integration" uv run pytest tests/integration/ -q
fi

# ---- compatibility shards (one fresh server, three shards) -----------------
if has_stage compat; then
  compat_port=$(free_port)
  echo "starting fresh robotocore on :${compat_port} for compat shards"
  start_robotocore "$compat_port"
  compat_env=(env "ENDPOINT_URL=http://127.0.0.1:${compat_port}" uv run pytest)
  shard_jobs=${LOCAL_CI_JOBS:-8}
  # The [a-g] shard patterns must reach the shell UNQUOTED: they are shell
  # globs (pytest does not expand them itself; a quoted pattern collects 0
  # tests and "passes" hollow).
  timed "compat: a-g" "${compat_env[@]}" tests/compatibility/test_[a-g]*.py \
    --ignore=tests/compatibility/test_cross_service_compat.py -n"${shard_jobs}" --dist=loadfile -q
  timed "compat: h-r" "${compat_env[@]}" tests/compatibility/test_[h-r]*.py \
    --ignore=tests/compatibility/test_cross_service_compat.py -n"${shard_jobs}" --dist=loadfile -q
  timed "compat: s-z" "${compat_env[@]}" tests/compatibility/test_[s-z]*.py \
    --ignore=tests/compatibility/test_cross_service_compat.py -n"${shard_jobs}" --dist=loadfile -q
  stop_current_server
fi

if has_stage cross-service; then
  cs_port=$(free_port)
  echo "starting fresh robotocore on :${cs_port} for cross-service"
  start_robotocore "$cs_port"
  timed "compat: cross-service" env "ENDPOINT_URL=http://127.0.0.1:${cs_port}" \
    uv run pytest tests/compatibility/test_cross_service_compat.py \
    tests/compatibility/chaos/test_chaos_engineering_compat.py -q
  stop_current_server
fi

# ---- parity ----------------------------------------------------------------
if has_stage parity; then
  p_port=$(free_port)
  echo "starting fresh robotocore on :${p_port} for parity"
  start_robotocore "$p_port"
  timed "parity" env "ENDPOINT_URL=http://127.0.0.1:${p_port}" uv run pytest tests/parity/ -q
  stop_current_server
fi

echo
for r in "${results[@]}"; do echo "$r"; done
exit "$exit_code"
