#!/usr/bin/env bash
# Start the Rust robotocore server for parity/compat testing.
#
# The port is done: everything runs from the single Rust binary. There is no
# Python server and no moto sidecar anymore — rotocore replaces both.
#
#   RUST_PORT (4566) LOG_PREFIX (parity) RUST_HOST (127.0.0.1)
# Writes one PID file: /tmp/${LOG_PREFIX}_rust.pid
set -euo pipefail
cd "$(dirname "$0")/.."
RUST_PORT=${RUST_PORT:-4566}
RUST_HOST=${RUST_HOST:-127.0.0.1}
LOG_PREFIX=${LOG_PREFIX:-parity}

# The lambda shim imports boto3 through this interpreter (crates/lambda/
# src/runtime.rs reads ROBOTOCORE_PY). Without it, every lambda-driven
# compat test fails on `ModuleNotFoundError: No module named 'boto3'`.
if [ -z "${ROBOTOCORE_PY:-}" ] && [ -x "$PWD/.venv/bin/python" ]; then
    export ROBOTOCORE_PY="$PWD/.venv/bin/python"
fi

incumbent_pid() { # PID listening on PORT whose cmdline matches needle
    local port="$1" needle="$2"
    ss -ltnp "sport = :$port" 2>/dev/null \
        | grep -oP "pid=\K[0-9]+" \
        | while read -r p; do
            if [ -r "/proc/$p/cmdline" ] && tr '\0' ' ' < "/proc/$p/cmdline" | grep -aq "$needle"; then
                echo "$p"
            fi
        done | head -1
}

until_up() { # url retries
    local url="$1" tries="${2:-30}"
    for _ in $(seq 1 $tries); do
        curl -sf --max-time 3 "$url" > /dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

RUST_OLD=$(cat /tmp/${LOG_PREFIX}_rust.pid 2>/dev/null || true)
if [ -n "$RUST_OLD" ] && ! kill -0 "$RUST_OLD" 2>/dev/null; then RUST_OLD=""; fi
[ -z "$RUST_OLD" ] && RUST_OLD=$(incumbent_pid "$RUST_PORT" "robotocore-rust --port $RUST_PORT" || true)
if [ -n "$RUST_OLD" ]; then
    kill "$RUST_OLD" 2>/dev/null || true
    sleep 1
fi

cargo build --release 2>&1 | tail -1
nohup target/release/robotocore-rust \
    --host ${RUST_HOST} \
    --port $RUST_PORT \
    > /tmp/${LOG_PREFIX}_rust.log 2>&1 &
echo $! > /tmp/${LOG_PREFIX}_rust.pid
echo "Rust server PID: $(cat /tmp/${LOG_PREFIX}_rust.pid)"

if ! until_up "http://127.0.0.1:$RUST_PORT/_robotocore/health" 20; then
    echo "Rust server failed; see /tmp/${LOG_PREFIX}_rust.log"
    exit 1
fi

echo "Rust server up on $RUST_PORT (rotocore native, no sidecar)"
