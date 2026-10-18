#!/usr/bin/env bash
# Start the Rust server + moto sidecar (+ Python ground-truth) for parity testing.
# All ports are env-overridable so multiple worktrees can run side by side:
#   PY_PORT (4566) RUST_PORT (4567) SIDECAR_PORT (4568) LOG_PREFIX (parity)
# Tracking: one PID file per server: /tmp/${LOG_PREFIX}_{py,rust,sidecar}.pid
# Idempotent per LOG_PREFIX: kills the previously recorded PIDs before starting;
# never touches other LOG_PREFIXes' processes (no pkills by name).
set -euo pipefail
cd "$(dirname "$0")/.."
SIDECAR_PORT=${SIDECAR_PORT:-4568}
RUST_PORT=${RUST_PORT:-4567}
PY_PORT=${PY_PORT:-4566}
LOG_PREFIX=${LOG_PREFIX:-parity}

pid_alive() { # true when the recorded PID is a live process
    local pid="$1"
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

kill_pid() { # stop a recorded process if alive
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
}

start_py() {
    nohup env ROBOTOCORE_PORT=$PY_PORT DNS_DISABLED=1 uv run python -m robotocore.main \
        > /tmp/${LOG_PREFIX}_py.log 2>&1 &
    echo $! > /tmp/${LOG_PREFIX}_py.pid
    echo "Python server PID: $(cat /tmp/${LOG_PREFIX}_py.pid)"
}

start_sidecar() {
    nohup uv run python scripts/moto_sidecar.py --port $SIDECAR_PORT \
        > /tmp/${LOG_PREFIX}_sidecar.log 2>&1 &
    echo $! > /tmp/${LOG_PREFIX}_sidecar.pid
    echo "Sidecar PID: $(cat /tmp/${LOG_PREFIX}_sidecar.pid)"
}

start_rust() {
    cargo build --release 2>&1 | tail -1
    nohup target/release/robotocore-rust \
        --port $RUST_PORT \
        --moto-url http://127.0.0.1:$SIDECAR_PORT \
        > /tmp/${LOG_PREFIX}_rust.log 2>&1 &
    echo $! > /tmp/${LOG_PREFIX}_rust.pid
    echo "Rust server PID: $(cat /tmp/${LOG_PREFIX}_rust.pid)"
}

incumbent_pid() { # PID listening on PORT whose cmdline matches needle (regex)
    local port="$1" needle="$2"
    ss -ltnp "sport = :$port" 2>/dev/null \
        | grep -oP "pid=\K[0-9]+" \
        | while read -r p; do
            if [ -r "/proc/$p/cmdline" ] && grep -aqe "$needle" "/proc/$p/cmdline" 2>/dev/null; then
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

# --- Sidecar: stop recorded/first-generation incumbent, then (re)start --------
SIDECAR_OLD=$(cat /tmp/${LOG_PREFIX}_sidecar.pid 2>/dev/null || true)
if ! pid_alive "$SIDECAR_OLD"; then SIDECAR_OLD=""; fi
# Also catch an old sidecar still alive on OUR port (no PID record for it)
if [ -z "$SIDECAR_OLD" ]; then
    SIDECAR_OLD=$(incumbent_pid "$SIDECAR_PORT" "moto_sidecar.py --port $SIDECAR_PORT" || true)
fi
kill_pid "$SIDECAR_OLD"
start_sidecar
if ! until_up http://127.0.0.1:$SIDECAR_PORT/_sidecar/health; then
    echo "Sidecar failed; see /tmp/${LOG_PREFIX}_sidecar.log"; exit 1
fi
echo "Sidecar up on $SIDECAR_PORT"

# --- Python ground truth ------------------------------------------------------
if [ -f /tmp/${LOG_PREFIX}_py.pid ]; then
    PY_OLD=$(cat /tmp/${LOG_PREFIX}_py.pid 2>/dev/null || true)
    if ! pid_alive "$PY_OLD"; then PY_OLD=""; fi
fi
[ -z "${PY_OLD:-}" ] && PY_OLD=$(incumbent_pid "$PY_PORT" "robotocore.main" || true)
kill_pid "$PY_OLD"
start_py
if ! until_up http://127.0.0.1:$PY_PORT/_robotocore/health; then
    echo "Python server failed; see /tmp/${LOG_PREFIX}_py.log"; exit 1
fi
echo "Python server up on $PY_PORT"

# --- Rust server --------------------------------------------------------------
RUST_OLD=$(cat /tmp/${LOG_PREFIX}_rust.pid 2>/dev/null || true)
if ! pid_alive "$RUST_OLD"; then RUST_OLD=""; fi
[ -z "$RUST_OLD" ] && RUST_OLD=$(incumbent_pid "$RUST_PORT" "robotocore-rust --port $RUST_PORT" || true)
kill_pid "$RUST_OLD"
start_rust
if ! until_up http://127.0.0.1:$RUST_PORT/_robotocore/health 20; then
    echo "Rust server failed; see /tmp/${LOG_PREFIX}_rust.log"; exit 1
fi

# Verify bridge
for i in $(seq 1 30); do
    RESP=$(curl -s -X POST http://127.0.0.1:$RUST_PORT/ --max-time 10 \
        -H 'Content-Type: application/x-www-form-urlencoded' \
        -H 'Authorization: AWS4-HMAC-SHA256 Credential=123456789012/20240101/us-east-1/ec2/aws4_request' \
        -H 'X-Amz-Date: 20240101T000000Z' \
        -d 'Action=DescribeInstances&Version=2016-11-15' 2>/dev/null || echo "")
    if echo "$RESP" | grep -q "DescribeInstancesResponse"; then
        echo "Bridge WORKING"
        exit 0
    fi
    sleep 2
done
echo "Bridge NOT working"
exit 1
