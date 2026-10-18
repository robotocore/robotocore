#!/usr/bin/env bash
# Start the Rust server + moto sidecar (+ Python ground-truth) for parity testing.
# All ports are env-overridable so multiple worktrees can run side by side:
#   PY_PORT (4566) RUST_PORT (4567) SIDECAR_PORT (4568) LOG_PREFIX (parity)
# Writes one PID file per server: /tmp/${LOG_PREFIX}_{py,rust,sidecar}.pid
set -euo pipefail
cd "$(dirname "$0")/.."
SIDECAR_PORT=${SIDECAR_PORT:-4568}
RUST_PORT=${RUST_PORT:-4567}
PY_PORT=${PY_PORT:-4566}
LOG_PREFIX=${LOG_PREFIX:-parity}

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

until_up() { # url retries
    local url="$1" tries="${2:-30}"
    for _ in $(seq 1 $tries); do
        curl -sf --max-time 3 "$url" > /dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

start_sidecar
if ! until_up http://127.0.0.1:$SIDECAR_PORT/_sidecar/health; then
    echo "Sidecar failed; see /tmp/${LOG_PREFIX}_sidecar.log"; exit 1
fi

if [ -f /tmp/${LOG_PREFIX}_py.pid ]; then kill "$(cat /tmp/${LOG_PREFIX}_py.pid)" 2>/dev/null || true; fi
curl -sf --max-time 2 http://127.0.0.1:$PY_PORT/_robotocore/health > /dev/null 2>&1 || start_py
if ! until_up http://127.0.0.1:$PY_PORT/_robotocore/health; then
    start_py
    until_up http://127.0.0.1:$PY_PORT/_robotocore/health || { echo "Python server failed; see /tmp/${LOG_PREFIX}_py.log"; exit 1; }
fi
echo "Python server up on $PY_PORT"

start_rust
if ! curl -sf --max-time 3 http://127.0.0.1:$RUST_PORT/_robotocore/health > /dev/null 2>&1; then
    until_up http://127.0.0.1:$RUST_PORT/_robotocore/health 20 || { echo "Rust server failed; see /tmp/${LOG_PREFIX}_rust.log"; exit 1; }
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
