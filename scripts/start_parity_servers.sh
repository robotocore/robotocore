#!/usr/bin/env bash
# Start the Rust server + moto sidecar (+ Python ground-truth) for parity testing.
# All ports are env-overridable so multiple machines/worktrees can run side by side:
#   PY_PORT (4566)  RUST_PORT (4567)  SIDECAR_PORT (4568)
set -euo pipefail
cd "$(dirname "$0")/.."
SIDECAR_PORT=${SIDECAR_PORT:-4568}
RUST_PORT=${RUST_PORT:-4567}
PY_PORT=${PY_PORT:-4566}
LOG_PREFIX=${LOG_PREFIX:-parity}

pkill -9 -f "moto_sidecar.*--port $SIDECAR_PORT" 2>/dev/null || true
pkill -f "robotocore-rust.*--port $RUST_PORT" 2>/dev/null || true
pkill -f "robotocore.main" 2>/dev/null || true
sleep 1

# Sidecar (moto bridge, detached)
nohup uv run python scripts/moto_sidecar.py --port $SIDECAR_PORT \
    > /tmp/${LOG_PREFIX}_sidecar.log 2>&1 &
echo "Sidecar PID: $!"
for i in $(seq 1 30); do
    curl -sf --max-time 3 http://127.0.0.1:$SIDECAR_PORT/_sidecar/health > /dev/null 2>&1 && break
    sleep 1
done

# Python ground truth server (detached)
nohup env ROBOTOCORE_PORT=$PY_PORT uv run python -m robotocore.main \
    > /tmp/${LOG_PREFIX}_py.log 2>&1 &
echo "Python server PID: $!"
for i in $(seq 1 30); do
    curl -sf --max-time 3 http://127.0.0.1:$PY_PORT/_robotocore/health > /dev/null 2>&1 && break
    sleep 1
done

# Rust server
cargo build --release 2>&1 | tail -1
nohup target/release/robotocore-rust \
    --port $RUST_PORT \
    --moto-url http://127.0.0.1:$SIDECAR_PORT \
    > /tmp/${LOG_PREFIX}_rust.log 2>&1 &
echo "Rust server PID: $!"
for i in $(seq 1 20); do
    curl -sf --max-time 3 http://127.0.0.1:$RUST_PORT/_robotocore/health > /dev/null 2>&1 && break
    sleep 1
done

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
