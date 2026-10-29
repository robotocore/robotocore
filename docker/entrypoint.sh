#!/bin/bash
# Local-development entrypoint: prints the banner and execs the rust server.
#
# The published image is distroless (no shell) and runs the binary directly —
# this script is for bare-metal dev runs or any image that mounts a shell.
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROBOTOCORE_BIN:-$ROOT/target/release/robotocore-rust}"
PORT="${ROBOTOCORE_PORT:-4566}"

echo ""
echo " ____       _           _                          "
echo "|  _ \ ___ | |__   ___ | |_ ___   ___ ___  _ __ ___  "
echo "| |_) / _ \| '_ \ / _ \| __/ _ \ / __/ _ \| '__/ _ \ "
echo "|  _ < (_) | |_) | (_) | || (_) | (_| (_) | | |  __/ "
echo "|_| \_\___/|_.__/ \___/ \__\___/ \___\___/|_|  \___| "
echo ""
echo "Robotocore v${ROBOTOCORE_VERSION:-1.0.0} — Free AWS Emulator"
echo "Port: ${PORT} | Host: loopback only (see src/bin/robotocore-rust.rs)"
echo ""

# Create state directory if configured
if [ -n "$ROBOTOCORE_STATE_DIR" ]; then
    mkdir -p "$ROBOTOCORE_STATE_DIR"
    echo "State directory: $ROBOTOCORE_STATE_DIR"
fi

# Run boot hooks
if [ -d /etc/robotocore/init/boot.d ]; then
    for script in /etc/robotocore/init/boot.d/*.sh; do
        [ -f "$script" ] && echo "Running boot hook: $script" && bash "$script"
    done
fi

if [ ! -x "$BIN" ]; then
    echo "Error: rust server binary not found at $BIN." >&2
    echo "Build it first with: cargo build --release" >&2
    exit 1
fi

exec "$BIN" --port "$PORT" "$@"
