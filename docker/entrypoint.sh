#!/bin/bash
set -e

echo ""
echo " ____       _           _                          "
echo "|  _ \ ___ | |__   ___ | |_ ___   ___ ___  _ __ ___  "
echo "| |_) / _ \| '_ \ / _ \| __/ _ \ / __/ _ \| '__/ _ \ "
echo "|  _ < (_) | |_) | (_) | || (_) | (_| (_) | | |  __/ "
echo "|_| \_\___/|_.__/ \___/ \__\___/ \___\___/|_|  \___| "
echo ""
echo "Robotocore v${ROBOTOCORE_VERSION:-1.0.0} — Free AWS Emulator"
echo "Port: ${ROBOTOCORE_PORT:-4566} | Host: ${ROBOTOCORE_HOST:-0.0.0.0}"
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

# Start the server (use venv python directly — uv is not in the runtime image)
exec /app/.venv/bin/python -m uvicorn robotocore.gateway.app:app \
    --host "${ROBOTOCORE_HOST:-0.0.0.0}" \
    --port "${ROBOTOCORE_PORT:-4566}" \
    "$@"
