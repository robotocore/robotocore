"""Robotocore entrypoint."""

import os

import uvicorn


def main() -> None:
    host = os.environ.get("ROBOTOCORE_HOST", "127.0.0.1")
    port = int(os.environ.get("ROBOTOCORE_PORT", "4566"))
    debug = os.environ.get("ROBOTOCORE_DEBUG", "0") == "1"

    uvicorn.run(
        "robotocore.gateway.app:app",
        host=host,
        port=port,
        reload=debug,
        log_level="debug" if debug else "info",
    )


if __name__ == "__main__":
    main()
