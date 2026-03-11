"""Robotocore entrypoint."""

import os

import uvicorn

from robotocore.observability.banner import print_banner
from robotocore.observability.hooks import run_init_hooks
from robotocore.observability.logging import setup_logging


def main() -> None:
    host = os.environ.get("ROBOTOCORE_HOST", "127.0.0.1")
    port = int(os.environ.get("ROBOTOCORE_PORT", "4566"))
    debug = os.environ.get("ROBOTOCORE_DEBUG", "0") == "1"

    setup_logging()
    run_init_hooks("boot")
    print_banner(host=host, port=port)

    # Start built-in DNS server (resolves *.amazonaws.com locally)
    from robotocore.dns.server import start_dns_server

    start_dns_server()

    uvicorn.run(
        "robotocore.gateway.app:app",
        host=host,
        port=port,
        reload=debug,
        log_level="debug" if debug else "info",
    )


if __name__ == "__main__":
    main()
