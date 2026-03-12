"""Robotocore entrypoint."""

import logging
import os
import threading

import uvicorn

from robotocore.observability.banner import print_banner
from robotocore.observability.hooks import run_init_hooks
from robotocore.observability.logging import setup_logging

logger = logging.getLogger(__name__)


def _start_https_server(host: str, debug: bool) -> threading.Thread | None:
    """Optionally start a second uvicorn instance serving HTTPS.

    Supports two configuration modes:

    1. New-style env vars (preferred):
       - ROBOTOCORE_TLS=1           — auto-generate self-signed cert
       - ROBOTOCORE_TLS_CERT/KEY    — explicit cert/key paths
       - ROBOTOCORE_TLS_PORT        — HTTPS port (default 4567)

    2. Legacy env vars (gateway.tls):
       - HTTPS_DISABLED=1           — disable HTTPS
       - CUSTOM_SSL_CERT_PATH/KEY   — explicit cert/key paths
       - ROBOTOCORE_HTTPS_PORT      — HTTPS port (default 443)

    New-style takes priority when any ROBOTOCORE_TLS* var is set.

    Returns the thread if started, or None if TLS is disabled.
    """
    from robotocore.tls import get_tls_config

    tls_cert, tls_key, tls_port = get_tls_config()

    # If the new-style env vars didn't yield a config, fall back to legacy
    if tls_cert is None:
        from robotocore.gateway.tls import TLSConfig, ensure_certificate

        config = TLSConfig.from_env()
        if not config.enabled:
            logger.info("HTTPS disabled")
            return None

        # Publish TLS config to app module so the /_robotocore/tls/info endpoint works
        import robotocore.gateway.app as app_module

        app_module._tls_config = config

        cert_path, key_path = ensure_certificate(config)
        app_module._tls_cert_path = cert_path
        tls_cert = str(cert_path)
        tls_key = str(key_path)
        tls_port = config.https_port

    logger.info("Starting HTTPS server on %s:%d", host, tls_port)

    uv_config = uvicorn.Config(
        "robotocore.gateway.app:app",
        host=host,
        port=tls_port,
        ssl_certfile=tls_cert,
        ssl_keyfile=tls_key,
        log_level="debug" if debug else "info",
    )
    server = uvicorn.Server(uv_config)

    thread = threading.Thread(target=server.run, name="https-server", daemon=True)
    thread.start()
    return thread


def main() -> None:
    # Load configuration profiles before reading any other config
    from robotocore.config import load_config

    load_config()

    host = os.environ.get("ROBOTOCORE_HOST", "127.0.0.1")
    port = int(os.environ.get("ROBOTOCORE_PORT", "4566"))
    debug = os.environ.get("ROBOTOCORE_DEBUG", "0") == "1"

    setup_logging()
    run_init_hooks("boot")
    print_banner(host=host, port=port)

    # Start built-in DNS server (resolves *.amazonaws.com locally)
    from robotocore.dns.server import start_dns_server

    start_dns_server()

    # Start HTTPS server in background thread (if enabled)
    _start_https_server(host, debug)

    # Start HTTP server (blocking)
    uvicorn.run(
        "robotocore.gateway.app:app",
        host=host,
        port=port,
        reload=debug,
        log_level="debug" if debug else "info",
    )


if __name__ == "__main__":
    main()
