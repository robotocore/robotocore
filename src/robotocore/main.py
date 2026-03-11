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

    Returns the thread if started, or None if TLS is disabled.
    """
    from robotocore.gateway.tls import TLSConfig, ensure_certificate

    config = TLSConfig.from_env()
    if not config.enabled:
        logger.info("HTTPS disabled (HTTPS_DISABLED=1)")
        return None

    # Publish TLS config to app module so the /_robotocore/tls/info endpoint works
    import robotocore.gateway.app as app_module

    app_module._tls_config = config

    cert_path, key_path = ensure_certificate(config)
    app_module._tls_cert_path = cert_path

    logger.info("Starting HTTPS server on %s:%d", host, config.https_port)

    uv_config = uvicorn.Config(
        "robotocore.gateway.app:app",
        host=host,
        port=config.https_port,
        ssl_certfile=str(cert_path),
        ssl_keyfile=str(key_path),
        log_level="debug" if debug else "info",
    )
    server = uvicorn.Server(uv_config)

    thread = threading.Thread(target=server.run, name="https-server", daemon=True)
    thread.start()
    return thread


def main() -> None:
    host = os.environ.get("ROBOTOCORE_HOST", "127.0.0.1")
    port = int(os.environ.get("ROBOTOCORE_PORT", "4566"))
    debug = os.environ.get("ROBOTOCORE_DEBUG", "0") == "1"

    setup_logging()
    run_init_hooks("boot")
    print_banner(host=host, port=port)

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
