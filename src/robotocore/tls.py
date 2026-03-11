"""Self-signed TLS certificate generation for local development.

Uses subprocess + openssl to avoid adding cryptography as a dependency.

Environment variables:
    ROBOTOCORE_TLS=1         — auto-generate self-signed cert and enable TLS
    ROBOTOCORE_TLS_CERT      — path to PEM certificate file
    ROBOTOCORE_TLS_KEY       — path to PEM private key file
    ROBOTOCORE_TLS_PORT      — HTTPS port (default: 4567)
"""

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TLS_PORT = 4567


def generate_self_signed(
    cert_path: Path | None = None,
    key_path: Path | None = None,
    subject: str = "/CN=localhost",
    days: int = 365,
) -> tuple[str, str]:
    """Generate a self-signed certificate and private key using openssl.

    If cert_path/key_path are not provided, files are created in a temp directory.

    Returns:
        Tuple of (cert_path, key_path) as strings.

    Raises:
        RuntimeError: If openssl is not available or fails.
    """
    if cert_path is None or key_path is None:
        tmp_dir = Path(tempfile.mkdtemp(prefix="robotocore-tls-"))
        cert_path = cert_path or tmp_dir / "cert.pem"
        key_path = key_path or tmp_dir / "key.pem"

    cert_path = Path(cert_path)
    key_path = Path(key_path)
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        str(key_path),
        "-out",
        str(cert_path),
        "-days",
        str(days),
        "-nodes",
        "-subj",
        subject,
    ]

    try:
        subprocess.run(  # noqa: S603
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "openssl not found on PATH. Install OpenSSL to use auto-generated TLS certificates."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"openssl failed (exit {exc.returncode}): {exc.stderr}") from exc

    if not cert_path.exists() or not key_path.exists():
        raise RuntimeError(
            f"openssl ran successfully but certificate files were not created: "
            f"cert={cert_path}, key={key_path}"
        )

    logger.warning(
        "Auto-generated self-signed TLS certificate for development only: %s",
        cert_path,
    )
    return str(cert_path), str(key_path)


def get_tls_config() -> tuple[str | None, str | None, int]:
    """Parse TLS configuration from environment variables.

    Returns:
        Tuple of (cert_path, key_path, tls_port).
        cert_path and key_path are None if TLS is not configured.

    Raises:
        ValueError: If only one of cert/key is provided.
    """
    import os

    tls_cert = os.environ.get("ROBOTOCORE_TLS_CERT")
    tls_key = os.environ.get("ROBOTOCORE_TLS_KEY")
    auto_tls = os.environ.get("ROBOTOCORE_TLS", "").strip() == "1"
    tls_port = int(os.environ.get("ROBOTOCORE_TLS_PORT", str(DEFAULT_TLS_PORT)))

    # Explicit cert/key paths take priority
    if tls_cert and tls_key:
        cert_path = Path(tls_cert)
        key_path = Path(tls_key)
        if not cert_path.exists():
            raise FileNotFoundError(f"TLS certificate not found: {tls_cert}")
        if not key_path.exists():
            raise FileNotFoundError(f"TLS key not found: {tls_key}")
        return str(cert_path), str(key_path), tls_port

    # One without the other is an error
    if tls_cert and not tls_key:
        raise ValueError(
            "ROBOTOCORE_TLS_CERT is set but ROBOTOCORE_TLS_KEY is not. Both are required."
        )
    if tls_key and not tls_cert:
        raise ValueError(
            "ROBOTOCORE_TLS_KEY is set but ROBOTOCORE_TLS_CERT is not. Both are required."
        )

    # Auto-generate self-signed cert
    if auto_tls:
        cert, key = generate_self_signed()
        return cert, key, tls_port

    return None, None, tls_port
