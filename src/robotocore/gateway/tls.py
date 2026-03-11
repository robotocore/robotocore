"""TLS certificate management for HTTPS termination.

Supports:
- Auto-generated self-signed certificates on first boot
- Custom certificates via environment variables
- Certificate caching to avoid regeneration
- SSL context creation for uvicorn
"""

import datetime
import logging
import os
import ssl
from dataclasses import dataclass, field
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

logger = logging.getLogger(__name__)

# Default state directory for generated certs
DEFAULT_STATE_DIR = "/tmp/robotocore"
DEFAULT_CERTS_SUBDIR = "certs"
DEFAULT_HTTPS_PORT = 443
CERT_VALIDITY_DAYS = 365
RSA_KEY_SIZE = 2048

# SANs for self-signed cert
DEFAULT_SANS = [
    "localhost",
    "*.localhost",
    "*.amazonaws.com",
    "*.localhost.localstack.cloud",
]


@dataclass
class TLSConfig:
    """TLS configuration parsed from environment variables."""

    enabled: bool = True
    https_port: int = DEFAULT_HTTPS_PORT
    custom_cert_path: str | None = None
    custom_key_path: str | None = None
    redirect_http: bool = False
    state_dir: str = DEFAULT_STATE_DIR
    cert_dir: Path = field(default_factory=lambda: Path(DEFAULT_STATE_DIR) / DEFAULT_CERTS_SUBDIR)

    @classmethod
    def from_env(cls) -> "TLSConfig":
        """Build TLS config from environment variables."""
        disabled = os.environ.get("HTTPS_DISABLED", "0") == "1"
        port = int(os.environ.get("ROBOTOCORE_HTTPS_PORT", str(DEFAULT_HTTPS_PORT)))
        custom_cert = os.environ.get("CUSTOM_SSL_CERT_PATH") or None
        custom_key = os.environ.get("CUSTOM_SSL_KEY_PATH") or None
        redirect = os.environ.get("HTTPS_REDIRECT", "0") == "1"
        state_dir = os.environ.get("ROBOTOCORE_STATE_DIR", DEFAULT_STATE_DIR)
        cert_dir = Path(state_dir) / DEFAULT_CERTS_SUBDIR

        return cls(
            enabled=not disabled,
            https_port=port,
            custom_cert_path=custom_cert,
            custom_key_path=custom_key,
            redirect_http=redirect,
            state_dir=state_dir,
            cert_dir=cert_dir,
        )


def generate_self_signed_cert(
    cert_path: Path,
    key_path: Path,
    sans: list[str] | None = None,
) -> tuple[Path, Path]:
    """Generate a self-signed certificate and RSA private key.

    Args:
        cert_path: Where to write the PEM certificate.
        key_path: Where to write the PEM private key.
        sans: Subject Alternative Names. Defaults to DEFAULT_SANS.

    Returns:
        Tuple of (cert_path, key_path).
    """
    if sans is None:
        sans = DEFAULT_SANS

    # Generate RSA key
    key = rsa.generate_private_key(public_exponent=65537, key_size=RSA_KEY_SIZE)

    # Build certificate
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "Robotocore Local CA"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Robotocore"),
        ]
    )

    now = datetime.datetime.now(datetime.UTC)
    san_entries: list[x509.GeneralName] = []
    for name in sans:
        if name.startswith("*."):
            san_entries.append(x509.DNSName(name))
        else:
            san_entries.append(x509.DNSName(name))

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=CERT_VALIDITY_DAYS))
        .add_extension(
            x509.SubjectAlternativeName(san_entries),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    # Ensure directory exists
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)

    # Write files
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )

    logger.info("Generated self-signed TLS certificate: %s", cert_path)
    return cert_path, key_path


def ensure_certificate(config: TLSConfig) -> tuple[Path, Path]:
    """Return (cert_path, key_path), generating a self-signed cert if needed.

    If custom cert/key paths are configured and valid, returns those.
    Otherwise, generates (or reuses cached) a self-signed certificate.
    """
    # Custom cert takes priority
    if config.custom_cert_path and config.custom_key_path:
        cert_path = Path(config.custom_cert_path)
        key_path = Path(config.custom_key_path)
        _validate_cert_file(cert_path)
        _validate_key_file(key_path)
        return cert_path, key_path

    # Auto-generate: check cache first
    cert_path = config.cert_dir / "server.crt"
    key_path = config.cert_dir / "server.key"

    if cert_path.exists() and key_path.exists():
        logger.info("Reusing cached TLS certificate: %s", cert_path)
        return cert_path, key_path

    return generate_self_signed_cert(cert_path, key_path)


def _validate_cert_file(path: Path) -> None:
    """Validate that a file looks like a PEM certificate."""
    if not path.exists():
        raise FileNotFoundError(f"Certificate file not found: {path}")
    data = path.read_bytes()
    if b"-----BEGIN CERTIFICATE-----" not in data:
        raise ValueError(f"Invalid certificate file (not PEM format): {path}")


def _validate_key_file(path: Path) -> None:
    """Validate that a file looks like a PEM private key."""
    if not path.exists():
        raise FileNotFoundError(f"Key file not found: {path}")
    data = path.read_bytes()
    if b"-----BEGIN" not in data or b"PRIVATE KEY-----" not in data:
        raise ValueError(f"Invalid key file (not PEM format): {path}")


def create_ssl_context(cert_path: Path, key_path: Path) -> ssl.SSLContext:
    """Create an SSL context for uvicorn from cert and key files."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    return ctx


def get_cert_info(cert_path: Path) -> dict:
    """Read a PEM certificate and return human-readable info."""
    data = cert_path.read_bytes()
    cert = x509.load_pem_x509_certificate(data)

    # Extract SANs
    try:
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        sans = san_ext.value.get_values_for_type(x509.DNSName)
    except x509.ExtensionNotFound:
        sans = []

    return {
        "subject": cert.subject.rfc4514_string(),
        "issuer": cert.issuer.rfc4514_string(),
        "not_valid_before": cert.not_valid_before_utc.isoformat(),
        "not_valid_after": cert.not_valid_after_utc.isoformat(),
        "serial_number": str(cert.serial_number),
        "sans": sans,
    }
