"""Unit tests for TLS certificate management."""

import datetime
import os
from pathlib import Path
from unittest import mock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from robotocore.gateway.tls import (
    CERT_VALIDITY_DAYS,
    DEFAULT_HTTPS_PORT,
    DEFAULT_SANS,
    RSA_KEY_SIZE,
    TLSConfig,
    ensure_certificate,
    generate_self_signed_cert,
    get_cert_info,
)


class TestGenerateSelfSignedCert:
    """Test self-signed certificate generation."""

    def test_generates_valid_cert_and_key(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        assert cert_path.exists()
        assert key_path.exists()

        # Parse cert
        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        assert cert.subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)[0].value == (
            "Robotocore Local CA"
        )

    def test_cert_has_correct_sans(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        dns_names = san_ext.value.get_values_for_type(x509.DNSName)

        for expected in DEFAULT_SANS:
            assert expected in dns_names, f"Missing SAN: {expected}"

    def test_cert_custom_sans(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        custom_sans = ["myhost.local", "*.myhost.local"]

        generate_self_signed_cert(cert_path, key_path, sans=custom_sans)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        dns_names = san_ext.value.get_values_for_type(x509.DNSName)

        assert dns_names == custom_sans

    def test_cert_validity_period(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        now = datetime.datetime.now(datetime.UTC)

        # not_valid_before should be close to now
        assert abs((cert.not_valid_before_utc - now).total_seconds()) < 60

        # not_valid_after should be ~365 days from now
        expected_expiry = now + datetime.timedelta(days=CERT_VALIDITY_DAYS)
        delta = abs((cert.not_valid_after_utc - expected_expiry).total_seconds())
        assert delta < 60

    def test_rsa_key_is_2048_bits(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        key = load_pem_private_key(key_path.read_bytes(), password=None)
        assert isinstance(key, rsa.RSAPrivateKey)
        assert key.key_size == RSA_KEY_SIZE

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "deep" / "nested" / "server.crt"
        key_path = tmp_path / "deep" / "nested" / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        assert cert_path.exists()
        assert key_path.exists()


class TestEnsureCertificate:
    """Test certificate loading and caching."""

    def test_generates_cert_when_none_exists(self, tmp_path: Path) -> None:
        config = TLSConfig(cert_dir=tmp_path / "certs")

        cert_path, key_path = ensure_certificate(config)

        assert cert_path.exists()
        assert key_path.exists()
        assert "server.crt" in str(cert_path)

    def test_caches_cert_on_second_call(self, tmp_path: Path) -> None:
        config = TLSConfig(cert_dir=tmp_path / "certs")

        cert_path1, _ = ensure_certificate(config)
        stat1 = cert_path1.stat()

        cert_path2, _ = ensure_certificate(config)
        stat2 = cert_path2.stat()

        # Same file, same mtime (not regenerated)
        assert cert_path1 == cert_path2
        assert stat1.st_mtime == stat2.st_mtime

    def test_loads_custom_cert(self, tmp_path: Path) -> None:
        # Generate a real cert to use as "custom"
        cert_path = tmp_path / "custom.crt"
        key_path = tmp_path / "custom.key"
        generate_self_signed_cert(cert_path, key_path)

        config = TLSConfig(
            custom_cert_path=str(cert_path),
            custom_key_path=str(key_path),
        )

        loaded_cert, loaded_key = ensure_certificate(config)
        assert loaded_cert == cert_path
        assert loaded_key == key_path

    def test_custom_cert_missing_raises(self) -> None:
        config = TLSConfig(
            custom_cert_path="/nonexistent/cert.pem",
            custom_key_path="/nonexistent/key.pem",
        )
        with pytest.raises(FileNotFoundError):
            ensure_certificate(config)

    def test_custom_cert_invalid_format_raises(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "bad.crt"
        key_path = tmp_path / "bad.key"
        cert_path.write_text("not a certificate")
        key_path.write_text("not a key")

        config = TLSConfig(
            custom_cert_path=str(cert_path),
            custom_key_path=str(key_path),
        )
        with pytest.raises(ValueError, match="Invalid certificate"):
            ensure_certificate(config)


class TestTLSConfig:
    """Test TLS configuration from environment variables."""

    def test_default_values(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            config = TLSConfig.from_env()

        assert config.enabled is True
        assert config.https_port == DEFAULT_HTTPS_PORT
        assert config.custom_cert_path is None
        assert config.custom_key_path is None
        assert config.redirect_http is False

    def test_https_disabled(self) -> None:
        with mock.patch.dict(os.environ, {"HTTPS_DISABLED": "1"}, clear=True):
            config = TLSConfig.from_env()

        assert config.enabled is False

    def test_custom_port(self) -> None:
        with mock.patch.dict(os.environ, {"ROBOTOCORE_HTTPS_PORT": "8443"}, clear=True):
            config = TLSConfig.from_env()

        assert config.https_port == 8443

    def test_custom_cert_paths(self) -> None:
        env = {
            "CUSTOM_SSL_CERT_PATH": "/my/cert.pem",
            "CUSTOM_SSL_KEY_PATH": "/my/key.pem",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = TLSConfig.from_env()

        assert config.custom_cert_path == "/my/cert.pem"
        assert config.custom_key_path == "/my/key.pem"

    def test_redirect_enabled(self) -> None:
        with mock.patch.dict(os.environ, {"HTTPS_REDIRECT": "1"}, clear=True):
            config = TLSConfig.from_env()

        assert config.redirect_http is True

    def test_custom_state_dir_affects_cert_dir(self) -> None:
        with mock.patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": "/data"}, clear=True):
            config = TLSConfig.from_env()

        assert config.cert_dir == Path("/data/certs")


class TestGetCertInfo:
    """Test certificate info extraction."""

    def test_returns_correct_info(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        info = get_cert_info(cert_path)

        assert "Robotocore Local CA" in info["subject"]
        assert "Robotocore" in info["issuer"]
        assert info["not_valid_before"] is not None
        assert info["not_valid_after"] is not None
        assert "localhost" in info["sans"]
        assert "*.amazonaws.com" in info["sans"]
        assert len(info["serial_number"]) > 0
