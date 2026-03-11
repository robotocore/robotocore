"""Unit tests for TLS certificate management."""

import datetime
import os
import ssl
from pathlib import Path
from unittest import mock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.x509.oid import NameOID

from robotocore.gateway.tls import (
    CERT_VALIDITY_DAYS,
    DEFAULT_CERTS_SUBDIR,
    DEFAULT_HTTPS_PORT,
    DEFAULT_SANS,
    DEFAULT_STATE_DIR,
    RSA_KEY_SIZE,
    TLSConfig,
    _validate_cert_file,
    _validate_key_file,
    create_ssl_context,
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

        # robotocore.cloud is the primary SAN, localstack.cloud is backward-compat alias
        assert "*.localhost.robotocore.cloud" in dns_names
        assert "*.localhost.localstack.cloud" in dns_names
        robotocore_idx = dns_names.index("*.localhost.robotocore.cloud")
        localstack_idx = dns_names.index("*.localhost.localstack.cloud")
        assert robotocore_idx < localstack_idx, (
            "robotocore.cloud SAN must come before localstack.cloud"
        )

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

    def test_cert_is_self_signed(self, tmp_path: Path) -> None:
        """Subject and issuer must be identical for self-signed certs."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        assert cert.subject == cert.issuer

    def test_cert_organization_name(self, tmp_path: Path) -> None:
        """Certificate must include Organization=Robotocore."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        org = cert.subject.get_attributes_for_oid(NameOID.ORGANIZATION_NAME)
        assert len(org) == 1
        assert org[0].value == "Robotocore"

    def test_rsa_key_public_exponent(self, tmp_path: Path) -> None:
        """RSA key should use standard public exponent 65537."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        key = load_pem_private_key(key_path.read_bytes(), password=None)
        assert isinstance(key, rsa.RSAPrivateKey)
        assert key.public_key().public_numbers().e == 65537

    def test_cert_uses_sha256_signature(self, tmp_path: Path) -> None:
        """Certificate must be signed with SHA-256."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        assert isinstance(cert.signature_hash_algorithm, hashes.SHA256)

    def test_cert_pem_format_markers(self, tmp_path: Path) -> None:
        """Generated files must have correct PEM markers."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert_data = cert_path.read_bytes()
        assert b"-----BEGIN CERTIFICATE-----" in cert_data
        assert b"-----END CERTIFICATE-----" in cert_data

        key_data = key_path.read_bytes()
        assert b"-----BEGIN RSA PRIVATE KEY-----" in key_data
        assert b"-----END RSA PRIVATE KEY-----" in key_data

    def test_key_is_not_encrypted(self, tmp_path: Path) -> None:
        """Private key must be unencrypted (no passphrase)."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        # Should load without password -- would raise if encrypted
        key = load_pem_private_key(key_path.read_bytes(), password=None)
        assert key is not None

    def test_cert_public_key_matches_private_key(self, tmp_path: Path) -> None:
        """Certificate's embedded public key must match the generated private key."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        key = load_pem_private_key(key_path.read_bytes(), password=None)

        cert_pub = cert.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        key_pub = key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        assert cert_pub == key_pub

    def test_returns_paths(self, tmp_path: Path) -> None:
        """generate_self_signed_cert returns (cert_path, key_path) tuple."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        result = generate_self_signed_cert(cert_path, key_path)

        assert result == (cert_path, key_path)

    def test_san_extension_not_critical(self, tmp_path: Path) -> None:
        """The SAN extension should be marked as non-critical."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path)

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        assert san_ext.critical is False

    def test_each_generation_gets_unique_serial(self, tmp_path: Path) -> None:
        """Each generated cert should have a unique serial number."""
        cert1 = tmp_path / "a.crt"
        key1 = tmp_path / "a.key"
        cert2 = tmp_path / "b.crt"
        key2 = tmp_path / "b.key"

        generate_self_signed_cert(cert1, key1)
        generate_self_signed_cert(cert2, key2)

        c1 = x509.load_pem_x509_certificate(cert1.read_bytes())
        c2 = x509.load_pem_x509_certificate(cert2.read_bytes())
        assert c1.serial_number != c2.serial_number

    def test_empty_sans_list(self, tmp_path: Path) -> None:
        """Passing an empty list of SANs should produce a cert with no SAN entries."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path, sans=[])

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        dns_names = san_ext.value.get_values_for_type(x509.DNSName)
        assert dns_names == []

    def test_wildcard_sans_handled(self, tmp_path: Path) -> None:
        """Wildcard SANs (*.example.com) should be included as DNS names."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"

        generate_self_signed_cert(cert_path, key_path, sans=["*.example.com", "example.com"])

        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        dns_names = san_ext.value.get_values_for_type(x509.DNSName)
        assert "*.example.com" in dns_names
        assert "example.com" in dns_names


class TestValidateCertFile:
    """Test _validate_cert_file edge cases."""

    def test_valid_cert_passes(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        # Should not raise
        _validate_cert_file(cert_path)

    def test_missing_file_raises_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError, match="Certificate file not found"):
            _validate_cert_file(Path("/nonexistent/cert.pem"))

    def test_non_pem_content_raises_value_error(self, tmp_path: Path) -> None:
        bad_cert = tmp_path / "bad.crt"
        bad_cert.write_text("this is not a certificate")

        with pytest.raises(ValueError, match="Invalid certificate file.*not PEM format"):
            _validate_cert_file(bad_cert)

    def test_empty_file_raises_value_error(self, tmp_path: Path) -> None:
        empty_cert = tmp_path / "empty.crt"
        empty_cert.write_bytes(b"")

        with pytest.raises(ValueError, match="Invalid certificate file"):
            _validate_cert_file(empty_cert)

    def test_key_file_as_cert_raises_value_error(self, tmp_path: Path) -> None:
        """A PEM key file should fail cert validation (no BEGIN CERTIFICATE marker)."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        with pytest.raises(ValueError, match="Invalid certificate file"):
            _validate_cert_file(key_path)


class TestValidateKeyFile:
    """Test _validate_key_file edge cases."""

    def test_valid_key_passes(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        # Should not raise
        _validate_key_file(key_path)

    def test_missing_file_raises_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError, match="Key file not found"):
            _validate_key_file(Path("/nonexistent/key.pem"))

    def test_non_pem_content_raises_value_error(self, tmp_path: Path) -> None:
        bad_key = tmp_path / "bad.key"
        bad_key.write_text("this is not a key")

        with pytest.raises(ValueError, match="Invalid key file.*not PEM format"):
            _validate_key_file(bad_key)

    def test_empty_file_raises_value_error(self, tmp_path: Path) -> None:
        empty_key = tmp_path / "empty.key"
        empty_key.write_bytes(b"")

        with pytest.raises(ValueError, match="Invalid key file"):
            _validate_key_file(empty_key)

    def test_cert_file_as_key_raises_value_error(self, tmp_path: Path) -> None:
        """A PEM cert file should fail key validation (no PRIVATE KEY marker)."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        with pytest.raises(ValueError, match="Invalid key file"):
            _validate_key_file(cert_path)

    def test_partial_begin_marker_rejected(self, tmp_path: Path) -> None:
        """File with BEGIN but missing PRIVATE KEY should be rejected."""
        bad_key = tmp_path / "partial.key"
        bad_key.write_text("-----BEGIN SOMETHING-----\ndata\n-----END SOMETHING-----")

        with pytest.raises(ValueError, match="Invalid key file"):
            _validate_key_file(bad_key)


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

    def test_custom_key_invalid_format_raises(self, tmp_path: Path) -> None:
        """Valid cert but invalid key should raise ValueError for key."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        bad_key = tmp_path / "bad.key"
        bad_key.write_text("not a key")

        config = TLSConfig(
            custom_cert_path=str(cert_path),
            custom_key_path=str(bad_key),
        )
        with pytest.raises(ValueError, match="Invalid key file"):
            ensure_certificate(config)

    def test_partial_custom_config_cert_only_falls_through(self, tmp_path: Path) -> None:
        """If only custom_cert_path is set but not custom_key_path, auto-generate."""
        config = TLSConfig(
            custom_cert_path="/some/cert.pem",
            custom_key_path=None,
            cert_dir=tmp_path / "certs",
        )

        cert_path, key_path = ensure_certificate(config)
        # Should auto-generate since both must be set for custom
        assert cert_path.exists()
        assert "server.crt" in str(cert_path)

    def test_partial_custom_config_key_only_falls_through(self, tmp_path: Path) -> None:
        """If only custom_key_path is set but not custom_cert_path, auto-generate."""
        config = TLSConfig(
            custom_cert_path=None,
            custom_key_path="/some/key.pem",
            cert_dir=tmp_path / "certs",
        )

        cert_path, key_path = ensure_certificate(config)
        assert cert_path.exists()
        assert "server.crt" in str(cert_path)

    def test_generated_cert_path_uses_config_cert_dir(self, tmp_path: Path) -> None:
        """Auto-generated cert goes into config.cert_dir."""
        custom_dir = tmp_path / "my_certs"
        config = TLSConfig(cert_dir=custom_dir)

        cert_path, key_path = ensure_certificate(config)

        assert cert_path.parent == custom_dir
        assert key_path.parent == custom_dir


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

    def test_https_disabled_zero_means_enabled(self) -> None:
        """HTTPS_DISABLED=0 should keep TLS enabled."""
        with mock.patch.dict(os.environ, {"HTTPS_DISABLED": "0"}, clear=True):
            config = TLSConfig.from_env()

        assert config.enabled is True

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

    def test_redirect_disabled_by_default(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            config = TLSConfig.from_env()

        assert config.redirect_http is False

    def test_redirect_zero_means_disabled(self) -> None:
        with mock.patch.dict(os.environ, {"HTTPS_REDIRECT": "0"}, clear=True):
            config = TLSConfig.from_env()

        assert config.redirect_http is False

    def test_custom_state_dir_affects_cert_dir(self) -> None:
        with mock.patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": "/data"}, clear=True):
            config = TLSConfig.from_env()

        assert config.cert_dir == Path("/data/certs")

    def test_default_state_dir(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            config = TLSConfig.from_env()

        assert config.state_dir == DEFAULT_STATE_DIR
        assert config.cert_dir == Path(DEFAULT_STATE_DIR) / DEFAULT_CERTS_SUBDIR

    def test_empty_custom_cert_path_treated_as_none(self) -> None:
        """Empty CUSTOM_SSL_CERT_PATH should be treated as unset."""
        with mock.patch.dict(os.environ, {"CUSTOM_SSL_CERT_PATH": ""}, clear=True):
            config = TLSConfig.from_env()

        assert config.custom_cert_path is None

    def test_empty_custom_key_path_treated_as_none(self) -> None:
        """Empty CUSTOM_SSL_KEY_PATH should be treated as unset."""
        with mock.patch.dict(os.environ, {"CUSTOM_SSL_KEY_PATH": ""}, clear=True):
            config = TLSConfig.from_env()

        assert config.custom_key_path is None

    def test_all_env_vars_together(self) -> None:
        """All TLS env vars set simultaneously."""
        env = {
            "HTTPS_DISABLED": "0",
            "ROBOTOCORE_HTTPS_PORT": "9443",
            "CUSTOM_SSL_CERT_PATH": "/ssl/cert.pem",
            "CUSTOM_SSL_KEY_PATH": "/ssl/key.pem",
            "HTTPS_REDIRECT": "1",
            "ROBOTOCORE_STATE_DIR": "/my/state",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = TLSConfig.from_env()

        assert config.enabled is True
        assert config.https_port == 9443
        assert config.custom_cert_path == "/ssl/cert.pem"
        assert config.custom_key_path == "/ssl/key.pem"
        assert config.redirect_http is True
        assert config.cert_dir == Path("/my/state/certs")

    def test_dataclass_defaults(self) -> None:
        """TLSConfig() with no args should have sane defaults."""
        config = TLSConfig()

        assert config.enabled is True
        assert config.https_port == DEFAULT_HTTPS_PORT
        assert config.custom_cert_path is None
        assert config.custom_key_path is None
        assert config.redirect_http is False
        assert config.state_dir == DEFAULT_STATE_DIR


class TestCreateSSLContext:
    """Test SSL context creation."""

    def test_creates_valid_context(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        ctx = create_ssl_context(cert_path, key_path)

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.protocol == ssl.PROTOCOL_TLS_SERVER

    def test_context_has_loaded_cert(self, tmp_path: Path) -> None:
        """SSL context should successfully load the cert chain."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        ctx = create_ssl_context(cert_path, key_path)

        # get_ciphers returns non-empty list if context is properly configured
        assert len(ctx.get_ciphers()) > 0

    def test_mismatched_cert_key_raises(self, tmp_path: Path) -> None:
        """Loading a cert with a different key should raise ssl.SSLError."""
        cert1 = tmp_path / "cert1.crt"
        key1 = tmp_path / "key1.key"
        generate_self_signed_cert(cert1, key1)

        cert2 = tmp_path / "cert2.crt"
        key2 = tmp_path / "key2.key"
        generate_self_signed_cert(cert2, key2)

        with pytest.raises(ssl.SSLError):
            create_ssl_context(cert1, key2)

    def test_nonexistent_cert_raises(self, tmp_path: Path) -> None:
        """Missing cert file should raise FileNotFoundError or ssl.SSLError."""
        key_path = tmp_path / "server.key"
        # Create only the key
        cert_path = tmp_path / "real.crt"
        generate_self_signed_cert(cert_path, key_path)

        with pytest.raises((FileNotFoundError, ssl.SSLError)):
            create_ssl_context(tmp_path / "nonexistent.crt", key_path)

    def test_nonexistent_key_raises(self, tmp_path: Path) -> None:
        """Missing key file should raise FileNotFoundError or ssl.SSLError."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "real.key"
        generate_self_signed_cert(cert_path, key_path)

        with pytest.raises((FileNotFoundError, ssl.SSLError)):
            create_ssl_context(cert_path, tmp_path / "nonexistent.key")


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
        assert "*.localhost.robotocore.cloud" in info["sans"]
        assert "*.localhost.localstack.cloud" in info["sans"]
        assert len(info["serial_number"]) > 0

    def test_all_expected_keys_present(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        info = get_cert_info(cert_path)

        expected_keys = {
            "subject",
            "issuer",
            "not_valid_before",
            "not_valid_after",
            "serial_number",
            "sans",
        }
        assert set(info.keys()) == expected_keys

    def test_dates_are_iso_format(self, tmp_path: Path) -> None:
        """Dates should be valid ISO 8601 strings."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        info = get_cert_info(cert_path)

        # Should parse without error
        datetime.datetime.fromisoformat(info["not_valid_before"])
        datetime.datetime.fromisoformat(info["not_valid_after"])

    def test_serial_number_is_string(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        info = get_cert_info(cert_path)

        assert isinstance(info["serial_number"], str)
        # Serial number should be a large integer as string
        assert int(info["serial_number"]) > 0

    def test_custom_sans_in_info(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path, sans=["custom.local"])

        info = get_cert_info(cert_path)

        assert info["sans"] == ["custom.local"]

    def test_cert_without_san_extension(self, tmp_path: Path) -> None:
        """A cert with no SAN extension should return empty sans list."""
        # Build a cert manually without SAN extension
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, "No SAN Cert"),
            ]
        )
        now = datetime.datetime.now(datetime.UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=1))
            .sign(key, hashes.SHA256())
        )

        cert_path = tmp_path / "no_san.crt"
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

        info = get_cert_info(cert_path)
        assert info["sans"] == []
        assert "No SAN Cert" in info["subject"]

    def test_subject_rfc4514_format(self, tmp_path: Path) -> None:
        """Subject should be in RFC 4514 format."""
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        info = get_cert_info(cert_path)

        # RFC 4514 uses comma-separated key=value pairs
        assert "CN=Robotocore Local CA" in info["subject"]
        assert "O=Robotocore" in info["subject"]


class TestConstants:
    """Verify module-level constants have expected values."""

    def test_default_https_port(self) -> None:
        assert DEFAULT_HTTPS_PORT == 443

    def test_cert_validity_days(self) -> None:
        assert CERT_VALIDITY_DAYS == 365

    def test_rsa_key_size(self) -> None:
        assert RSA_KEY_SIZE == 2048

    def test_default_sans_includes_localhost(self) -> None:
        assert "localhost" in DEFAULT_SANS

    def test_default_sans_includes_wildcard_localhost(self) -> None:
        assert "*.localhost" in DEFAULT_SANS

    def test_default_sans_includes_amazonaws(self) -> None:
        assert "*.amazonaws.com" in DEFAULT_SANS

    def test_default_sans_includes_robotocore_cloud(self) -> None:
        assert "*.localhost.robotocore.cloud" in DEFAULT_SANS

    def test_default_sans_includes_localstack_cloud(self) -> None:
        assert "*.localhost.localstack.cloud" in DEFAULT_SANS

    def test_default_state_dir(self) -> None:
        assert DEFAULT_STATE_DIR == "/tmp/robotocore"

    def test_default_certs_subdir(self) -> None:
        assert DEFAULT_CERTS_SUBDIR == "certs"


class TestStartHttpsServer:
    """Test the _start_https_server function from main.py."""

    def test_returns_none_when_disabled(self) -> None:
        """When HTTPS_DISABLED=1, _start_https_server returns None."""
        from robotocore.main import _start_https_server

        with mock.patch.dict(os.environ, {"HTTPS_DISABLED": "1"}, clear=False):
            result = _start_https_server("127.0.0.1", debug=False)

        assert result is None

    def test_starts_thread_when_enabled(self, tmp_path: Path) -> None:
        """When TLS enabled, _start_https_server returns a daemon thread."""
        import threading

        from robotocore.main import _start_https_server

        # Pre-generate certs so ensure_certificate finds them
        cert_dir = tmp_path / "certs"
        cert_dir.mkdir()
        generate_self_signed_cert(cert_dir / "server.crt", cert_dir / "server.key")

        env = {"ROBOTOCORE_STATE_DIR": str(tmp_path), "ROBOTOCORE_HTTPS_PORT": "19443"}

        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch("robotocore.main.uvicorn.Server") as mock_server_cls,
            mock.patch("robotocore.main.uvicorn.Config") as mock_config_cls,
        ):
            # Make .run() a no-op so the thread doesn't actually start a server
            mock_server_cls.return_value.run = mock.MagicMock()

            thread = _start_https_server("127.0.0.1", debug=False)

        assert thread is not None
        assert isinstance(thread, threading.Thread)
        assert thread.daemon is True
        assert thread.name == "https-server"

        # Verify uvicorn.Config was called with ssl params
        config_call = mock_config_cls.call_args
        assert config_call.kwargs["ssl_certfile"] is not None
        assert config_call.kwargs["ssl_keyfile"] is not None
        assert config_call.kwargs["port"] == 19443
