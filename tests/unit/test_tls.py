"""Tests for robotocore.tls — self-signed cert generation and TLS config parsing."""

import os
import subprocess
from pathlib import Path
from unittest import mock

import pytest

from robotocore.tls import DEFAULT_TLS_PORT, generate_self_signed, get_tls_config


class TestGenerateSelfSigned:
    """Tests for the self-signed certificate generator."""

    def test_generates_cert_and_key_files(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"

        result_cert, result_key = generate_self_signed(cert_path=cert_path, key_path=key_path)

        assert Path(result_cert).exists()
        assert Path(result_key).exists()
        assert result_cert == str(cert_path)
        assert result_key == str(key_path)

    def test_cert_is_valid_pem(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"

        generate_self_signed(cert_path=cert_path, key_path=key_path)

        cert_data = cert_path.read_text()
        assert "-----BEGIN CERTIFICATE-----" in cert_data
        assert "-----END CERTIFICATE-----" in cert_data

    def test_key_is_valid_pem(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"

        generate_self_signed(cert_path=cert_path, key_path=key_path)

        key_data = key_path.read_text()
        assert "-----BEGIN PRIVATE KEY-----" in key_data
        assert "-----END PRIVATE KEY-----" in key_data

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "deep" / "nested" / "cert.pem"
        key_path = tmp_path / "deep" / "nested" / "key.pem"

        generate_self_signed(cert_path=cert_path, key_path=key_path)

        assert cert_path.exists()
        assert key_path.exists()

    def test_generates_to_temp_dir_when_no_paths(self) -> None:
        cert, key = generate_self_signed()

        assert Path(cert).exists()
        assert Path(key).exists()
        assert "robotocore-tls-" in cert

    def test_custom_subject(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"

        generate_self_signed(cert_path=cert_path, key_path=key_path, subject="/CN=myhost.local")

        # Verify the subject using openssl
        result = subprocess.run(
            ["openssl", "x509", "-in", str(cert_path), "-subject", "-noout"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "myhost.local" in result.stdout

    def test_raises_on_missing_openssl(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"

        with mock.patch("robotocore.tls.subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(RuntimeError, match="openssl not found"):
                generate_self_signed(cert_path=cert_path, key_path=key_path)

    def test_raises_on_openssl_failure(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"

        with mock.patch(
            "robotocore.tls.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "openssl", stderr="bad args"),
        ):
            with pytest.raises(RuntimeError, match="openssl failed"):
                generate_self_signed(cert_path=cert_path, key_path=key_path)


class TestGetTlsConfig:
    """Tests for TLS config parsing from environment variables."""

    def test_no_env_vars_returns_none(self) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("ROBOTOCORE_TLS")}
        with mock.patch.dict(os.environ, env, clear=True):
            cert, key, port = get_tls_config()

        assert cert is None
        assert key is None
        assert port == DEFAULT_TLS_PORT

    def test_explicit_cert_and_key(self, tmp_path: Path) -> None:
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("cert content")
        key_file.write_text("key content")

        env = {
            "ROBOTOCORE_TLS_CERT": str(cert_file),
            "ROBOTOCORE_TLS_KEY": str(key_file),
        }
        with mock.patch.dict(os.environ, env):
            cert, key, port = get_tls_config()

        assert cert == str(cert_file)
        assert key == str(key_file)
        assert port == DEFAULT_TLS_PORT

    def test_explicit_cert_and_key_with_custom_port(self, tmp_path: Path) -> None:
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("cert")
        key_file.write_text("key")

        env = {
            "ROBOTOCORE_TLS_CERT": str(cert_file),
            "ROBOTOCORE_TLS_KEY": str(key_file),
            "ROBOTOCORE_TLS_PORT": "8443",
        }
        with mock.patch.dict(os.environ, env):
            cert, key, port = get_tls_config()

        assert port == 8443

    def test_cert_without_key_raises(self, tmp_path: Path) -> None:
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("cert")

        env = {"ROBOTOCORE_TLS_CERT": str(cert_file)}
        # Clear any existing ROBOTOCORE_TLS_KEY
        cleared = {k: v for k, v in os.environ.items() if k != "ROBOTOCORE_TLS_KEY"}
        cleared.update(env)
        with mock.patch.dict(os.environ, cleared, clear=True):
            with pytest.raises(ValueError, match="ROBOTOCORE_TLS_KEY is not"):
                get_tls_config()

    def test_key_without_cert_raises(self, tmp_path: Path) -> None:
        key_file = tmp_path / "key.pem"
        key_file.write_text("key")

        env = {"ROBOTOCORE_TLS_KEY": str(key_file)}
        cleared = {k: v for k, v in os.environ.items() if k != "ROBOTOCORE_TLS_CERT"}
        cleared.update(env)
        with mock.patch.dict(os.environ, cleared, clear=True):
            with pytest.raises(ValueError, match="ROBOTOCORE_TLS_CERT is not"):
                get_tls_config()

    def test_missing_cert_file_raises(self, tmp_path: Path) -> None:
        env = {
            "ROBOTOCORE_TLS_CERT": str(tmp_path / "nonexistent.pem"),
            "ROBOTOCORE_TLS_KEY": str(tmp_path / "key.pem"),
        }
        with mock.patch.dict(os.environ, env):
            with pytest.raises(FileNotFoundError, match="certificate not found"):
                get_tls_config()

    def test_missing_key_file_raises(self, tmp_path: Path) -> None:
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("cert")

        env = {
            "ROBOTOCORE_TLS_CERT": str(cert_file),
            "ROBOTOCORE_TLS_KEY": str(tmp_path / "nonexistent.pem"),
        }
        with mock.patch.dict(os.environ, env):
            with pytest.raises(FileNotFoundError, match="key not found"):
                get_tls_config()

    def test_auto_tls_generates_certs(self) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("ROBOTOCORE_TLS")}
        env["ROBOTOCORE_TLS"] = "1"
        with mock.patch.dict(os.environ, env, clear=True):
            cert, key, port = get_tls_config()

        assert cert is not None
        assert key is not None
        assert Path(cert).exists()
        assert Path(key).exists()
        assert port == DEFAULT_TLS_PORT

    def test_auto_tls_not_triggered_by_other_values(self) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("ROBOTOCORE_TLS")}
        env["ROBOTOCORE_TLS"] = "yes"
        with mock.patch.dict(os.environ, env, clear=True):
            cert, key, port = get_tls_config()

        assert cert is None
        assert key is None

    def test_explicit_cert_takes_priority_over_auto(self, tmp_path: Path) -> None:
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("cert")
        key_file.write_text("key")

        env = {
            "ROBOTOCORE_TLS": "1",
            "ROBOTOCORE_TLS_CERT": str(cert_file),
            "ROBOTOCORE_TLS_KEY": str(key_file),
        }
        with mock.patch.dict(os.environ, env):
            cert, key, port = get_tls_config()

        # Should use explicit paths, not auto-generated
        assert cert == str(cert_file)
        assert key == str(key_file)
