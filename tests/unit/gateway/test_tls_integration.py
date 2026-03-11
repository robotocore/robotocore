"""Semantic integration tests for TLS support.

Tests SSL context creation, the management endpoint, and TLS info reporting.
"""

import ssl
from pathlib import Path
from unittest import mock

from starlette.testclient import TestClient

from robotocore.gateway.tls import (
    TLSConfig,
    create_ssl_context,
    generate_self_signed_cert,
)


class TestSSLContextCreation:
    """Test SSL context creation from certificates."""

    def test_ssl_context_with_self_signed_cert(self, tmp_path: Path) -> None:
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        ctx = create_ssl_context(cert_path, key_path)

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.protocol == ssl.PROTOCOL_TLS_SERVER

    def test_ssl_context_with_custom_cert(self, tmp_path: Path) -> None:
        # Generate a cert to act as "custom"
        cert_path = tmp_path / "custom.crt"
        key_path = tmp_path / "custom.key"
        generate_self_signed_cert(cert_path, key_path)

        ctx = create_ssl_context(cert_path, key_path)

        assert isinstance(ctx, ssl.SSLContext)


class TestTLSInfoEndpoint:
    """Test the /_robotocore/tls/info management endpoint."""

    def test_tls_info_enabled(self, tmp_path: Path) -> None:
        """TLS info endpoint returns cert details when enabled."""
        from robotocore.gateway.app import app

        # Generate a cert
        cert_path = tmp_path / "certs" / "server.crt"
        key_path = tmp_path / "certs" / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        config = TLSConfig(enabled=True, cert_dir=tmp_path / "certs")

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", cert_path),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_robotocore/tls/info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is True
        assert "Robotocore Local CA" in data["certificate"]["subject"]
        assert "localhost" in data["certificate"]["sans"]
        assert data["custom_certificate"] is False

    def test_tls_info_disabled(self) -> None:
        """TLS info endpoint reports disabled when HTTPS_DISABLED=1."""
        from robotocore.gateway.app import app

        config = TLSConfig(enabled=False)

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", None),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_robotocore/tls/info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is False
        assert data["certificate"] is None

    def test_tls_info_custom_cert(self, tmp_path: Path) -> None:
        """TLS info endpoint indicates custom certificate."""
        from robotocore.gateway.app import app

        cert_path = tmp_path / "custom.crt"
        key_path = tmp_path / "custom.key"
        generate_self_signed_cert(cert_path, key_path)

        config = TLSConfig(
            enabled=True,
            custom_cert_path=str(cert_path),
            custom_key_path=str(key_path),
        )

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", cert_path),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_robotocore/tls/info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is True
        assert data["custom_certificate"] is True
