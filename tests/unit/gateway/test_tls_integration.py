"""Semantic integration tests for TLS support.

Tests SSL context creation, the management endpoint, and TLS info reporting.
"""

import ssl
from pathlib import Path
from unittest import mock

from starlette.testclient import TestClient

from robotocore.gateway.tls import (
    DEFAULT_HTTPS_PORT,
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
        assert "*.localhost.robotocore.cloud" in data["certificate"]["sans"]
        assert "*.localhost.localstack.cloud" in data["certificate"]["sans"]
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

    def test_tls_info_includes_https_port(self, tmp_path: Path) -> None:
        """TLS info endpoint includes the configured HTTPS port."""
        from robotocore.gateway.app import app

        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        config = TLSConfig(enabled=True, https_port=8443, cert_dir=tmp_path)

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", cert_path),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_robotocore/tls/info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["https_port"] == 8443

    def test_tls_info_disabled_includes_port(self) -> None:
        """Even when disabled, the endpoint reports the configured port."""
        from robotocore.gateway.app import app

        config = TLSConfig(enabled=False, https_port=9443)

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", None),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_robotocore/tls/info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["https_port"] == 9443
        assert data["enabled"] is False

    def test_tls_info_default_port(self) -> None:
        """Default port should be 443."""
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
        assert data["https_port"] == DEFAULT_HTTPS_PORT

    def test_tls_info_cert_has_all_fields(self, tmp_path: Path) -> None:
        """Certificate info in endpoint response should have all expected fields."""
        from robotocore.gateway.app import app

        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        generate_self_signed_cert(cert_path, key_path)

        config = TLSConfig(enabled=True, cert_dir=tmp_path)

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", cert_path),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_robotocore/tls/info")

        data = resp.json()
        cert_info = data["certificate"]
        assert "subject" in cert_info
        assert "issuer" in cert_info
        assert "not_valid_before" in cert_info
        assert "not_valid_after" in cert_info
        assert "serial_number" in cert_info
        assert "sans" in cert_info

    def test_tls_info_via_localstack_alias(self, tmp_path: Path) -> None:
        """The /_localstack/ alias should also work for TLS info."""
        from robotocore.gateway.app import app

        config = TLSConfig(enabled=False)

        with (
            mock.patch("robotocore.gateway.app._tls_config", config),
            mock.patch("robotocore.gateway.app._tls_cert_path", None),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/_localstack/tls/info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is False
