"""Tests for multi-account support in the Moto bridge layer."""

from unittest.mock import MagicMock

from robotocore.providers.moto_bridge import _build_werkzeug_request


def _make_starlette_request(method="GET", path="/", headers=None):
    """Build a minimal Starlette-like request object for testing."""
    request = MagicMock()
    request.method = method
    url = MagicMock()
    url.path = path
    url.query = None
    request.url = url
    request.headers = headers or {"host": "localhost:4566"}
    request.scope = {}
    return request


class TestBuildWerkzeugRequestMultiAccount:
    def test_default_account_id_injected(self):
        """When no account_id is specified, the default is injected."""
        request = _make_starlette_request()
        werkzeug_req = _build_werkzeug_request(request, b"")
        assert werkzeug_req.headers.get("x-moto-account-id") == "123456789012"

    def test_custom_account_id_injected(self):
        """When a custom account_id is passed, it is injected into the header."""
        request = _make_starlette_request()
        werkzeug_req = _build_werkzeug_request(request, b"", account_id="999888777666")
        assert werkzeug_req.headers.get("x-moto-account-id") == "999888777666"

    def test_account_id_overrides_existing_header(self):
        """If the incoming request already has x-moto-account-id, we override it."""
        request = _make_starlette_request(
            headers={"host": "localhost:4566", "x-moto-account-id": "old-value"}
        )
        werkzeug_req = _build_werkzeug_request(request, b"", account_id="111222333444")
        assert werkzeug_req.headers.get("x-moto-account-id") == "111222333444"

    def test_other_headers_preserved(self):
        """Non-account headers are preserved when account_id is injected."""
        headers = {"host": "localhost:4566", "content-type": "application/json"}
        request = _make_starlette_request(headers=headers)
        werkzeug_req = _build_werkzeug_request(request, b"test-body", account_id="555666777888")
        assert werkzeug_req.headers.get("content-type") == "application/json"
        assert werkzeug_req.headers.get("x-moto-account-id") == "555666777888"
