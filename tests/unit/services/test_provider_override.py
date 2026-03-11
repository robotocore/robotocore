"""Tests for provider override functionality."""

import os
from unittest.mock import patch

import pytest

from robotocore.services.loader import (
    get_effective_provider,
    get_provider_overrides,
    get_service_info_with_status,
    init_loader,
    parse_provider_overrides,
    reset_loader,
    resolve_provider_override,
)


@pytest.fixture(autouse=True)
def _reset():
    """Reset loader state before each test."""
    reset_loader()
    yield
    reset_loader()


def _fake_handler(request, region, account_id):
    """A fake native handler for testing."""
    return None


def _another_fake_handler(request, region, account_id):
    """Another fake handler."""
    return None


FAKE_NATIVE_PROVIDERS = {
    "s3": _fake_handler,
    "dynamodb": _another_fake_handler,
}


class TestParseProviderOverrides:
    def test_dynamodb_moto_override(self):
        """PROVIDER_OVERRIDE_DYNAMODB=moto is parsed correctly."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_DYNAMODB": "moto"}):
            result = parse_provider_overrides()
        assert result["dynamodb"] == "moto"

    def test_s3_native_override(self):
        """PROVIDER_OVERRIDE_S3=native is parsed correctly."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_S3": "native"}):
            result = parse_provider_overrides()
        assert result["s3"] == "native"

    def test_dotted_path_override(self):
        """PROVIDER_OVERRIDE_S3=my.module.MyHandler is parsed as a dotted path."""
        with patch.dict(
            os.environ,
            {"PROVIDER_OVERRIDE_S3": "my.module.MyHandler"},
        ):
            result = parse_provider_overrides()
        assert result["s3"] == "my.module.MyHandler"

    def test_invalid_service_ignored(self):
        """Override for a nonexistent service is warned and ignored."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_FAKESVC": "moto"}):
            result = parse_provider_overrides()
        assert "fakesvc" not in result
        assert "fake-svc" not in result

    def test_multiple_overrides(self):
        """Multiple overrides can be set simultaneously."""
        with patch.dict(
            os.environ,
            {
                "PROVIDER_OVERRIDE_DYNAMODB": "moto",
                "PROVIDER_OVERRIDE_S3": "native",
            },
        ):
            result = parse_provider_overrides()
        assert result["dynamodb"] == "moto"
        assert result["s3"] == "native"

    def test_override_with_hyphen_service(self):
        """PROVIDER_OVERRIDE_COGNITO_IDP=moto resolves to cognito-idp."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_COGNITO_IDP": "moto"}):
            result = parse_provider_overrides()
        assert result.get("cognito-idp") == "moto"


class TestResolveProviderOverride:
    def test_moto_returns_none(self):
        """Moto override returns None (signals use Moto bridge)."""
        result = resolve_provider_override("dynamodb", "moto")
        assert result is None

    def test_native_returns_handler(self):
        """Native override returns the native handler from NATIVE_PROVIDERS."""
        with patch(
            "robotocore.services.loader.NATIVE_PROVIDERS",
            {"s3": _fake_handler},
            create=True,
        ):
            # native imports from robotocore.gateway.app, need to patch it there
            from robotocore.gateway import app

            original = getattr(app, "NATIVE_PROVIDERS", {})
            app.NATIVE_PROVIDERS = {"s3": _fake_handler}
            try:
                result = resolve_provider_override("s3", "native")
                assert result is _fake_handler
            finally:
                app.NATIVE_PROVIDERS = original

    def test_invalid_dotted_path_returns_none(self):
        """Invalid dotted path logs warning and returns None."""
        result = resolve_provider_override("s3", "nonexistent.module.Handler")
        assert result is None

    def test_valid_dotted_path_returns_object(self):
        """Valid dotted path imports and returns the object."""
        result = resolve_provider_override(
            "s3",
            "tests.unit.services.test_provider_override._fake_handler",
        )
        assert result is _fake_handler


class TestGetEffectiveProvider:
    def test_no_override_uses_native(self):
        """Without override, uses native provider if available."""
        init_loader()
        result = get_effective_provider("s3", FAKE_NATIVE_PROVIDERS)
        assert result is _fake_handler

    def test_no_override_no_native_returns_none(self):
        """Without override and no native provider, returns None (Moto)."""
        init_loader()
        result = get_effective_provider("kms", FAKE_NATIVE_PROVIDERS)
        assert result is None

    def test_moto_override_forces_moto(self):
        """PROVIDER_OVERRIDE_DYNAMODB=moto returns None even if native exists."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_DYNAMODB": "moto"}):
            init_loader()
        result = get_effective_provider("dynamodb", FAKE_NATIVE_PROVIDERS)
        assert result is None

    def test_native_override_forces_native(self):
        """PROVIDER_OVERRIDE_S3=native returns the native handler."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_S3": "native"}):
            init_loader()
        result = get_effective_provider("s3", FAKE_NATIVE_PROVIDERS)
        assert result is _fake_handler

    def test_override_applied_before_first_request(self):
        """Override is resolved during init, before any request."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_DYNAMODB": "moto"}):
            init_loader()
        overrides = get_provider_overrides()
        assert "dynamodb" in overrides
        assert overrides["dynamodb"] == "moto"

    def test_override_visible_in_service_info(self):
        """Override appears in service info endpoint data."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_DYNAMODB": "moto"}):
            init_loader()
        info = get_service_info_with_status("dynamodb")
        assert info.get("provider_override") == "moto"
