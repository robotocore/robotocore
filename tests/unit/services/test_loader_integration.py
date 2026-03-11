"""Semantic integration tests for the service loader.

Tests end-to-end behavior: env vars -> loader -> gateway filtering.
"""

import os
from unittest.mock import patch

import pytest

from robotocore.services.loader import (
    get_service_info_with_status,
    init_loader,
    is_service_allowed,
    reset_loader,
)
from robotocore.services.registry import SERVICE_REGISTRY


@pytest.fixture(autouse=True)
def _reset():
    """Reset loader state before each test."""
    reset_loader()
    yield
    reset_loader()


class TestEndToEndServiceFiltering:
    def test_s3_allowed_sqs_blocked(self):
        """SERVICES=s3: S3 is allowed, SQS is not."""
        with patch.dict(os.environ, {"SERVICES": "s3"}):
            init_loader()
        assert is_service_allowed("s3") is True
        assert is_service_allowed("sqs") is False

    def test_all_services_allowed_by_default(self):
        """Without SERVICES, every registered service is allowed."""
        init_loader()
        for name in SERVICE_REGISTRY:
            assert is_service_allowed(name) is True, f"{name} should be allowed"

    def test_unregistered_always_blocked(self):
        """An unregistered service is always blocked."""
        init_loader()
        assert is_service_allowed("not_a_service") is False

        with patch.dict(os.environ, {"SERVICES": "s3"}):
            reset_loader()
            init_loader()
        assert is_service_allowed("not_a_service") is False


class TestEndToEndEagerLoading:
    def test_eager_loading_calls_get_backend(self):
        """EAGER_SERVICE_LOADING=1 calls get_backend for each service."""
        with patch.dict(os.environ, {"EAGER_SERVICE_LOADING": "1"}):
            with patch("robotocore.services.loader.initialize_service") as mock:
                init_loader()
        # At minimum, all registry services should be initialized
        called = {call.args[0] for call in mock.call_args_list}
        assert called == set(SERVICE_REGISTRY.keys())

    def test_eager_with_filter(self):
        """EAGER_SERVICE_LOADING + SERVICES=s3,sqs only loads 2."""
        with patch.dict(os.environ, {"EAGER_SERVICE_LOADING": "1", "SERVICES": "s3,sqs"}):
            with patch("robotocore.services.loader.initialize_service") as mock:
                init_loader()
        called = {call.args[0] for call in mock.call_args_list}
        assert called == {"s3", "sqs"}


class TestEndToEndProviderOverride:
    def test_override_changes_provider_resolution(self):
        """PROVIDER_OVERRIDE_DYNAMODB=moto forces DynamoDB to use Moto."""
        from robotocore.services.loader import get_effective_provider

        fake_native = {"dynamodb": lambda r, reg, acc: "native_response"}
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_DYNAMODB": "moto"}):
            init_loader()
        provider = get_effective_provider("dynamodb", fake_native)
        assert provider is None  # None means use Moto bridge


class TestServicesEndpointData:
    def test_enabled_disabled_in_service_info(self):
        """Service info correctly shows enabled/disabled."""
        with patch.dict(os.environ, {"SERVICES": "s3,sqs"}):
            init_loader()

        s3_info = get_service_info_with_status("s3")
        assert s3_info["enabled"] is True

        dynamodb_info = get_service_info_with_status("dynamodb")
        assert dynamodb_info["enabled"] is False

    def test_override_in_service_info(self):
        """Provider override appears in service info."""
        with patch.dict(os.environ, {"PROVIDER_OVERRIDE_S3": "moto"}):
            init_loader()

        info = get_service_info_with_status("s3")
        assert info["provider_override"] == "moto"
        assert info["enabled"] is True

    def test_all_services_enabled_by_default(self):
        """Without SERVICES, all services show enabled=True."""
        init_loader()
        for name in SERVICE_REGISTRY:
            info = get_service_info_with_status(name)
            assert info["enabled"] is True, f"{name} should be enabled"
