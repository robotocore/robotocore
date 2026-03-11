"""Tests for service loading controls."""

import os
from unittest.mock import patch

import pytest

from robotocore.services.loader import (
    get_allowed_services,
    get_service_info_with_status,
    init_loader,
    is_service_allowed,
    parse_services_env,
    reset_loader,
)
from robotocore.services.registry import SERVICE_REGISTRY


@pytest.fixture(autouse=True)
def _reset():
    """Reset loader state before each test."""
    reset_loader()
    yield
    reset_loader()


class TestParseServicesEnv:
    def test_default_all_services_available(self):
        """When SERVICES is not set, all services are available."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SERVICES", None)
            result = parse_services_env()
        assert result is None

    def test_services_env_filters(self):
        """SERVICES=s3,sqs returns only those two."""
        with patch.dict(os.environ, {"SERVICES": "s3,sqs"}):
            result = parse_services_env()
        assert result == {"s3", "sqs"}

    def test_empty_services_returns_none(self):
        """Empty SERVICES value is treated as unset."""
        with patch.dict(os.environ, {"SERVICES": ""}):
            result = parse_services_env()
        assert result is None

    def test_services_with_spaces_stripped(self):
        """Whitespace around service names is stripped."""
        with patch.dict(os.environ, {"SERVICES": " s3 , sqs , lambda "}):
            result = parse_services_env()
        assert result == {"s3", "sqs", "lambda"}

    def test_invalid_service_name_skipped(self):
        """Invalid service names produce a warning and are skipped."""
        with patch.dict(os.environ, {"SERVICES": "s3,not_a_real_service,sqs"}):
            result = parse_services_env()
        assert result == {"s3", "sqs"}

    def test_all_invalid_returns_none(self):
        """If all service names are invalid, returns None (all services)."""
        with patch.dict(os.environ, {"SERVICES": "bogus1,bogus2"}):
            result = parse_services_env()
        assert result is None


class TestIsServiceAllowed:
    def test_all_allowed_when_no_filter(self):
        """Without SERVICES env var, all registered services are allowed."""
        init_loader()
        assert is_service_allowed("s3") is True
        assert is_service_allowed("dynamodb") is True
        assert is_service_allowed("sqs") is True

    def test_only_listed_allowed_with_filter(self):
        """With SERVICES=s3,sqs, only s3 and sqs are allowed."""
        with patch.dict(os.environ, {"SERVICES": "s3,sqs"}):
            init_loader()
        assert is_service_allowed("s3") is True
        assert is_service_allowed("sqs") is True
        assert is_service_allowed("dynamodb") is False

    def test_unregistered_service_not_allowed(self):
        """An unregistered service is never allowed."""
        init_loader()
        assert is_service_allowed("totally_fake_service") is False

    def test_get_allowed_services_returns_set(self):
        """get_allowed_services returns the filter set."""
        with patch.dict(os.environ, {"SERVICES": "s3,sqs"}):
            init_loader()
        result = get_allowed_services()
        assert result == {"s3", "sqs"}

    def test_get_allowed_services_none_when_unset(self):
        """get_allowed_services returns None when all services are allowed."""
        init_loader()
        assert get_allowed_services() is None


class TestEagerLoading:
    def test_eager_loading_initializes_services(self):
        """EAGER_SERVICE_LOADING=1 triggers backend initialization."""
        with patch.dict(os.environ, {"EAGER_SERVICE_LOADING": "1"}):
            with patch("robotocore.services.loader.initialize_service") as mock_init:
                init_loader()
        # Should have been called for every registered service
        assert mock_init.call_count == len(SERVICE_REGISTRY)

    def test_eager_loading_with_services_filter(self):
        """EAGER_SERVICE_LOADING + SERVICES only loads listed services."""
        with patch.dict(os.environ, {"EAGER_SERVICE_LOADING": "1", "SERVICES": "s3,sqs"}):
            with patch("robotocore.services.loader.initialize_service") as mock_init:
                init_loader()
        assert mock_init.call_count == 2
        called_services = {call.args[0] for call in mock_init.call_args_list}
        assert called_services == {"s3", "sqs"}

    def test_no_eager_loading_by_default(self):
        """Without EAGER_SERVICE_LOADING, services are not pre-initialized."""
        with patch("robotocore.services.loader.initialize_service") as mock_init:
            init_loader()
        assert mock_init.call_count == 0


class TestServiceAvailabilityCheck:
    def test_service_info_enabled(self):
        """Service info shows enabled=True for allowed services."""
        with patch.dict(os.environ, {"SERVICES": "s3,sqs"}):
            init_loader()
        info = get_service_info_with_status("s3")
        assert info["enabled"] is True
        assert info["name"] == "s3"
        assert "status" in info

    def test_service_info_disabled(self):
        """Service info shows enabled=False for filtered-out services."""
        with patch.dict(os.environ, {"SERVICES": "s3"}):
            init_loader()
        info = get_service_info_with_status("dynamodb")
        assert info["enabled"] is False

    def test_service_info_unregistered(self):
        """Unregistered service returns minimal info."""
        init_loader()
        info = get_service_info_with_status("nonexistent")
        assert info["enabled"] is False
        assert info["registered"] is False
