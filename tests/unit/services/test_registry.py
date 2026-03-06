"""Tests for the service registry."""

from robotocore.services.registry import (
    SERVICE_REGISTRY,
    ServiceStatus,
    get_enabled_services,
    is_service_enabled,
)


class TestServiceRegistry:
    def test_has_core_services(self):
        for svc in ["s3", "sqs", "sns", "dynamodb", "iam", "sts", "lambda"]:
            assert svc in SERVICE_REGISTRY

    def test_all_have_valid_status(self):
        valid = {
            ServiceStatus.MOTO_BACKED,
            ServiceStatus.NATIVE,
            ServiceStatus.EXTERNAL,
            ServiceStatus.PLANNED,
        }
        for info in SERVICE_REGISTRY.values():
            assert info.status in valid

    def test_native_services(self):
        native = [k for k, v in SERVICE_REGISTRY.items() if v.status == ServiceStatus.NATIVE]
        assert "sqs" in native
        assert "sns" in native
        assert "s3" in native
        assert "firehose" in native

    def test_get_enabled_services(self):
        services = get_enabled_services()
        assert len(services) >= 30
        assert "s3" in services
        assert "sts" in services

    def test_is_service_enabled(self):
        assert is_service_enabled("s3")
        assert not is_service_enabled("nonexistent")

    def test_protocols_set(self):
        assert SERVICE_REGISTRY["s3"].protocol == "rest-xml"
        assert SERVICE_REGISTRY["dynamodb"].protocol == "json"
        assert SERVICE_REGISTRY["sts"].protocol == "query"
        assert SERVICE_REGISTRY["ec2"].protocol == "ec2"
