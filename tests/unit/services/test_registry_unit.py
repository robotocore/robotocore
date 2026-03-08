"""Unit tests for the service registry."""

from robotocore.services.registry import (
    SERVICE_REGISTRY,
    ServiceInfo,
    ServiceStatus,
)


class TestServiceRegistry:
    def test_registry_not_empty(self):
        assert len(SERVICE_REGISTRY) > 100

    def test_all_entries_are_service_info(self):
        for name, info in SERVICE_REGISTRY.items():
            assert isinstance(info, ServiceInfo), f"{name} is not ServiceInfo"

    def test_all_have_protocol(self):
        valid = {"json", "rest-json", "rest-xml", "query", "ec2", "smithy-rpc-v2-cbor"}
        for name, info in SERVICE_REGISTRY.items():
            assert info.protocol in valid, f"{name} has invalid protocol: {info.protocol}"

    def test_core_services_are_native(self):
        native = [
            "s3",
            "sqs",
            "sns",
            "dynamodb",
            "lambda",
            "iam",
            "sts",
            "cloudformation",
            "events",
            "logs",
            "kinesis",
        ]
        for svc in native:
            assert svc in SERVICE_REGISTRY, f"{svc} missing"
            assert SERVICE_REGISTRY[svc].status == ServiceStatus.NATIVE

    def test_name_matches_key(self):
        for key, info in SERVICE_REGISTRY.items():
            assert info.name == key, f"Key '{key}' != name '{info.name}'"

    def test_no_duplicate_names(self):
        names = list(SERVICE_REGISTRY.keys())
        assert len(names) == len(set(names))


class TestServiceStatus:
    def test_enum_values(self):
        assert ServiceStatus.MOTO_BACKED.value == "moto_backed"
        assert ServiceStatus.NATIVE.value == "native"
        assert ServiceStatus.EXTERNAL.value == "external"
        assert ServiceStatus.PLANNED.value == "planned"


class TestServiceInfo:
    def test_create(self):
        info = ServiceInfo("test", ServiceStatus.NATIVE, "json", "A test service")
        assert info.name == "test"
        assert info.status == ServiceStatus.NATIVE
        assert info.protocol == "json"
        assert info.description == "A test service"

    def test_default_description(self):
        info = ServiceInfo("test", ServiceStatus.MOTO_BACKED, "query")
        assert info.description == ""
