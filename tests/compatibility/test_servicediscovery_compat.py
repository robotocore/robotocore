"""ServiceDiscovery (Cloud Map) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sd():
    return make_client("servicediscovery")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def namespace(sd):
    """Create an HTTP namespace and clean it up after the test."""
    name = _unique("ns")
    resp = sd.create_http_namespace(Name=name)
    op_id = resp["OperationId"]
    # Get the namespace ID from the operation
    op = sd.get_operation(OperationId=op_id)
    ns_id = op["Operation"]["Targets"]["NAMESPACE"]
    yield {"Id": ns_id, "Name": name, "OperationId": op_id}
    try:
        sd.delete_namespace(Id=ns_id)
    except Exception:
        pass


@pytest.fixture
def service(sd, namespace):
    """Create a service in the namespace and clean it up after the test."""
    name = _unique("svc")
    resp = sd.create_service(Name=name, NamespaceId=namespace["Id"])
    svc = resp["Service"]
    yield svc
    try:
        sd.delete_service(Id=svc["Id"])
    except Exception:
        pass


class TestServiceDiscoveryNamespaceOperations:
    def test_create_http_namespace(self, sd):
        name = _unique("ns")
        resp = sd.create_http_namespace(Name=name)
        assert "OperationId" in resp
        op = sd.get_operation(OperationId=resp["OperationId"])
        assert op["Operation"]["Status"] == "SUCCESS"
        ns_id = op["Operation"]["Targets"]["NAMESPACE"]
        # Cleanup
        sd.delete_namespace(Id=ns_id)

    def test_get_namespace(self, sd, namespace):
        resp = sd.get_namespace(Id=namespace["Id"])
        ns = resp["Namespace"]
        assert ns["Id"] == namespace["Id"]
        assert ns["Name"] == namespace["Name"]
        assert ns["Type"] == "HTTP"

    def test_list_namespaces(self, sd, namespace):
        resp = sd.list_namespaces()
        ids = [ns["Id"] for ns in resp["Namespaces"]]
        assert namespace["Id"] in ids

    def test_list_namespaces_contains_type(self, sd, namespace):
        resp = sd.list_namespaces()
        for ns in resp["Namespaces"]:
            if ns["Id"] == namespace["Id"]:
                assert ns["Type"] == "HTTP"
                break
        else:
            pytest.fail("Namespace not found in list")

    def test_delete_namespace(self, sd):
        name = _unique("ns")
        resp = sd.create_http_namespace(Name=name)
        op = sd.get_operation(OperationId=resp["OperationId"])
        ns_id = op["Operation"]["Targets"]["NAMESPACE"]
        del_resp = sd.delete_namespace(Id=ns_id)
        assert "OperationId" in del_resp

    def test_get_operation(self, sd, namespace):
        op = sd.get_operation(OperationId=namespace["OperationId"])
        assert op["Operation"]["Status"] == "SUCCESS"
        assert "NAMESPACE" in op["Operation"]["Targets"]

    def test_list_operations(self, sd, namespace):
        resp = sd.list_operations()
        op_ids = [op["Id"] for op in resp["Operations"]]
        assert namespace["OperationId"] in op_ids


class TestServiceDiscoveryServiceOperations:
    def test_create_service(self, sd, namespace):
        name = _unique("svc")
        resp = sd.create_service(Name=name, NamespaceId=namespace["Id"])
        svc = resp["Service"]
        assert svc["Name"] == name
        assert "Id" in svc
        assert "Arn" in svc
        sd.delete_service(Id=svc["Id"])

    def test_get_service(self, sd, service):
        resp = sd.get_service(Id=service["Id"])
        svc = resp["Service"]
        assert svc["Id"] == service["Id"]
        assert svc["Name"] == service["Name"]

    def test_list_services(self, sd, service):
        resp = sd.list_services()
        ids = [s["Id"] for s in resp["Services"]]
        assert service["Id"] in ids

    def test_delete_service(self, sd, namespace):
        name = _unique("svc")
        resp = sd.create_service(Name=name, NamespaceId=namespace["Id"])
        svc_id = resp["Service"]["Id"]
        sd.delete_service(Id=svc_id)
        # Verify it's gone
        resp = sd.list_services()
        ids = [s["Id"] for s in resp["Services"]]
        assert svc_id not in ids


class TestServiceDiscoveryTags:
    def test_tag_resource(self, sd, service):
        sd.tag_resource(
            ResourceARN=service["Arn"],
            Tags=[{"Key": "env", "Value": "test"}],
        )
        resp = sd.list_tags_for_resource(ResourceARN=service["Arn"])
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"

    def test_tag_resource_multiple(self, sd, service):
        sd.tag_resource(
            ResourceARN=service["Arn"],
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        resp = sd.list_tags_for_resource(ResourceARN=service["Arn"])
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

    def test_list_tags_for_resource_empty(self, sd, service):
        resp = sd.list_tags_for_resource(ResourceARN=service["Arn"])
        assert resp["Tags"] == []

    def test_untag_resource(self, sd, service):
        sd.tag_resource(
            ResourceARN=service["Arn"],
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        sd.untag_resource(ResourceARN=service["Arn"], TagKeys=["env"])
        resp = sd.list_tags_for_resource(ResourceARN=service["Arn"])
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert "env" not in tags
        assert tags["team"] == "platform"

    def test_tag_resource_overwrite(self, sd, service):
        sd.tag_resource(
            ResourceARN=service["Arn"],
            Tags=[{"Key": "env", "Value": "dev"}],
        )
        sd.tag_resource(
            ResourceARN=service["Arn"],
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        resp = sd.list_tags_for_resource(ResourceARN=service["Arn"])
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "prod"
