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


class TestServiceDiscoveryInstanceOperations:
    def test_register_instance(self, sd, service):
        inst_id = _unique("inst")
        resp = sd.register_instance(
            ServiceId=service["Id"],
            InstanceId=inst_id,
            Attributes={"AWS_INSTANCE_IPV4": "10.0.0.1", "AWS_INSTANCE_PORT": "8080"},
        )
        assert "OperationId" in resp
        sd.deregister_instance(ServiceId=service["Id"], InstanceId=inst_id)

    def test_get_instance(self, sd, service):
        inst_id = _unique("inst")
        sd.register_instance(
            ServiceId=service["Id"],
            InstanceId=inst_id,
            Attributes={"AWS_INSTANCE_IPV4": "10.0.0.2"},
        )
        try:
            resp = sd.get_instance(ServiceId=service["Id"], InstanceId=inst_id)
            inst = resp["Instance"]
            assert inst["Id"] == inst_id
            assert inst["Attributes"]["AWS_INSTANCE_IPV4"] == "10.0.0.2"
        finally:
            sd.deregister_instance(ServiceId=service["Id"], InstanceId=inst_id)

    def test_deregister_instance(self, sd, service):
        inst_id = _unique("inst")
        sd.register_instance(
            ServiceId=service["Id"],
            InstanceId=inst_id,
            Attributes={"AWS_INSTANCE_IPV4": "10.0.0.3"},
        )
        resp = sd.deregister_instance(ServiceId=service["Id"], InstanceId=inst_id)
        assert "OperationId" in resp
        # Verify it's gone
        list_resp = sd.list_instances(ServiceId=service["Id"])
        ids = [i["Id"] for i in list_resp["Instances"]]
        assert inst_id not in ids

    def test_list_instances(self, sd, service):
        inst_id = _unique("inst")
        sd.register_instance(
            ServiceId=service["Id"],
            InstanceId=inst_id,
            Attributes={"AWS_INSTANCE_IPV4": "10.0.0.4"},
        )
        try:
            resp = sd.list_instances(ServiceId=service["Id"])
            assert "Instances" in resp
            ids = [i["Id"] for i in resp["Instances"]]
            assert inst_id in ids
        finally:
            sd.deregister_instance(ServiceId=service["Id"], InstanceId=inst_id)

    def test_get_instances_health_status(self, sd, service):
        inst_id = _unique("inst")
        sd.register_instance(
            ServiceId=service["Id"],
            InstanceId=inst_id,
            Attributes={"AWS_INSTANCE_IPV4": "10.0.0.5"},
        )
        try:
            resp = sd.get_instances_health_status(ServiceId=service["Id"])
            assert "Status" in resp
        finally:
            sd.deregister_instance(ServiceId=service["Id"], InstanceId=inst_id)

    def test_update_instance_custom_health_status(self, sd, namespace):
        # Need a service with custom health check for this op
        name = _unique("svc")
        svc_resp = sd.create_service(
            Name=name,
            NamespaceId=namespace["Id"],
            HealthCheckCustomConfig={"FailureThreshold": 1},
        )
        svc_id = svc_resp["Service"]["Id"]
        inst_id = _unique("inst")
        sd.register_instance(
            ServiceId=svc_id,
            InstanceId=inst_id,
            Attributes={"AWS_INSTANCE_IPV4": "10.0.0.6"},
        )
        try:
            sd.update_instance_custom_health_status(
                ServiceId=svc_id,
                InstanceId=inst_id,
                Status="HEALTHY",
            )
            # If we get here without error, the op is implemented
            assert True
        finally:
            sd.deregister_instance(ServiceId=svc_id, InstanceId=inst_id)
            sd.delete_service(Id=svc_id)


class TestServiceDiscoveryNamespaceTypes:
    def test_create_private_dns_namespace(self, sd):
        name = _unique("priv") + ".local"
        resp = sd.create_private_dns_namespace(Name=name, Vpc="vpc-12345")
        assert "OperationId" in resp
        op = sd.get_operation(OperationId=resp["OperationId"])
        ns_id = op["Operation"]["Targets"]["NAMESPACE"]
        sd.delete_namespace(Id=ns_id)

    def test_create_public_dns_namespace(self, sd):
        name = _unique("pub") + ".example.com"
        resp = sd.create_public_dns_namespace(Name=name)
        assert "OperationId" in resp
        op = sd.get_operation(OperationId=resp["OperationId"])
        ns_id = op["Operation"]["Targets"]["NAMESPACE"]
        sd.delete_namespace(Id=ns_id)

    def test_update_http_namespace(self, sd, namespace):
        resp = sd.update_http_namespace(
            Id=namespace["Id"],
            Namespace={"Description": "updated"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_private_dns_namespace(self, sd):
        name = _unique("priv") + ".local"
        resp = sd.create_private_dns_namespace(Name=name, Vpc="vpc-12345")
        op = sd.get_operation(OperationId=resp["OperationId"])
        ns_id = op["Operation"]["Targets"]["NAMESPACE"]
        try:
            upd = sd.update_private_dns_namespace(
                Id=ns_id,
                Namespace={"Description": "updated"},
            )
            assert "OperationId" in upd
        finally:
            sd.delete_namespace(Id=ns_id)

    def test_update_public_dns_namespace(self, sd):
        name = _unique("pub") + ".example.com"
        resp = sd.create_public_dns_namespace(Name=name)
        op = sd.get_operation(OperationId=resp["OperationId"])
        ns_id = op["Operation"]["Targets"]["NAMESPACE"]
        try:
            upd = sd.update_public_dns_namespace(
                Id=ns_id,
                Namespace={"Description": "updated"},
            )
            assert "OperationId" in upd
        finally:
            sd.delete_namespace(Id=ns_id)

    def test_update_service(self, sd, service):
        resp = sd.update_service(
            Id=service["Id"],
            Service={"Description": "updated-desc"},
        )
        assert "OperationId" in resp
