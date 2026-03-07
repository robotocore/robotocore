"""SWF compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def swf():
    return make_client("swf")


def _uid():
    return uuid.uuid4().hex[:8]


class TestSWFOperations:
    def test_register_domain(self, swf):
        name = f"test-domain-{_uid()}"
        response = swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        swf.deprecate_domain(name=name)

    def test_list_domains(self, swf):
        name = f"list-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        response = swf.list_domains(registrationStatus="REGISTERED")
        domain_names = [d["name"] for d in response["domainInfos"]]
        assert name in domain_names
        swf.deprecate_domain(name=name)

    def test_describe_domain(self, swf):
        name = f"describe-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="60",
        )
        response = swf.describe_domain(name=name)
        assert response["domainInfo"]["name"] == name
        assert response["domainInfo"]["status"] == "REGISTERED"
        assert response["configuration"]["workflowExecutionRetentionPeriodInDays"] == "60"
        swf.deprecate_domain(name=name)

    def test_deprecate_domain(self, swf):
        name = f"deprecate-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        swf.deprecate_domain(name=name)
        response = swf.describe_domain(name=name)
        assert response["domainInfo"]["status"] == "DEPRECATED"

    def test_register_workflow_type(self, swf):
        name = f"workflow-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        response = swf.register_workflow_type(
            domain=name,
            name="test-workflow",
            version="1.0",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        types = swf.list_workflow_types(
            domain=name,
            registrationStatus="REGISTERED",
        )
        type_names = [t["workflowType"]["name"] for t in types["typeInfos"]]
        assert "test-workflow" in type_names

        swf.deprecate_workflow_type(
            domain=name,
            workflowType={"name": "test-workflow", "version": "1.0"},
        )
        swf.deprecate_domain(name=name)

    def test_register_activity_type(self, swf):
        domain_name = f"activity-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        response = swf.register_activity_type(
            domain=domain_name, name="test-activity", version="1.0"
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        types = swf.list_activity_types(domain=domain_name, registrationStatus="REGISTERED")
        names = [t["activityType"]["name"] for t in types["typeInfos"]]
        assert "test-activity" in names
        swf.deprecate_activity_type(
            domain=domain_name,
            activityType={"name": "test-activity", "version": "1.0"},
        )
        swf.deprecate_domain(name=domain_name)

    def test_list_deprecated_domains(self, swf):
        domain_name = f"dep-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        swf.deprecate_domain(name=domain_name)
        response = swf.list_domains(registrationStatus="DEPRECATED")
        names = [d["name"] for d in response["domainInfos"]]
        assert domain_name in names

    def test_describe_workflow_type(self, swf):
        domain_name = f"desc-wf-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(domain=domain_name, name="desc-workflow", version="1.0")
        response = swf.describe_workflow_type(
            domain=domain_name,
            workflowType={"name": "desc-workflow", "version": "1.0"},
        )
        assert response["typeInfo"]["workflowType"]["name"] == "desc-workflow"
        swf.deprecate_workflow_type(
            domain=domain_name,
            workflowType={"name": "desc-workflow", "version": "1.0"},
        )
        swf.deprecate_domain(name=domain_name)

    def test_describe_activity_type(self, swf):
        domain_name = f"desc-act-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        swf.register_activity_type(domain=domain_name, name="desc-activity", version="2.0")
        response = swf.describe_activity_type(
            domain=domain_name,
            activityType={"name": "desc-activity", "version": "2.0"},
        )
        assert response["typeInfo"]["activityType"]["name"] == "desc-activity"
        swf.deprecate_activity_type(
            domain=domain_name,
            activityType={"name": "desc-activity", "version": "2.0"},
        )
        swf.deprecate_domain(name=domain_name)
