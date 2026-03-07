"""CodeDeploy compatibility tests."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def codedeploy():
    return make_client("codedeploy")


@pytest.fixture
def iam():
    return make_client("iam")


TRUST_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "codedeploy.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def service_role_arn(iam):
    role_name = _unique("cd-role")
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=TRUST_POLICY,
        Path="/",
    )
    arn = resp["Role"]["Arn"]
    yield arn
    iam.delete_role(RoleName=role_name)


@pytest.fixture
def application(codedeploy):
    name = _unique("test-app")
    resp = codedeploy.create_application(applicationName=name, computePlatform="Server")
    yield {"name": name, "applicationId": resp["applicationId"]}


@pytest.fixture
def deployment_group(codedeploy, application, service_role_arn):
    dg_name = _unique("test-dg")
    resp = codedeploy.create_deployment_group(
        applicationName=application["name"],
        deploymentGroupName=dg_name,
        serviceRoleArn=service_role_arn,
    )
    yield {
        "applicationName": application["name"],
        "deploymentGroupName": dg_name,
        "deploymentGroupId": resp["deploymentGroupId"],
    }


class TestCodeDeployOperations:
    def test_list_applications(self, codedeploy):
        resp = codedeploy.list_applications()
        assert "applications" in resp

    def test_create_application(self, codedeploy):
        name = _unique("test-app")
        resp = codedeploy.create_application(
            applicationName=name, computePlatform="Server"
        )
        assert "applicationId" in resp

    def test_get_application(self, codedeploy, application):
        resp = codedeploy.get_application(applicationName=application["name"])
        app = resp["application"]
        assert app["applicationName"] == application["name"]
        assert app["computePlatform"] == "Server"

    def test_create_application_appears_in_list(self, codedeploy, application):
        resp = codedeploy.list_applications()
        assert application["name"] in resp["applications"]

    def test_create_deployment_group(self, codedeploy, application, service_role_arn):
        dg_name = _unique("test-dg")
        resp = codedeploy.create_deployment_group(
            applicationName=application["name"],
            deploymentGroupName=dg_name,
            serviceRoleArn=service_role_arn,
        )
        assert "deploymentGroupId" in resp

    def test_list_deployment_groups(self, codedeploy, deployment_group):
        resp = codedeploy.list_deployment_groups(
            applicationName=deployment_group["applicationName"]
        )
        assert deployment_group["deploymentGroupName"] in resp["deploymentGroups"]
