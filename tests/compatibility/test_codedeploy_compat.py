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
        resp = codedeploy.create_application(applicationName=name, computePlatform="Server")
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


class TestCodedeployAutoCoverage:
    """Auto-generated coverage tests for codedeploy."""

    @pytest.fixture
    def client(self):
        return make_client("codedeploy")

    def test_list_deployments(self, client):
        """ListDeployments returns a response."""
        resp = client.list_deployments()
        assert "deployments" in resp

    def test_create_and_get_deployment(self, client):
        """CreateDeployment + GetDeployment."""
        iam_client = make_client("iam")
        role_name = _unique("cd-role")
        role_resp = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Path="/",
        )
        role_arn = role_resp["Role"]["Arn"]
        try:
            app_name = _unique("test-app")
            client.create_application(applicationName=app_name, computePlatform="Server")
            dg_name = _unique("test-dg")
            client.create_deployment_group(
                applicationName=app_name,
                deploymentGroupName=dg_name,
                serviceRoleArn=role_arn,
            )
            dep_resp = client.create_deployment(
                applicationName=app_name,
                deploymentGroupName=dg_name,
                revision={
                    "revisionType": "GitHub",
                    "gitHubLocation": {
                        "repository": "test/repo",
                        "commitId": "abc123",
                    },
                },
            )
            deployment_id = dep_resp["deploymentId"]
            assert deployment_id

            get_resp = client.get_deployment(deploymentId=deployment_id)
            info = get_resp["deploymentInfo"]
            assert info["deploymentId"] == deployment_id
            assert info["applicationName"] == app_name
        finally:
            iam_client.delete_role(RoleName=role_name)

    def test_batch_get_applications(self, client):
        """BatchGetApplications returns application details."""
        app_name = _unique("test-app")
        client.create_application(applicationName=app_name, computePlatform="Server")
        resp = client.batch_get_applications(applicationNames=[app_name])
        assert "applicationsInfo" in resp
        assert len(resp["applicationsInfo"]) == 1
        assert resp["applicationsInfo"][0]["applicationName"] == app_name

    def test_batch_get_deployments(self, client):
        """BatchGetDeployments returns deployment details."""
        iam_client = make_client("iam")
        role_name = _unique("cd-role")
        role_resp = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Path="/",
        )
        role_arn = role_resp["Role"]["Arn"]
        try:
            app_name = _unique("test-app")
            client.create_application(applicationName=app_name, computePlatform="Server")
            dg_name = _unique("test-dg")
            client.create_deployment_group(
                applicationName=app_name,
                deploymentGroupName=dg_name,
                serviceRoleArn=role_arn,
            )
            dep_resp = client.create_deployment(
                applicationName=app_name,
                deploymentGroupName=dg_name,
                revision={
                    "revisionType": "GitHub",
                    "gitHubLocation": {
                        "repository": "test/repo",
                        "commitId": "abc123",
                    },
                },
            )
            deployment_id = dep_resp["deploymentId"]
            resp = client.batch_get_deployments(deploymentIds=[deployment_id])
            assert "deploymentsInfo" in resp
            assert len(resp["deploymentsInfo"]) == 1
            assert resp["deploymentsInfo"][0]["deploymentId"] == deployment_id
        finally:
            iam_client.delete_role(RoleName=role_name)

    def test_tag_and_list_tags_for_resource(self, client):
        """TagResource + ListTagsForResource on an application."""
        app_name = _unique("test-app")
        client.create_application(applicationName=app_name, computePlatform="Server")
        # CodeDeploy uses applicationId-based ARN; construct from known pattern
        app_arn = f"arn:aws:codedeploy:us-east-1:123456789012:application:{app_name}"
        client.tag_resource(
            ResourceArn=app_arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        resp = client.list_tags_for_resource(ResourceArn=app_arn)
        assert "Tags" in resp
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags.get("env") == "test"

        # UntagResource
        client.untag_resource(
            ResourceArn=app_arn,
            TagKeys=["env"],
        )
        resp2 = client.list_tags_for_resource(ResourceArn=app_arn)
        tags2 = {t["Key"]: t["Value"] for t in resp2.get("Tags", [])}
        assert "env" not in tags2

    def test_get_deployment_group(self, client):
        """GetDeploymentGroup returns group details."""
        iam_client = make_client("iam")
        role_name = _unique("cd-role")
        role_resp = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Path="/",
        )
        role_arn = role_resp["Role"]["Arn"]
        try:
            app_name = _unique("test-app")
            client.create_application(applicationName=app_name, computePlatform="Server")
            dg_name = _unique("test-dg")
            client.create_deployment_group(
                applicationName=app_name,
                deploymentGroupName=dg_name,
                serviceRoleArn=role_arn,
            )
            resp = client.get_deployment_group(
                applicationName=app_name,
                deploymentGroupName=dg_name,
            )
            info = resp["deploymentGroupInfo"]
            assert info["deploymentGroupName"] == dg_name
            assert info["applicationName"] == app_name
        finally:
            iam_client.delete_role(RoleName=role_name)
