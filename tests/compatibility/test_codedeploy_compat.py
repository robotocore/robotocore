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

    def test_list_deployment_configs(self, client):
        """ListDeploymentConfigs returns default configs."""
        resp = client.list_deployment_configs()
        assert "deploymentConfigsList" in resp
        assert len(resp["deploymentConfigsList"]) > 0

    def test_list_on_premises_instances(self, client):
        """ListOnPremisesInstances returns a response."""
        resp = client.list_on_premises_instances()
        assert "instanceNames" in resp

    def test_list_github_account_token_names(self, client):
        """ListGitHubAccountTokenNames returns a response."""
        resp = client.list_git_hub_account_token_names()
        assert "tokenNameList" in resp

    def test_create_and_delete_deployment_config(self, client):
        """CreateDeploymentConfig + GetDeploymentConfig + DeleteDeploymentConfig."""
        config_name = _unique("test-config")
        client.create_deployment_config(
            deploymentConfigName=config_name,
            minimumHealthyHosts={"type": "HOST_COUNT", "value": 1},
        )
        resp = client.get_deployment_config(deploymentConfigName=config_name)
        info = resp["deploymentConfigInfo"]
        assert info["deploymentConfigName"] == config_name

        client.delete_deployment_config(deploymentConfigName=config_name)
        with pytest.raises(client.exceptions.DeploymentConfigDoesNotExistException):
            client.get_deployment_config(deploymentConfigName=config_name)

    def test_delete_application(self, client):
        """DeleteApplication removes the application."""
        app_name = _unique("test-app")
        client.create_application(applicationName=app_name, computePlatform="Server")
        client.delete_application(applicationName=app_name)
        resp = client.list_applications()
        assert app_name not in resp["applications"]

    def test_delete_deployment_group(self, client):
        """DeleteDeploymentGroup removes the group."""
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
            client.delete_deployment_group(
                applicationName=app_name,
                deploymentGroupName=dg_name,
            )
            resp = client.list_deployment_groups(applicationName=app_name)
            assert dg_name not in resp["deploymentGroups"]
        finally:
            iam_client.delete_role(RoleName=role_name)

    def test_update_application(self, client):
        """UpdateApplication renames the application."""
        old_name = _unique("test-app")
        new_name = _unique("renamed-app")
        client.create_application(applicationName=old_name, computePlatform="Server")
        client.update_application(applicationName=old_name, newApplicationName=new_name)
        resp = client.list_applications()
        assert new_name in resp["applications"]
        assert old_name not in resp["applications"]

    def test_update_deployment_group(self, client):
        """UpdateDeploymentGroup changes group settings."""
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
            new_dg_name = _unique("renamed-dg")
            client.create_deployment_group(
                applicationName=app_name,
                deploymentGroupName=dg_name,
                serviceRoleArn=role_arn,
            )
            client.update_deployment_group(
                applicationName=app_name,
                currentDeploymentGroupName=dg_name,
                newDeploymentGroupName=new_dg_name,
            )
            resp = client.list_deployment_groups(applicationName=app_name)
            assert new_dg_name in resp["deploymentGroups"]
            assert dg_name not in resp["deploymentGroups"]
        finally:
            iam_client.delete_role(RoleName=role_name)

    def test_register_application_revision(self, client):
        """RegisterApplicationRevision + GetApplicationRevision."""
        app_name = _unique("test-app")
        client.create_application(applicationName=app_name, computePlatform="Server")
        revision = {
            "revisionType": "GitHub",
            "gitHubLocation": {
                "repository": "test/repo",
                "commitId": "def456",
            },
        }
        client.register_application_revision(
            applicationName=app_name,
            revision=revision,
        )
        resp = client.get_application_revision(
            applicationName=app_name,
            revision=revision,
        )
        assert "applicationName" in resp
        assert resp["applicationName"] == app_name

    def test_list_application_revisions(self, client):
        """ListApplicationRevisions after registering a revision."""
        app_name = _unique("test-app")
        client.create_application(applicationName=app_name, computePlatform="Server")
        revision = {
            "revisionType": "GitHub",
            "gitHubLocation": {
                "repository": "test/repo",
                "commitId": "ghi789",
            },
        }
        client.register_application_revision(applicationName=app_name, revision=revision)
        resp = client.list_application_revisions(applicationName=app_name)
        assert "revisions" in resp

    def test_batch_get_deployment_groups(self, client):
        """BatchGetDeploymentGroups returns group details."""
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
            resp = client.batch_get_deployment_groups(
                applicationName=app_name,
                deploymentGroupNames=[dg_name],
            )
            assert "deploymentGroupsInfo" in resp
            assert len(resp["deploymentGroupsInfo"]) == 1
            assert resp["deploymentGroupsInfo"][0]["deploymentGroupName"] == dg_name
        finally:
            iam_client.delete_role(RoleName=role_name)

    def test_batch_get_application_revisions(self, client):
        """BatchGetApplicationRevisions returns revision info."""
        app_name = _unique("test-app")
        client.create_application(applicationName=app_name, computePlatform="Server")
        revision = {
            "revisionType": "GitHub",
            "gitHubLocation": {
                "repository": "test/repo",
                "commitId": "rev123",
            },
        }
        client.register_application_revision(applicationName=app_name, revision=revision)
        resp = client.batch_get_application_revisions(
            applicationName=app_name,
            revisions=[revision],
        )
        assert "applicationName" in resp
        assert resp["applicationName"] == app_name
        assert "revisions" in resp

    def test_register_on_premises_instance(self, client):
        """RegisterOnPremisesInstance + GetOnPremisesInstance."""
        instance_name = _unique("on-prem")
        iam_arn = f"arn:aws:iam::123456789012:user/{instance_name}"
        client.register_on_premises_instance(
            instanceName=instance_name,
            iamUserArn=iam_arn,
        )
        resp = client.get_on_premises_instance(instanceName=instance_name)
        info = resp["instanceInfo"]
        assert info["instanceName"] == instance_name

    def test_deregister_on_premises_instance(self, client):
        """DeregisterOnPremisesInstance removes the instance."""
        instance_name = _unique("on-prem")
        iam_arn = f"arn:aws:iam::123456789012:user/{instance_name}"
        client.register_on_premises_instance(
            instanceName=instance_name,
            iamUserArn=iam_arn,
        )
        client.deregister_on_premises_instance(instanceName=instance_name)
        resp = client.list_on_premises_instances()
        assert instance_name not in resp["instanceNames"]

    def test_batch_get_on_premises_instances(self, client):
        """BatchGetOnPremisesInstances returns instance info."""
        instance_name = _unique("on-prem")
        iam_arn = f"arn:aws:iam::123456789012:user/{instance_name}"
        client.register_on_premises_instance(
            instanceName=instance_name,
            iamUserArn=iam_arn,
        )
        resp = client.batch_get_on_premises_instances(instanceNames=[instance_name])
        assert "instanceInfos" in resp
        assert len(resp["instanceInfos"]) == 1
        assert resp["instanceInfos"][0]["instanceName"] == instance_name

    def test_add_tags_to_on_premises_instances(self, client):
        """AddTagsToOnPremisesInstances tags an on-prem instance."""
        instance_name = _unique("on-prem")
        iam_arn = f"arn:aws:iam::123456789012:user/{instance_name}"
        client.register_on_premises_instance(
            instanceName=instance_name,
            iamUserArn=iam_arn,
        )
        client.add_tags_to_on_premises_instances(
            tags=[{"Key": "env", "Value": "staging"}],
            instanceNames=[instance_name],
        )
        resp = client.get_on_premises_instance(instanceName=instance_name)
        tags = resp["instanceInfo"].get("tags", [])
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert tag_map.get("env") == "staging"

    def test_remove_tags_from_on_premises_instances(self, client):
        """RemoveTagsFromOnPremisesInstances removes tags."""
        instance_name = _unique("on-prem")
        iam_arn = f"arn:aws:iam::123456789012:user/{instance_name}"
        client.register_on_premises_instance(
            instanceName=instance_name,
            iamUserArn=iam_arn,
        )
        client.add_tags_to_on_premises_instances(
            tags=[{"Key": "env", "Value": "staging"}],
            instanceNames=[instance_name],
        )
        client.remove_tags_from_on_premises_instances(
            tags=[{"Key": "env", "Value": "staging"}],
            instanceNames=[instance_name],
        )
        resp = client.get_on_premises_instance(instanceName=instance_name)
        tags = resp["instanceInfo"].get("tags", [])
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert "env" not in tag_map

    def test_stop_deployment(self, client):
        """StopDeployment stops a running deployment."""
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
            resp = client.stop_deployment(deploymentId=deployment_id)
            assert "status" in resp
        finally:
            iam_client.delete_role(RoleName=role_name)

    def test_delete_resources_by_external_id(self, client):
        """DeleteResourcesByExternalId returns a response."""
        resp = client.delete_resources_by_external_id(externalId="nonexistent-id")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_git_hub_account_token(self, client):
        """DeleteGitHubAccountToken with nonexistent token."""
        resp = client.delete_git_hub_account_token(tokenName="nonexistent-token")
        assert "tokenName" in resp

    def test_continue_deployment_nonexistent(self, client):
        """ContinueDeployment with nonexistent deployment returns error."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.continue_deployment(deploymentId="d-XXXXXXXXX")

    def test_skip_wait_time_for_instance_termination(self, client):
        """SkipWaitTimeForInstanceTermination with nonexistent deployment."""
        # This operation is deprecated but still functional
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.skip_wait_time_for_instance_termination(deploymentId="d-XXXXXXXXX")

    def test_put_lifecycle_event_hook_execution_status(self, client):
        """PutLifecycleEventHookExecutionStatus with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.put_lifecycle_event_hook_execution_status(
                deploymentId="d-XXXXXXXXX",
                lifecycleEventHookExecutionId="fake-exec-id",
                status="Succeeded",
            )

    def test_batch_get_deployment_targets_nonexistent(self, client):
        """BatchGetDeploymentTargets with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.batch_get_deployment_targets(
                deploymentId="d-XXXXXXXXX",
                targetIds=["fake-target-id"],
            )

    def test_get_deployment_target_nonexistent(self, client):
        """GetDeploymentTarget with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.get_deployment_target(
                deploymentId="d-XXXXXXXXX",
                targetId="fake-target-id",
            )

    def test_list_deployment_targets_nonexistent(self, client):
        """ListDeploymentTargets with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.list_deployment_targets(deploymentId="d-XXXXXXXXX")

    def test_batch_get_deployment_instances_nonexistent(self, client):
        """BatchGetDeploymentInstances with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.batch_get_deployment_instances(
                deploymentId="d-XXXXXXXXX",
                instanceIds=["i-fake"],
            )

    def test_list_deployment_instances_nonexistent(self, client):
        """ListDeploymentInstances with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.list_deployment_instances(deploymentId="d-XXXXXXXXX")

    def test_get_deployment_instance_nonexistent(self, client):
        """GetDeploymentInstance with nonexistent deployment."""
        with pytest.raises(client.exceptions.DeploymentDoesNotExistException):
            client.get_deployment_instance(
                deploymentId="d-XXXXXXXXX",
                instanceId="i-fake",
            )
