"""Elastic Beanstalk compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def eb():
    return make_client("elasticbeanstalk")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestApplicationOperations:
    def test_describe_applications_empty(self, eb):
        resp = eb.describe_applications()
        assert "Applications" in resp

    def test_create_application(self, eb):
        name = _unique("app")
        resp = eb.create_application(ApplicationName=name)
        assert resp["Application"]["ApplicationName"] == name
        eb.delete_application(ApplicationName=name)

    def test_describe_applications_filtered(self, eb):
        name = _unique("desc-app")
        eb.create_application(ApplicationName=name)
        try:
            resp = eb.describe_applications(ApplicationNames=[name])
            apps = resp["Applications"]
            assert len(apps) == 1
            assert apps[0]["ApplicationName"] == name
        finally:
            eb.delete_application(ApplicationName=name)

    def test_delete_application(self, eb):
        name = _unique("del-app")
        eb.create_application(ApplicationName=name)
        eb.delete_application(ApplicationName=name)
        resp = eb.describe_applications(ApplicationNames=[name])
        assert len(resp["Applications"]) == 0

    def test_create_application_has_arn(self, eb):
        name = _unique("arn-app")
        resp = eb.create_application(ApplicationName=name)
        try:
            assert "ApplicationArn" in resp["Application"]
            assert name in resp["Application"]["ApplicationArn"]
        finally:
            eb.delete_application(ApplicationName=name)


class TestApplicationVersionOperations:
    def test_create_application_version(self, eb):
        """CreateApplicationVersion creates a version."""
        app_name = _unique("ver-app")
        eb.create_application(ApplicationName=app_name)
        try:
            resp = eb.create_application_version(
                ApplicationName=app_name,
                VersionLabel="v1",
                Description="initial version",
            )
            assert resp["ApplicationVersion"]["VersionLabel"] == "v1"
            assert resp["ApplicationVersion"]["ApplicationName"] == app_name
        finally:
            eb.delete_application(ApplicationName=app_name)

    def test_describe_application_versions_filtered(self, eb):
        """DescribeApplicationVersions filters by application."""
        app_name = _unique("dver-app")
        eb.create_application(ApplicationName=app_name)
        eb.create_application_version(
            ApplicationName=app_name,
            VersionLabel="v1",
        )
        try:
            resp = eb.describe_application_versions(ApplicationName=app_name)
            assert len(resp["ApplicationVersions"]) >= 1
            assert resp["ApplicationVersions"][0]["VersionLabel"] == "v1"
        finally:
            eb.delete_application(ApplicationName=app_name)


class TestSolutionStacks:
    def test_list_available_solution_stacks(self, eb):
        resp = eb.list_available_solution_stacks()
        stacks = resp["SolutionStacks"]
        assert isinstance(stacks, list)
        assert len(stacks) > 0


class TestEnvironmentOperations:
    @pytest.fixture
    def app(self, eb):
        name = _unique("env-app")
        eb.create_application(ApplicationName=name)
        yield name
        eb.delete_application(ApplicationName=name)

    @pytest.fixture
    def solution_stack(self, eb):
        resp = eb.list_available_solution_stacks()
        return resp["SolutionStacks"][0]

    def test_describe_environments_empty(self, eb):
        resp = eb.describe_environments()
        assert "Environments" in resp

    def test_create_environment(self, eb, app, solution_stack):
        env_name = _unique("env")
        resp = eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        assert resp["EnvironmentName"] == env_name
        assert resp["ApplicationName"] == app
        assert "EnvironmentId" in resp

    def test_describe_environments_filtered(self, eb, app, solution_stack):
        env_name = _unique("desc-env")
        eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        resp = eb.describe_environments(EnvironmentNames=[env_name])
        envs = resp["Environments"]
        assert len(envs) == 1
        assert envs[0]["EnvironmentName"] == env_name

    def test_describe_environments_by_application(self, eb, app, solution_stack):
        env_name = _unique("byapp-env")
        eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        resp = eb.describe_environments(ApplicationName=app)
        envs = resp["Environments"]
        assert any(e["EnvironmentName"] == env_name for e in envs)

    def test_environment_has_status(self, eb, app, solution_stack):
        env_name = _unique("status-env")
        resp = eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        assert "Status" in resp


class TestTagsForResource:
    @pytest.fixture
    def env_arn(self, eb):
        app_name = _unique("tag-app")
        env_name = _unique("tag-env")
        eb.create_application(ApplicationName=app_name)
        stacks = eb.list_available_solution_stacks()["SolutionStacks"]
        resp = eb.create_environment(
            ApplicationName=app_name,
            EnvironmentName=env_name,
            SolutionStackName=stacks[0],
        )
        arn = resp["EnvironmentArn"]
        yield arn
        eb.delete_application(ApplicationName=app_name)

    def test_list_tags_for_resource(self, eb, env_arn):
        resp = eb.list_tags_for_resource(ResourceArn=env_arn)
        assert "ResourceTags" in resp

    def test_update_tags_for_resource(self, eb, env_arn):
        eb.update_tags_for_resource(
            ResourceArn=env_arn,
            TagsToAdd=[{"Key": "env", "Value": "test"}],
        )
        resp = eb.list_tags_for_resource(ResourceArn=env_arn)
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTags"]}
        assert tags["env"] == "test"


class TestElasticBeanstalkAdditionalOps:
    """Tests for additional ElasticBeanstalk operations."""

    def test_describe_account_attributes(self, eb):
        resp = eb.describe_account_attributes()
        assert "ResourceQuotas" in resp

    def test_describe_application_versions_empty(self, eb):
        resp = eb.describe_application_versions()
        assert "ApplicationVersions" in resp
        assert isinstance(resp["ApplicationVersions"], list)

    def test_describe_configuration_options(self, eb):
        resp = eb.describe_configuration_options()
        assert "Options" in resp
        assert isinstance(resp["Options"], list)

    def test_describe_configuration_settings(self, eb):
        app_name = _unique("cfg-app")
        eb.create_application(ApplicationName=app_name)
        try:
            resp = eb.describe_configuration_settings(ApplicationName=app_name)
            assert "ConfigurationSettings" in resp
        finally:
            eb.delete_application(ApplicationName=app_name)

    def test_describe_environment_health(self, eb):
        resp = eb.describe_environment_health(
            EnvironmentName="nonexistent-env",
            AttributeNames=["All"],
        )
        assert "HealthStatus" in resp

    def test_describe_environment_managed_action_history(self, eb):
        resp = eb.describe_environment_managed_action_history()
        assert "ManagedActionHistoryItems" in resp
        assert isinstance(resp["ManagedActionHistoryItems"], list)

    def test_describe_environment_managed_actions(self, eb):
        resp = eb.describe_environment_managed_actions()
        assert "ManagedActions" in resp
        assert isinstance(resp["ManagedActions"], list)

    def test_describe_environment_resources(self, eb):
        resp = eb.describe_environment_resources(EnvironmentName="nonexistent-env")
        assert "EnvironmentResources" in resp

    def test_describe_events(self, eb):
        resp = eb.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_instances_health(self, eb):
        resp = eb.describe_instances_health(EnvironmentName="nonexistent-env")
        assert "InstanceHealthList" in resp
        assert isinstance(resp["InstanceHealthList"], list)

    def test_describe_platform_version(self, eb):
        resp = eb.describe_platform_version(
            PlatformArn="arn:aws:elasticbeanstalk:us-east-1::platform/test/1.0"
        )
        assert "PlatformDescription" in resp

    def test_list_platform_branches(self, eb):
        resp = eb.list_platform_branches()
        assert "PlatformBranchSummaryList" in resp
        assert isinstance(resp["PlatformBranchSummaryList"], list)

    def test_list_platform_versions(self, eb):
        resp = eb.list_platform_versions()
        assert "PlatformSummaryList" in resp
        assert isinstance(resp["PlatformSummaryList"], list)


class TestApplicationVersionFiltering:
    """Tests for application version filtering and multiple versions."""

    @pytest.fixture
    def app_with_versions(self, eb):
        name = _unique("ver-app")
        eb.create_application(ApplicationName=name)
        eb.create_application_version(ApplicationName=name, VersionLabel="v1", Description="first")
        eb.create_application_version(ApplicationName=name, VersionLabel="v2", Description="second")
        yield name
        eb.delete_application(ApplicationName=name)

    def test_describe_application_versions_multiple(self, eb, app_with_versions):
        """DescribeApplicationVersions returns multiple versions."""
        resp = eb.describe_application_versions(ApplicationName=app_with_versions)
        versions = resp["ApplicationVersions"]
        assert len(versions) >= 2
        labels = {v["VersionLabel"] for v in versions}
        assert "v1" in labels
        assert "v2" in labels

    def test_describe_application_versions_by_label(self, eb, app_with_versions):
        """DescribeApplicationVersions filters by VersionLabels."""
        resp = eb.describe_application_versions(
            ApplicationName=app_with_versions, VersionLabels=["v1"]
        )
        versions = resp["ApplicationVersions"]
        assert len(versions) == 1
        assert versions[0]["VersionLabel"] == "v1"

    def test_application_version_has_application_name(self, eb, app_with_versions):
        """ApplicationVersion includes ApplicationName."""
        resp = eb.describe_application_versions(ApplicationName=app_with_versions)
        for v in resp["ApplicationVersions"]:
            assert v["ApplicationName"] == app_with_versions

    def test_application_version_has_description(self, eb, app_with_versions):
        """ApplicationVersion includes Description when provided."""
        resp = eb.describe_application_versions(
            ApplicationName=app_with_versions, VersionLabels=["v1"]
        )
        assert resp["ApplicationVersions"][0]["Description"] == "first"


class TestEnvironmentDetails:
    """Tests for environment creation details and filtering."""

    @pytest.fixture
    def solution_stack(self, eb):
        resp = eb.list_available_solution_stacks()
        return resp["SolutionStacks"][0]

    @pytest.fixture
    def app(self, eb):
        name = _unique("envdet-app")
        eb.create_application(ApplicationName=name)
        yield name
        eb.delete_application(ApplicationName=name)

    def test_environment_has_arn(self, eb, app, solution_stack):
        """CreateEnvironment returns EnvironmentArn."""
        env_name = _unique("earn-env")
        resp = eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        assert "EnvironmentArn" in resp
        assert "environment" in resp["EnvironmentArn"]

    def test_describe_environments_by_id(self, eb, app, solution_stack):
        """DescribeEnvironments filters by EnvironmentIds."""
        env_name = _unique("byid-env")
        created = eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        env_id = created["EnvironmentId"]
        resp = eb.describe_environments(EnvironmentIds=[env_id])
        assert len(resp["Environments"]) == 1
        assert resp["Environments"][0]["EnvironmentId"] == env_id

    def test_create_environment_with_tags(self, eb, app, solution_stack):
        """CreateEnvironment with Tags makes tags retrievable."""
        env_name = _unique("tagging-env")
        created = eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
            Tags=[{"Key": "team", "Value": "backend"}],
        )
        arn = created["EnvironmentArn"]
        tags_resp = eb.list_tags_for_resource(ResourceArn=arn)
        tag_dict = {t["Key"]: t["Value"] for t in tags_resp["ResourceTags"]}
        assert tag_dict["team"] == "backend"

    def test_environment_has_solution_stack_name(self, eb, app, solution_stack):
        """CreateEnvironment returns a SolutionStackName field."""
        env_name = _unique("ss-env")
        resp = eb.create_environment(
            ApplicationName=app,
            EnvironmentName=env_name,
            SolutionStackName=solution_stack,
        )
        assert "SolutionStackName" in resp


class TestTagsRoundTrip:
    """Tests for tag add/remove round-trip."""

    @pytest.fixture
    def env_arn(self, eb):
        app_name = _unique("tagrt-app")
        env_name = _unique("tagrt-env")
        eb.create_application(ApplicationName=app_name)
        stacks = eb.list_available_solution_stacks()["SolutionStacks"]
        resp = eb.create_environment(
            ApplicationName=app_name,
            EnvironmentName=env_name,
            SolutionStackName=stacks[0],
        )
        arn = resp["EnvironmentArn"]
        yield arn
        eb.delete_application(ApplicationName=app_name)

    def test_add_multiple_tags(self, eb, env_arn):
        """UpdateTagsForResource can add multiple tags at once."""
        eb.update_tags_for_resource(
            ResourceArn=env_arn,
            TagsToAdd=[
                {"Key": "k1", "Value": "v1"},
                {"Key": "k2", "Value": "v2"},
            ],
        )
        resp = eb.list_tags_for_resource(ResourceArn=env_arn)
        tag_dict = {t["Key"]: t["Value"] for t in resp["ResourceTags"]}
        assert tag_dict["k1"] == "v1"
        assert tag_dict["k2"] == "v2"

    def test_remove_tags(self, eb, env_arn):
        """UpdateTagsForResource can remove tags."""
        eb.update_tags_for_resource(
            ResourceArn=env_arn,
            TagsToAdd=[{"Key": "removeme", "Value": "val"}],
        )
        eb.update_tags_for_resource(
            ResourceArn=env_arn,
            TagsToRemove=["removeme"],
        )
        resp = eb.list_tags_for_resource(ResourceArn=env_arn)
        keys = [t["Key"] for t in resp["ResourceTags"]]
        assert "removeme" not in keys
