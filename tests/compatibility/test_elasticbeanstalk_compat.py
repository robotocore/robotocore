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
